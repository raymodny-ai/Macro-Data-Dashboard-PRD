"""
数据库维护 DAG
==============
调度: 每周日 UTC 04:00 执行

任务:
  1. VACUUM ANALYZE 五张超表 (回收死元组 + 更新统计信息)
  2. 将维护日志写入 db_maintenance_log 表
  3. 检查表膨胀率 (可选, B7 升级)

架构原则:
  - 定期 VACUUM ANALYZE 维护数据库统计信息
  - 高频 UPSERT 覆写产生大量死元组, 不及时清理将导致查询性能劣化
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

DAG_ID = 'db_maintenance'

# 五张超表
HYPERTABLES = [
    'inflation_data',
    'fiscal_auction_data',
    'liquidity_corridor',
    'ai_capex_data',
    'market_contagion',
]

default_args = {
    'owner': 'macro_dashboard',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Weekly VACUUM ANALYZE + maintenance for all hypertables',
    schedule_interval='0 4 * * 0',  # 每周日 UTC 04:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'vacuum', 'weekly'],
    max_active_runs=1,
) as dag:

    # ========================================================================
    # Task 1: VACUUM ANALYZE 全部超表
    # ========================================================================
    def vacuum_analyze_all(**kwargs):
        """
        对五张超表执行 VACUUM ANALYZE
        VACUUM: 回收死元组空间
        ANALYZE: 更新列统计信息 (供查询优化器使用)
        """
        from lib.db_utils import get_db_connection

        results = []
        with get_db_connection() as conn:
            # VACUUM 不能在事务块内执行, 需 autocommit
            conn.autocommit = True
            with conn.cursor() as cur:
                for table in HYPERTABLES:
                    try:
                        logger.info(f"VACUUM ANALYZE {table} ...")
                        cur.execute(f"VACUUM ANALYZE {table}")
                        results.append({
                            'table': table,
                            'status': 'success',
                            'timestamp': datetime.now().isoformat(),
                        })
                        logger.info(f"  {table}: OK")
                    except Exception as e:
                        logger.error(f"  {table}: FAILED - {e}")
                        results.append({
                            'table': table,
                            'status': f'error: {e}',
                            'timestamp': datetime.now().isoformat(),
                        })
            conn.autocommit = False

        kwargs['ti'].xcom_push(key='vacuum_results', value=results)
        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"VACUUM ANALYZE complete: {success_count}/{len(HYPERTABLES)} succeeded")
        return results

    # ========================================================================
    # Task 2: 记录维护日志
    # ========================================================================
    def log_maintenance(**kwargs):
        """将维护结果写入 db_maintenance_log 表"""
        from lib.db_utils import get_db_connection

        ti = kwargs['ti']
        results = ti.xcom_pull(task_ids='vacuum_analyze', key='vacuum_results') or []

        if not results:
            logger.warning("No vacuum results to log")
            return

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for result in results:
                    cur.execute("""
                        INSERT INTO db_maintenance_log
                            (operation, table_name, status, executed_at, details)
                        VALUES (%s, %s, %s, NOW(), %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        'VACUUM_ANALYZE',
                        result['table'],
                        result['status'],
                        f"Executed by DAG '{DAG_ID}'",
                    ))
            conn.commit()

        logger.info(f"Maintenance log: {len(results)} entries recorded")

    # ========================================================================
    # Task 3: 表统计信息采集
    # ========================================================================
    def collect_table_stats(**kwargs):
        """
        采集各超表的行数/大小/最后更新时间
        供运维监控使用
        """
        from lib.db_utils import get_db_connection

        stats = []
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for table in HYPERTABLES:
                    # 行数估计 (基于 pg_stat)
                    cur.execute(f"""
                        SELECT
                            reltuples::bigint AS est_rows,
                            pg_size_pretty(pg_total_relation_size('{table}')) AS total_size,
                            pg_size_pretty(pg_relation_size('{table}')) AS table_size
                        FROM pg_class
                        WHERE relname = '{table}'
                    """)
                    row = cur.fetchone()
                    if row:
                        stat = {
                            'table': table,
                            'est_rows': int(row[0]),
                            'total_size': row[1],
                            'table_size': row[2],
                        }
                        stats.append(stat)
                        logger.info(
                            f"  {table}: ~{stat['est_rows']} rows, "
                            f"size={stat['total_size']}"
                        )

                    # 最后更新时间
                    date_col = 'record_date' if table != 'fiscal_auction_data' else 'auction_date'
                    if table == 'ai_capex_data':
                        date_col = 'report_date'
                    elif table == 'market_contagion':
                        date_col = 'trade_date'

                    cur.execute(f"""
                        SELECT MAX({date_col}) FROM {table}
                    """)
                    last_date = cur.fetchone()
                    if last_date and last_date[0]:
                        logger.info(f"  {table}: latest data = {last_date[0]}")

        kwargs['ti'].xcom_push(key='table_stats', value=stats)
        return stats

    # ========================================================================
    # DAG 任务编排
    # ========================================================================
    task_vacuum = PythonOperator(
        task_id='vacuum_analyze',
        python_callable=vacuum_analyze_all,
    )

    task_log = PythonOperator(
        task_id='log_maintenance',
        python_callable=log_maintenance,
    )

    task_stats = PythonOperator(
        task_id='collect_table_stats',
        python_callable=collect_table_stats,
    )

    # VACUUM → 记录日志 → 采集统计
    task_vacuum >> task_log >> task_stats
