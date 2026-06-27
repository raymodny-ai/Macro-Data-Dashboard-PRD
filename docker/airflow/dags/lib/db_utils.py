"""
数据库工具模块
架构原则: UPSERT 无锁覆写 + 连接池管理 + DB 配置读取
         + COPY 临时表批量优化 + 计算结果智能路由
"""
import io
import csv
import os
from contextlib import contextmanager
from typing import List, Dict, Optional, Any
import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# 连接池配置
# ============================================================================
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'timescaledb'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'dbname': os.getenv('POSTGRES_DB', 'macro_dashboard'),
    'user': os.getenv('POSTGRES_USER', 'dashboard'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
}


@contextmanager
def get_db_connection():
    """上下文管理器: 自动管理数据库连接生命周期"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        logger.error(f"DB operation failed: {e}")
        raise
    finally:
        conn.close()


# ============================================================================
# UPSERT 无锁覆写 (架构原则: ON CONFLICT DO UPDATE, 严禁 DELETE+INSERT)
# ============================================================================
def upsert_records(
    table_name: str,
    records: List[Dict[str, Any]],
    conflict_columns: List[str],
    batch_size: int = 500,
) -> int:
    """
    通用 UPSERT 写入函数 (批量)
    
    Args:
        table_name: 目标表名 (如 'liquidity_corridor')
        records: 记录列表, 每条记录为字典
        conflict_columns: 冲突键 (如 ['record_date', 'symbol'])
        batch_size: 单批插入行数, 默认500
    
    Returns:
        实际写入的记录数
    """
    if not records:
        logger.warning(f"[{table_name}] No records to upsert")
        return 0

    columns = list(records[0].keys())
    update_cols = [c for c in columns if c not in conflict_columns]
    update_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)
    conflict_clause = ', '.join(conflict_columns)

    sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_clause}, updated_at = NOW()
    """

    total_written = 0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 分批写入避免单次内存峰值
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                values = [tuple(row.get(c) for c in columns) for row in batch]
                execute_values(cur, sql, values, page_size=batch_size)
                total_written += len(batch)
                logger.info(f"[{table_name}] Batch {i // batch_size + 1}: {len(batch)} rows written")
        conn.commit()

    logger.info(f"[{table_name}] UPSERT complete: {total_written}/{len(records)} rows")
    return total_written


# ============================================================================
# COPY + 临时表批量 UPSERT (架构原则: 降低逐行 UPSERT 的 WAL 压力)
# ============================================================================
def bulk_upsert_via_temp_table(
    table_name: str,
    records: List[Dict[str, Any]],
    conflict_columns: List[str],
    staging_suffix: str = '_staging',
) -> int:
    """
    高性能批量 UPSERT: COPY 写入临时表 → INSERT ON CONFLICT 合并至主表

    适用场景: T-180 全量覆盖大批量数据 (~900+ rows/batch)
    性能: COPY ~10000+ rows/sec vs execute_values ~500 rows/sec

    流程:
      1. CREATE TEMP TABLE {table}_staging (LIKE {table} INCLUDING ALL)
      2. COPY {staging} FROM STDIN (psycopg2 copy_expert)
      3. INSERT INTO {table} SELECT * FROM {staging} ON CONFLICT DO UPDATE
      4. DROP TABLE {staging}

    Args:
        table_name: 目标表名
        records: 记录列表
        conflict_columns: 冲突键
        staging_suffix: 临时表后缀

    Returns:
        实际写入的记录数
    """
    if not records:
        logger.warning(f"[{table_name}] No records to bulk upsert")
        return 0

    columns = list(records[0].keys())
    staging_table = f"{table_name}{staging_suffix}"
    update_cols = [c for c in columns if c not in conflict_columns]
    update_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)
    conflict_clause = ', '.join(conflict_columns)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Step 1: 创建同构临时表 (继承约束/类型, 不含索引)
            cur.execute(f"""
                CREATE TEMP TABLE IF NOT EXISTS {staging_table}
                (LIKE {table_name} INCLUDING DEFAULTS)
                ON COMMIT DELETE ROWS
            """)
            # 清空临时表 (防止事务复用残留)
            cur.execute(f"TRUNCATE TABLE {staging_table}")

            # Step 2: COPY FROM STDIN (CSV 格式, 高速写入)
            copy_buffer = io.StringIO()
            writer = csv.writer(copy_buffer)
            for row in records:
                writer.writerow([
                    _to_csv_value(row.get(c)) for c in columns
                ])
            copy_buffer.seek(0)

            copy_sql = (
                f"COPY {staging_table} ({', '.join(columns)}) "
                f"FROM STDIN WITH (FORMAT CSV, NULL '\\\\N')"
            )
            cur.copy_expert(copy_sql, copy_buffer)

            # Step 3: INSERT ON CONFLICT 合并至主表
            merge_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                SELECT {', '.join(columns)} FROM {staging_table}
                ON CONFLICT ({conflict_clause})
                DO UPDATE SET {update_clause}, updated_at = NOW()
            """
            cur.execute(merge_sql)
            merged_count = cur.rowcount

            # Step 4: 清理临时表
            cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

        conn.commit()

    logger.info(
        f"[{table_name}] Bulk UPSERT (COPY+temp): "
        f"{len(records)} staged, {merged_count} merged"
    )
    return len(records)


def _to_csv_value(val: Any) -> str:
    """将 Python 值转换为 CSV 写入格式"""
    if val is None:
        return '\\N'  # PostgreSQL COPY NULL 标记
    return str(val)


# ============================================================================
# 计算结果智能路由写入 (架构原则: 计算层输出标准化推回 TimescaleDB)
# ============================================================================
def push_calculated_results(
    table_name: str,
    records: List[Dict[str, Any]],
    conflict_columns: List[str],
    bulk_threshold: int = 500,
) -> int:
    """
    计算结果推回 TimescaleDB 的智能路由函数

    自动选择写入策略:
      - records < bulk_threshold: 使用 upsert_records() (execute_values)
      - records >= bulk_threshold: 使用 bulk_upsert_via_temp_table() (COPY)

    架构原则: Python 重度计算分离 → 标准化输出 List[Dict] → 智能路由 → DB

    Args:
        table_name: 目标表名 (如 'inflation_data', 'market_contagion')
        records: 计算模块输出的记录列表
        conflict_columns: 冲突键
        bulk_threshold: 切换为批量模式的阈值行数

    Returns:
        实际写入的记录数
    """
    if not records:
        logger.info(f"[{table_name}] No calculated results to push")
        return 0

    if len(records) < bulk_threshold:
        logger.info(
            f"[{table_name}] Using execute_values strategy "
            f"({len(records)} rows < {bulk_threshold} threshold)"
        )
        return upsert_records(table_name, records, conflict_columns)
    else:
        logger.info(
            f"[{table_name}] Using COPY+temp_table strategy "
            f"({len(records)} rows >= {bulk_threshold} threshold)"
        )
        return bulk_upsert_via_temp_table(table_name, records, conflict_columns)


# ============================================================================
# 配置读取 (架构原则: 规则引擎抽离 - 从 DB/YAML 读取阈值)
# ============================================================================
def get_system_thresholds() -> Dict[str, Any]:
    """
    从数据库/YAML读取系统阈值配置 (B1 MVP 使用硬编码, B4/B7 升级为热更新)
    
    Returns:
        {
            'sofr_iorb_spread_tight': -0.03,   # 充裕上限
            'sofr_iorb_spread_stress': 0.0,     # 紧张上限
            'crisis_burst_threshold': 0.10,     # 水管爆裂阈值 (单日飙升≥10bp)
            ...
        }
    """
    # B1 MVP: 硬编码默认值, 后续升级为 DB 配置表
    return {
        # 三级状态触发器阈值
        'spread_threshold_tight': float(os.getenv('THRESHOLD_SPREAD_TIGHT', -0.03)),
        'spread_threshold_stress': float(os.getenv('THRESHOLD_SPREAD_STRESS', 0.0)),
        # 水管爆裂预警: 单日利差飙升 >= 10bp (0.10%)
        'crisis_burst_bps': float(os.getenv('THRESHOLD_CRISIS_BURST_BPS', 0.10)),
        # 数据质量: 最大允许连续空值天数
        'max_consecutive_nulls': int(os.getenv('MAX_CONSECUTIVE_NULLS', 3)),
        # 数据质量: 利差合理范围
        'spread_min': float(os.getenv('SPREAD_MIN', -2.0)),
        'spread_max': float(os.getenv('SPREAD_MAX', 2.0)),
    }


# ============================================================================
# Webhook 发射器 (架构原则: 事件驱动缓存强驱逐)
# ============================================================================
def send_completion_webhook(
    dag_id: str,
    data_group: str,
    record_count: int,
    extra: Optional[Dict] = None,
):
    """
    DAG 完成后发射 Webhook, 触发 FastAPI → Redis 缓存强驱逐 → SSE 广播
    
    Args:
        dag_id: DAG 标识符
        data_group: 数据分组 (用于 Redis key pattern 匹配)
        record_count: 本次写入记录数
        extra: 额外负载字段
    """
    import httpx
    from datetime import datetime

    webhook_url = os.getenv(
        'WEBHOOK_URL',
        'http://fastapi:8000/api/v1/internal/cache-invalidate'
    )
    webhook_secret = os.getenv('WEBHOOK_SECRET', '')

    payload = {
        'event': 'dag_completed',
        'dag_id': dag_id,
        'data_group': data_group,
        'record_count': record_count,
        'timestamp': datetime.now().isoformat(),
    }
    if extra:
        payload.update(extra)

    headers = {
        'X-Webhook-Secret': webhook_secret,
        'Content-Type': 'application/json',
    }

    try:
        response = httpx.post(webhook_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Webhook sent: {dag_id} -> {data_group} ({record_count} records)")
    except Exception as e:
        # Webhook 失败不中断 DAG (non-fatal)
        logger.warning(f"Webhook failed (non-fatal): {e}")


# ============================================================================
# 连续聚合刷新 (架构原则: 按需刷新, 非全量重算)
# ============================================================================
def refresh_continuous_aggregate(
    view_name: str,
    start_date: str,
    end_date: str,
):
    """
    精准重建特定时间窗口的连续聚合视图
    
    Args:
        view_name: 连续聚合视图名称 (如 'spread_percentiles_30d')
        start_date: 窗口起始 (YYYY-MM-DD)
        end_date: 窗口结束 (YYYY-MM-DD)
    """
    sql = f"""
        CALL refresh_continuous_aggregate(
            '{view_name}',
            '{start_date}'::date,
            '{end_date}'::date + interval '1 day'
        )
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info(f"Refreshed continuous aggregate '{view_name}' [{start_date} -> {end_date}]")
