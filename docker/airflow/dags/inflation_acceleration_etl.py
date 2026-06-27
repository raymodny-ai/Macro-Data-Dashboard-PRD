"""
通胀二阶导组 ETL DAG
====================
T-180 全量拉取:
  - CPILFESL: 核心 CPI (剔除食品和能源)
  - CES0500000003: 总私人部门平均时薪
  - CES3000000008: 制造业时薪
  - CES7000000003: 休闲酒店业时薪
  - CES5000000003: 信息产业时薪

计算流水线:
  原始数据 → 3MMA → MoM 增速 → 二阶加速度 → "薪柴复燃"预警 → UPSERT

调度: 每月第2个工作日 UTC 14:00 (CPI/Payroll 数据发布后)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import logging

logger = logging.getLogger(__name__)

from lib.db_utils import push_calculated_results, send_completion_webhook, refresh_continuous_aggregate
from lib.rate_limiter import fred_rate_limiter
from lib.calculators.inflation_acceleration import process_inflation_group

DAG_ID = 'inflation_acceleration_etl'
DATA_GROUP = 'inflation'
ROLLING_WINDOW_DAYS = 180

# FRED 序列 ID 列表
FRED_SERIES_IDS = [
    'CPILFESL',          # 核心 CPI
    'CES0500000003',     # 总私人部门时薪
    'CES3000000008',     # 制造业时薪
    'CES7000000003',     # 休闲酒店业时薪
    'CES5000000003',     # 信息产业时薪
]

default_args = {
    'owner': 'macro_dashboard',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='T-180 通胀二阶导: CPI+时薪 → 3MMA → MoM → 加速度 → 薪柴复燃预警',
    schedule_interval='0 14 2-8 * 1-5',  # 每月2-8日工作日 UTC 14:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'inflation', 't180', 'acceleration'],
    max_active_runs=1,
) as dag:

    # ====================================================================
    # Task 1: 计算 T-180 日期范围
    # ====================================================================
    def compute_date_range(**kwargs):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=ROLLING_WINDOW_DAYS)
        date_range = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
        }
        logger.info(f"T-{ROLLING_WINDOW_DAYS}: {date_range['start_date']} -> {date_range['end_date']}")
        kwargs['ti'].xcom_push(key='date_range', value=date_range)
        return date_range

    # ====================================================================
    # Task 2: FRED API 批量拉取 5 个序列
    # ====================================================================
    def fetch_fred_series(**kwargs):
        """T-180 全量拉取 5 个 FRED 序列 (限流保护)"""
        import os
        from fredapi import Fred
        import pandas as pd

        ti = kwargs['ti']
        date_range = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        fred = Fred(api_key=os.getenv('FRED_API_KEY', ''))
        raw_data = {}

        for series_id in FRED_SERIES_IDS:
            fred_rate_limiter.wait_and_acquire()
            logger.info(f"Fetching {series_id} [{date_range['start_date']} -> {date_range['end_date']}]")
            try:
                series = fred.get_series(
                    series_id,
                    observation_start=date_range['start_date'],
                    observation_end=date_range['end_date'],
                )
                raw_data[series_id] = series
                logger.info(f"  {series_id}: {len(series)} observations")
            except Exception as e:
                logger.error(f"  {series_id}: FAILED - {e}")
                raw_data[series_id] = pd.Series(dtype=float)

        # 转换为 JSON 可序列化格式
        serialized = {
            sid: {str(k): float(v) if v is not None else None for k, v in s.items()}
            for sid, s in raw_data.items()
        }
        ti.xcom_push(key='raw_data', value=serialized)
        return {sid: len(s) for sid, s in raw_data.items()}

    # ====================================================================
    # Task 3: 计算 3MMA + MoM + 二阶加速度 + 薪柴复燃
    # ====================================================================
    def compute_acceleration(**kwargs):
        """调用 B2 计算模块: process_inflation_group()"""
        import pandas as pd

        ti = kwargs['ti']
        serialized = ti.xcom_pull(task_ids='fetch_fred_series', key='raw_data')

        # 反序列化为 pd.Series
        raw_data = {
            sid: pd.Series({pd.Timestamp(k): v for k, v in data.items()})
            for sid, data in serialized.items()
        }

        records = process_inflation_group(raw_data)
        logger.info(f"Acceleration computed: {len(records)} records")

        ti.xcom_push(key='calculated_records', value=records)
        return len(records)

    # ====================================================================
    # Task 4: 数据质量门禁
    # ====================================================================
    def quality_gate(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='compute_acceleration', key='calculated_records')
        if records and len(records) > 50:
            logger.info(f"Quality gate PASSED: {len(records)} records")
            return True
        logger.error(f"Quality gate FAILED: only {len(records) if records else 0} records")
        return False

    # ====================================================================
    # Task 5: UPSERT 入库 (智能路由)
    # ====================================================================
    def upsert_inflation_data(**kwargs):
        """UPSERT 写入 inflation_data 超表"""
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='compute_acceleration', key='calculated_records')

        count = push_calculated_results(
            table_name='inflation_data',
            records=records,
            conflict_columns=['record_date', 'symbol'],
        )
        ti.xcom_push(key='upsert_count', value=count)
        return count

    # ====================================================================
    # Task 6: 按需刷新连续聚合视图
    # ====================================================================
    def refresh_aggregates(**kwargs):
        """
        精准重建 T-180 窗口的连续聚合视图
        架构原则: 按需刷新, 非全量重算
        """
        ti = kwargs['ti']
        date_range = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        refresh_continuous_aggregate(
            view_name='inflation_monthly_agg',
            start_date=date_range['start_date'],
            end_date=date_range['end_date'],
        )
        logger.info("Continuous aggregate 'inflation_monthly_agg' refreshed")

    # ====================================================================
    # Task 7: 发射 Webhook
    # ====================================================================
    def emit_webhook(**kwargs):
        ti = kwargs['ti']
        count = ti.xcom_pull(task_ids='upsert_inflation_data', key='upsert_count') or 0
        send_completion_webhook(
            dag_id=DAG_ID,
            data_group=DATA_GROUP,
            record_count=count,
        )

    # ====================================================================
    # DAG 编排
    # ====================================================================
    t1 = PythonOperator(task_id='compute_date_range', python_callable=compute_date_range)
    t2 = PythonOperator(task_id='fetch_fred_series', python_callable=fetch_fred_series)
    t3 = PythonOperator(task_id='compute_acceleration', python_callable=compute_acceleration)
    t4 = ShortCircuitOperator(task_id='quality_gate', python_callable=quality_gate)
    t5 = PythonOperator(task_id='upsert_inflation_data', python_callable=upsert_inflation_data)
    t6 = PythonOperator(task_id='refresh_aggregates', python_callable=refresh_aggregates)
    t7 = PythonOperator(task_id='emit_webhook', python_callable=emit_webhook)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
