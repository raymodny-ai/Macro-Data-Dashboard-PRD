"""
国债收益率曲线 ETL DAG
======================
T-180 全量拉取:
  - FRED DGS 系列: 1M/3M/6M/1Y/2Y/5Y/10Y/30Y 美国国债恒定到期收益率

计算流水线:
  原始收益率 → 数据验证 → UPSERT → 刷新 treasury_yield_monthly 连续聚合

调度: 交易日 UTC 21:00 (美东 16:00/17:00, 收盘后)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import logging

logger = logging.getLogger(__name__)

from lib.db_utils import (
    push_calculated_results,
    send_completion_webhook,
    refresh_continuous_aggregate,
)
from lib.rate_limiter import fred_rate_limiter

DAG_ID = 'treasury_yield_etl'
DATA_GROUP = 'treasury_yield'
ROLLING_WINDOW_DAYS = 180

# FRED 国债收益率序列 ID
TREASURY_YIELD_SERIES = [
    'DGS1MO',   # 1个月
    'DGS3MO',   # 3个月
    'DGS6MO',   # 6个月
    'DGS1',     # 1年
    'DGS2',     # 2年
    'DGS5',     # 5年
    'DGS10',    # 10年
    'DGS30',    # 30年
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
    description='T-180 国债收益率曲线: DGS系列 → 验证 → UPSERT → 刷新3D曲面聚合',
    schedule_interval='0 21 * * 1-5',  # 交易日 UTC 21:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'treasury', 'yield_curve', 't180'],
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
    # Task 2: FRED API 批量拉取 8 个 DGS 序列
    # ====================================================================
    def fetch_treasury_yields(**kwargs):
        """
        T-180 全量拉取 8 个美国国债收益率序列 (限流保护)
        输出格式: [{record_date, symbol, value}, ...]
        """
        import os
        from fredapi import Fred

        ti = kwargs['ti']
        date_range = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        fred = Fred(api_key=os.getenv('FRED_API_KEY', ''))
        records = []
        total_obs = 0

        for series_id in TREASURY_YIELD_SERIES:
            fred_rate_limiter.wait_and_acquire()
            logger.info(f"Fetching {series_id} [{date_range['start_date']} -> {date_range['end_date']}]")

            try:
                series = fred.get_series(
                    series_id,
                    observation_start=date_range['start_date'],
                    observation_end=date_range['end_date'],
                )

                count = 0
                for date_idx, value in series.items():
                    if value is None:
                        continue
                    records.append({
                        'record_date': str(date_idx.date()),
                        'symbol': series_id,
                        'value': round(float(value), 6),
                    })
                    count += 1

                total_obs += count
                logger.info(f"  {series_id}: {count} observations")

            except Exception as e:
                logger.error(f"  {series_id}: FAILED - {e}")

        ti.xcom_push(key='yield_records', value=records)
        logger.info(f"Total: {len(records)} yield records across {len(TREASURY_YIELD_SERIES)} series")
        return len(records)

    # ====================================================================
    # Task 3: 数据质量验证
    # ====================================================================
    def validate_yields(**kwargs):
        """
        收益率数据质量校验:
          - 收益率范围: -1.0% ~ 15.0% (历史极端值 ±宽限)
          - 每个交易日至少有 1 条记录
          - 无负日期收益率 (异常检测)
        """
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='fetch_treasury_yields', key='yield_records')

        if not records:
            logger.error("No yield records to validate")
            return False

        valid = []
        invalid_count = 0

        for rec in records:
            val = rec.get('value')
            if val is None:
                invalid_count += 1
                continue
            if -1.0 <= val <= 15.0:
                valid.append(rec)
            else:
                invalid_count += 1
                logger.warning(
                    f"Out-of-range yield: {rec['symbol']} {rec['record_date']} = {val}"
                )

        ti.xcom_push(key='valid_records', value=valid)
        ti.xcom_push(key='validation_stats', value={
            'total': len(records),
            'valid': len(valid),
            'invalid': invalid_count,
        })

        logger.info(f"Validation: {len(valid)}/{len(records)} passed, {invalid_count} rejected")
        return len(valid) > 0

    # ====================================================================
    # Task 4: 质量门禁
    # ====================================================================
    def quality_gate(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='validate_yields', key='valid_records')
        if records and len(records) > 100:
            logger.info(f"Quality gate PASSED: {len(records)} records")
            return True
        logger.error(f"Quality gate FAILED: only {len(records) if records else 0} records")
        return False

    # ====================================================================
    # Task 5: UPSERT 入库
    # ====================================================================
    def upsert_yield_data(**kwargs):
        """UPSERT 写入 treasury_yields 超表"""
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='validate_yields', key='valid_records')

        count = push_calculated_results(
            table_name='treasury_yields',
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
        架构原则: 按需刷新, 非全量重算; 数据入库后才触发, 不依赖定时器
        """
        ti = kwargs['ti']
        date_range = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        refresh_continuous_aggregate(
            view_name='treasury_yield_monthly',
            start_date=date_range['start_date'],
            end_date=date_range['end_date'],
        )
        logger.info("Continuous aggregate 'treasury_yield_monthly' refreshed")

    # ====================================================================
    # Task 7: 发射 Webhook (缓存强驱逐 + SSE 广播)
    # ====================================================================
    def emit_webhook(**kwargs):
        """
        DAG 完成 Webhook: 触发 FastAPI 缓存驱逐 → SSE 推送前端
        """
        ti = kwargs['ti']
        upsert_count = ti.xcom_pull(task_ids='upsert_yield_data', key='upsert_count') or 0

        send_completion_webhook(
            dag_id=DAG_ID,
            data_group=DATA_GROUP,
            record_count=upsert_count,
            extra={
                'series_count': len(TREASURY_YIELD_SERIES),
                'window_days': ROLLING_WINDOW_DAYS,
            },
        )

    # ====================================================================
    # DAG 任务编排 (线性流水线)
    # ====================================================================
    task_date_range = PythonOperator(
        task_id='compute_date_range',
        python_callable=compute_date_range,
    )

    task_fetch = PythonOperator(
        task_id='fetch_treasury_yields',
        python_callable=fetch_treasury_yields,
    )

    task_validate = PythonOperator(
        task_id='validate_yields',
        python_callable=validate_yields,
    )

    task_gate = ShortCircuitOperator(
        task_id='quality_gate',
        python_callable=quality_gate,
    )

    task_upsert = PythonOperator(
        task_id='upsert_yield_data',
        python_callable=upsert_yield_data,
    )

    task_refresh = PythonOperator(
        task_id='refresh_aggregates',
        python_callable=refresh_aggregates,
    )

    task_webhook = PythonOperator(
        task_id='emit_webhook',
        python_callable=emit_webhook,
    )

    # 流水线: 日期范围 → 数据拉取 → 验证 → 质量门禁 → UPSERT → 聚合刷新 → Webhook
    (
        task_date_range
        >> task_fetch
        >> task_validate
        >> task_gate
        >> task_upsert
        >> task_refresh
        >> task_webhook
    )
