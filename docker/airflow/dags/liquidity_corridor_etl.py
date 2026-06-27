"""
流动性走廊组 ETL DAG
===================
架构原则:
  - T-180 滚动全量覆盖: 每次触发拉取过去 180 天 SOFR/IORB 全量数据
  - catchup=False: 宕机恢复后仅执行最新 T-180 全量拉取, 不回填历史
  - 双层限流隔离: FRED 宽容接口走 Airflow Worker 进程内限流
  - UPSERT 无锁覆写: ON CONFLICT DO UPDATE, 严禁 DELETE+INSERT
  - "水管爆裂" 预警: 单日利差飙升 ≥10bp 触发 crisis_alert
  - 事件驱动缓存强驱逐: 写入完成后 Webhook → FastAPI → Redis DEL → SSE 广播
  - 按需刷新连续聚合: 精准重建 T-180 窗口的 spread_percentiles_30d 视图

调度: 交易日 UTC 12:00 (美东 7/8 AM)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import logging

logger = logging.getLogger(__name__)

# 导入共享库
from lib.db_utils import (
    upsert_records,
    get_system_thresholds,
    send_completion_webhook,
    refresh_continuous_aggregate,
)
from lib.rate_limiter import fred_rate_limiter
from lib.validators import (
    validate_liquidity_records,
    detect_crisis_burst,
    validate_record_count,
)

# ============================================================================
# T-180 滚动窗口参数
# ============================================================================
ROLLING_WINDOW_DAYS = 180
DAG_ID = 'liquidity_corridor_etl'
DATA_GROUP = 'liquidity'

# T-180 窗口预期最少记录数 (~120 个交易日, 保守取 80)
EXPECTED_MIN_RECORDS = 80


# ============================================================================
# DAG 定义
# ============================================================================
default_args = {
    'owner': 'macro_dashboard',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='T-180 全量拉取 SOFR/IORB 利差数据 + 三级状态触发 + 水管爆裂预警',
    schedule_interval='0 12 * * 1-5',  # 交易日 UTC 12:00
    start_date=datetime(2024, 1, 1),
    catchup=False,  # 强制禁用: 宕机恢复仅执行最新 T-180 全量拉取
    tags=['etl', 'liquidity', 't180', 'sofr_iorb'],
    max_active_runs=1,  # 严禁并发执行
) as dag:

    # ========================================================================
    # Task 1: 计算 T-180 滚动窗口日期范围
    # ========================================================================
    def compute_date_range(**kwargs):
        """计算 T-180 滚动窗口的起止日期并推送到 XCom"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=ROLLING_WINDOW_DAYS)

        date_range = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'window_days': ROLLING_WINDOW_DAYS,
        }

        logger.info(
            f"T-{ROLLING_WINDOW_DAYS} window: "
            f"{date_range['start_date']} -> {date_range['end_date']}"
        )
        kwargs['ti'].xcom_push(key='date_range', value=date_range)
        return date_range

    # ========================================================================
    # Task 2: FRED API T-180 全量拉取 SOFR + IORB
    # ========================================================================
    def fetch_sofr_iorb(**kwargs):
        """
        从 FRED API T-180 全量拉取 SOFR 和 IORB 日度利率数据
        架构原则: 宽容接口走 Airflow Worker 进程内限流 (≤10次/秒)
        """
        import os
        from fredapi import Fred

        ti = kwargs['ti']
        date_range = ti.xcom_pull(task_ids='compute_date_range', key='date_range')
        start_date = date_range['start_date']
        end_date = date_range['end_date']

        fred_api_key = os.getenv('FRED_API_KEY', '')
        if not fred_api_key:
            raise ValueError("FRED_API_KEY not configured!")

        fred = Fred(api_key=fred_api_key)

        # 限流保护: FRED 宽容接口 ≤10次/秒
        fred_rate_limiter.wait_and_acquire()
        logger.info(f"Fetching SOFR [{start_date} -> {end_date}]")
        sofr = fred.get_series('SOFR', observation_start=start_date, observation_end=end_date)

        fred_rate_limiter.wait_and_acquire()
        logger.info(f"Fetching IORB [{start_date} -> {end_date}]")
        iorb = fred.get_series('IORB', observation_start=start_date, observation_end=end_date)

        logger.info(f"SOFR: {len(sofr)} observations, IORB: {len(iorb)} observations")

        # 原始数据推送到 XCom (以 dict 格式, Airflow 序列化友好)
        raw_data = {
            'sofr': {
                str(k): float(v) if v is not None else None
                for k, v in sofr.items()
            },
            'iorb': {
                str(k): float(v) if v is not None else None
                for k, v in iorb.items()
            },
        }
        ti.xcom_push(key='raw_rates', value=raw_data)
        return {'sofr_count': len(sofr), 'iorb_count': len(iorb)}

    # ========================================================================
    # Task 3: 利差计算 + 三级状态触发器
    # ========================================================================
    def compute_spread_and_state(**kwargs):
        """
        SOFR-IORB 利差计算 + 三级状态触发器

        三级状态定义:
          0 = 充裕 (spread < -0.03): 回购利率低于准备金利率, 市场资金充沛
          1 = 紧张 (-0.03 ≤ spread ≤ 0): 利差收窄, 流动性边际收紧
          2 = 瘫痪 (spread > 0): 回购利率突破准备金利率, 银行间拆借冻结

        架构原则: 阈值从配置读取 (B1 MVP 使用环境变量, B4 升级为 DB 热更新)
        """
        ti = kwargs['ti']
        raw_data = ti.xcom_pull(task_ids='fetch_sofr_iorb', key='raw_rates')
        thresholds = get_system_thresholds()

        tight_threshold = thresholds['spread_threshold_tight']   # -0.03
        stress_threshold = thresholds['spread_threshold_stress']  # 0.0

        sofr_dict = raw_data['sofr']
        iorb_dict = raw_data['iorb']

        records = []
        # 以 SOFR 日期为主键, 对齐 IORB
        all_dates = sorted(set(sofr_dict.keys()) | set(iorb_dict.keys()))

        for date_str in all_dates:
            sofr_val = sofr_dict.get(date_str)
            iorb_val = iorb_dict.get(date_str)

            # 利差计算
            spread = None
            if sofr_val is not None and iorb_val is not None:
                spread = round(sofr_val - iorb_val, 6)

            # 三级状态触发器
            system_state = None
            if spread is not None:
                if spread < tight_threshold:
                    system_state = 0  # 充裕
                elif spread <= stress_threshold:
                    system_state = 1  # 紧张
                else:
                    system_state = 2  # 瘫痪

            records.append({
                'record_date': date_str,
                'symbol': 'SPREAD',
                'sofr_rate': sofr_val,
                'iorb_rate': iorb_val,
                'spread': spread,
                'system_state': system_state,
                'crisis_alert': False,  # 初始值, Task 4 会更新
            })

        logger.info(
            f"Computed {len(records)} spread records. "
            f"States: "
            f"充裕={sum(1 for r in records if r['system_state'] == 0)}, "
            f"紧张={sum(1 for r in records if r['system_state'] == 1)}, "
            f"瘫痪={sum(1 for r in records if r['system_state'] == 2)}"
        )

        ti.xcom_push(key='spread_records', value=records)
        return len(records)

    # ========================================================================
    # Task 4: 数据质量验证 + "水管爆裂" 预警检测
    # ========================================================================
    def validate_and_detect_crisis(**kwargs):
        """
        数据质量强验证 + 水管爆裂预警

        水管爆裂判定: 单日 SOFR-IORB 利差飙升 ≥ 10bp (0.10%)
        类似 2019年9月17日回购市场危机 (SOFR 单日跳涨至 5.25%)
        """
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='compute_spread_and_state', key='spread_records')
        thresholds = get_system_thresholds()

        if not records:
            raise ValueError("No spread records to validate!")

        # 记录数合理性校验
        validate_record_count(
            expected_min=EXPECTED_MIN_RECORDS,
            actual_count=len(records),
            context=f"T-{ROLLING_WINDOW_DAYS} liquidity corridor",
        )

        # 数据质量验证
        valid_records, report = validate_liquidity_records(
            records=records,
            spread_min=thresholds['spread_min'],
            spread_max=thresholds['spread_max'],
            max_consecutive_nulls=thresholds['max_consecutive_nulls'],
        )

        if not report.is_acceptable:
            raise ValueError(
                f"Data quality too low: {report.summary()}. "
                f"Aborting to prevent downstream pollution."
            )

        # 水管爆裂预警检测
        crisis_alerts = detect_crisis_burst(
            records=valid_records,
            burst_threshold_bps=thresholds['crisis_burst_bps'],
        )

        alert_dates = [a['record_date'] for a in crisis_alerts]
        logger.info(
            f"Validation passed: {len(valid_records)} records. "
            f"Pipe burst alerts: {len(alerts)} -> {alert_dates}"
        )

        ti.xcom_push(key='validated_records', value=valid_records)
        ti.xcom_push(key='crisis_alerts', value={
            'count': len(crisis_alerts),
            'dates': alert_dates,
        })
        return {
            'valid_count': len(valid_records),
            'alert_count': len(crisis_alerts),
        }

    # ========================================================================
    # Task 5: 是否继续写入判断 (质量门禁)
    # ========================================================================
    def quality_gate(**kwargs):
        """
        数据质量门禁: 验证通过才允许写入数据库
        返回 True 继续执行, False 短路中止
        """
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='validate_and_detect_crisis', key='validated_records')
        if records and len(records) > 0:
            logger.info(f"Quality gate PASSED: {len(records)} records ready for UPSERT")
            return True
        logger.error("Quality gate FAILED: no valid records")
        return False

    # ========================================================================
    # Task 6: UPSERT 无锁覆写入库
    # ========================================================================
    def upsert_liquidity_data(**kwargs):
        """
        UPSERT 写入 liquidity_corridor 超表
        架构原则: ON CONFLICT (record_date, symbol) DO UPDATE
                 严禁 DELETE + INSERT
        """
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='validate_and_detect_crisis', key='validated_records')

        if not records:
            logger.warning("No records to upsert")
            return 0

        count = upsert_records(
            table_name='liquidity_corridor',
            records=records,
            conflict_columns=['record_date', 'symbol'],
            batch_size=500,
        )

        ti.xcom_push(key='upsert_count', value=count)
        return count

    # ========================================================================
    # Task 7: 按需刷新连续聚合视图
    # ========================================================================
    def refresh_aggregates(**kwargs):
        """
        精准重建 T-180 窗口的连续聚合视图
        架构原则: 按需刷新, 非全量重算
        """
        ti = kwargs['ti']
        date_range = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        refresh_continuous_aggregate(
            view_name='spread_percentiles_30d',
            start_date=date_range['start_date'],
            end_date=date_range['end_date'],
        )
        logger.info("Continuous aggregate 'spread_percentiles_30d' refreshed")

    # ========================================================================
    # Task 8: 发射 Webhook (缓存强驱逐 + SSE 广播)
    # ========================================================================
    def emit_webhook(**kwargs):
        """
        DAG 完成 Webhook: 触发 FastAPI 缓存驱逐 → SSE 推送前端
        """
        ti = kwargs['ti']
        upsert_count = ti.xcom_pull(task_ids='upsert_liquidity_data', key='upsert_count') or 0
        crisis_info = ti.xcom_pull(task_ids='validate_and_detect_crisis', key='crisis_alerts') or {}

        send_completion_webhook(
            dag_id=DAG_ID,
            data_group=DATA_GROUP,
            record_count=upsert_count,
            extra={
                'crisis_alerts': crisis_info.get('count', 0),
                'crisis_dates': crisis_info.get('dates', []),
                'window_days': ROLLING_WINDOW_DAYS,
            },
        )

    # ========================================================================
    # DAG 任务编排 (线性流水线)
    # ========================================================================
    task_date_range = PythonOperator(
        task_id='compute_date_range',
        python_callable=compute_date_range,
    )

    task_fetch = PythonOperator(
        task_id='fetch_sofr_iorb',
        python_callable=fetch_sofr_iorb,
    )

    task_spread = PythonOperator(
        task_id='compute_spread_and_state',
        python_callable=compute_spread_and_state,
    )

    task_validate = PythonOperator(
        task_id='validate_and_detect_crisis',
        python_callable=validate_and_detect_crisis,
    )

    task_gate = ShortCircuitOperator(
        task_id='quality_gate',
        python_callable=quality_gate,
    )

    task_upsert = PythonOperator(
        task_id='upsert_liquidity_data',
        python_callable=upsert_liquidity_data,
    )

    task_refresh = PythonOperator(
        task_id='refresh_aggregates',
        python_callable=refresh_aggregates,
    )

    task_webhook = PythonOperator(
        task_id='emit_webhook',
        python_callable=emit_webhook,
    )

    # 流水线: 日期范围 → 数据拉取 → 利差计算 → 验证+预警 → 质量门禁 → UPSERT → 聚合刷新 → Webhook
    (
        task_date_range
        >> task_fetch
        >> task_spread
        >> task_validate
        >> task_gate
        >> task_upsert
        >> task_refresh
        >> task_webhook
    )
