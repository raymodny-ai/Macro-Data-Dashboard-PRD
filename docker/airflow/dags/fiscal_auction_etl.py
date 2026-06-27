"""
财政增量组 ETL DAG
==================
T-180 全量拉取:
  - US Treasury Fiscal Data API: 国债拍卖数据 (10Y/30Y)
  - FRED API: THREEFYTP10 (10年期 ACM 期限溢价)

计算流水线:
  拍卖数据 + ACM → 认购倍数 + 尾部点差 + 期限溢价 → "熊陡"判定 → UPSERT

调度: 每周三 UTC 13:00 (拍卖数据通常周二发布)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import logging
import os

logger = logging.getLogger(__name__)

from lib.db_utils import upsert_records, send_completion_webhook, get_system_thresholds
from lib.rate_limiter import fiscal_rate_limiter, fred_rate_limiter

DAG_ID = 'fiscal_auction_etl'
DATA_GROUP = 'fiscal'
ROLLING_WINDOW_DAYS = 180

# 财政部 API 端点
TREASURY_API_URL = 'https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/treasury_securities_auctions'

# 目标期限类型
TARGET_SECURITY_TYPES = ['10-Year', '30-Year']
SECURITY_TYPE_MAP = {'10-Year': '10Y', '30-Year': '30Y'}

default_args = {
    'owner': 'macro_dashboard',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='T-180 财政增量: Treasury拍卖 + ACM期限溢价 → 熊陡判定',
    schedule_interval='0 13 * * 3',  # 每周三 UTC 13:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'fiscal', 't180', 'treasury'],
    max_active_runs=1,
) as dag:

    # ====================================================================
    # Task 1: 日期范围
    # ====================================================================
    def compute_date_range(**kwargs):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=ROLLING_WINDOW_DAYS)
        dr = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
        }
        kwargs['ti'].xcom_push(key='date_range', value=dr)
        return dr

    # ====================================================================
    # Task 2: Treasury Fiscal Data API 拉取拍卖数据
    # ====================================================================
    def fetch_treasury_auctions(**kwargs):
        """
        调用 Treasury Fiscal Data API, T-180 全量拉取国债拍卖数据
        筛选 10-Year 和 30-Year 期限
        """
        import requests

        ti = kwargs['ti']
        dr = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        all_records = []
        for sec_type in TARGET_SECURITY_TYPES:
            fiscal_rate_limiter.wait_and_acquire()

            params = {
                'filter': f'security_type:eq:{sec_type},auction_date:gte:{dr["start_date"]}',
                'sort': '-auction_date',
                'format': 'json',
                'page[size]': 1000,
            }

            logger.info(f"Fetching Treasury auctions: {sec_type} [{dr['start_date']} ->]")
            try:
                resp = requests.get(TREASURY_API_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json().get('data', [])
                all_records.extend(data)
                logger.info(f"  {sec_type}: {len(data)} auctions")
            except Exception as e:
                logger.error(f"  {sec_type}: FAILED - {e}")

        ti.xcom_push(key='auction_data', value=all_records)
        return len(all_records)

    # ====================================================================
    # Task 3: FRED 拉取 ACM 期限溢价
    # ====================================================================
    def fetch_acm_term_premium(**kwargs):
        """从 FRED T-180 拉取 THREEFYTP10 (10Y ACM 期限溢价)"""
        import os
        from fredapi import Fred

        ti = kwargs['ti']
        dr = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        fred = Fred(api_key=os.getenv('FRED_API_KEY', ''))
        fred_rate_limiter.wait_and_acquire()

        logger.info(f"Fetching THREEFYTP10 [{dr['start_date']} -> {dr['end_date']}]")
        try:
            acm = fred.get_series(
                'THREEFYTP10',
                observation_start=dr['start_date'],
                observation_end=dr['end_date'],
            )
            # 转为 JSON 可序列化
            acm_dict = {str(k): float(v) if v is not None else None for k, v in acm.items()}
            ti.xcom_push(key='acm_data', value=acm_dict)
            logger.info(f"  THREEFYTP10: {len(acm)} observations")
            return len(acm)
        except Exception as e:
            logger.error(f"  THREEFYTP10: FAILED - {e}")
            ti.xcom_push(key='acm_data', value={})
            return 0

    # ====================================================================
    # Task 4: 解析拍卖数据 + 熊陡判定
    # ====================================================================
    def parse_and_detect(**kwargs):
        """
        解析拍卖数据, 合并 ACM 期限溢价, 执行熊陡判定

        熊陡判定:
          认购倍数 (bid_to_cover) 连续 2 次 < 阈值 (默认 2.4)
          AND ACM 期限溢价 > 阈值 (默认 0.01 = 1%)
          → fiscal_warning_flag = True
        """
        ti = kwargs['ti']
        auction_data = ti.xcom_pull(task_ids='fetch_treasury_auctions', key='auction_data')
        acm_dict = ti.xcom_pull(task_ids='fetch_acm_term_premium', key='acm_data')

        thresholds = get_system_thresholds()
        btc_threshold = float(os.getenv('THRESHOLD_BID_TO_COVER', '2.4'))
        acm_threshold = float(os.getenv('THRESHOLD_ACM_PREMIUM', '0.01'))

        records = []
        # 按 security_type 分组追踪认购倍数
        prev_low_btc = {'10Y': False, '30Y': False}

        for item in sorted(auction_data, key=lambda x: x.get('auction_date', '')):
            auction_date = item.get('auction_date', '')[:10]
            sec_type_raw = item.get('security_type', '')
            sec_type = SECURITY_TYPE_MAP.get(sec_type_raw, sec_type_raw[:3])

            if sec_type not in ('10Y', '30Y'):
                continue

            # 提取字段
            btc = _safe_float(item.get('bid_to_cover_ratio'))
            high_yield = _safe_float(item.get('high_yield'))
            expected_yield = _safe_float(item.get('expected_yield'))
            tail_spread = None
            if high_yield is not None and expected_yield is not None:
                tail_spread = round(high_yield - expected_yield, 6)

            # ACM 期限溢价 (按日期对齐)
            acm_val = acm_dict.get(auction_date)

            # 熊陡判定
            low_btc = btc is not None and btc < btc_threshold
            consecutive_low = low_btc and prev_low_btc.get(sec_type, False)
            high_acm = acm_val is not None and acm_val > acm_threshold
            warning = consecutive_low and high_acm

            prev_low_btc[sec_type] = low_btc

            records.append({
                'auction_date': auction_date,
                'security_type': sec_type,
                'bid_to_cover_ratio': btc,
                'high_yield': high_yield,
                'expected_yield': expected_yield,
                'tail_spread': tail_spread,
                'acm_term_premium': acm_val,
                'fiscal_warning_flag': warning,
            })

        warnings = sum(1 for r in records if r.get('fiscal_warning_flag'))
        logger.info(f"Parsed {len(records)} auction records, {warnings} bear-steepener alerts")

        ti.xcom_push(key='fiscal_records', value=records)
        return len(records)

    # ====================================================================
    # Task 5: 质量门禁
    # ====================================================================
    def quality_gate(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='parse_and_detect', key='fiscal_records')
        return bool(records and len(records) > 0)

    # ====================================================================
    # Task 6: UPSERT 入库
    # ====================================================================
    def upsert_fiscal_data(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='parse_and_detect', key='fiscal_records')
        count = upsert_records(
            table_name='fiscal_auction_data',
            records=records,
            conflict_columns=['auction_date', 'security_type'],
        )
        ti.xcom_push(key='upsert_count', value=count)
        return count

    # ====================================================================
    # Task 7: Webhook
    # ====================================================================
    def emit_webhook(**kwargs):
        ti = kwargs['ti']
        count = ti.xcom_pull(task_ids='upsert_fiscal_data', key='upsert_count') or 0
        send_completion_webhook(dag_id=DAG_ID, data_group=DATA_GROUP, record_count=count)

    # ====================================================================
    # DAG 编排
    # ====================================================================
    t1 = PythonOperator(task_id='compute_date_range', python_callable=compute_date_range)
    t2 = PythonOperator(task_id='fetch_treasury_auctions', python_callable=fetch_treasury_auctions)
    t3 = PythonOperator(task_id='fetch_acm_term_premium', python_callable=fetch_acm_term_premium)
    t4 = PythonOperator(task_id='parse_and_detect', python_callable=parse_and_detect)
    t5 = ShortCircuitOperator(task_id='quality_gate', python_callable=quality_gate)
    t6 = PythonOperator(task_id='upsert_fiscal_data', python_callable=upsert_fiscal_data)
    t7 = PythonOperator(task_id='emit_webhook', python_callable=emit_webhook)

    # Treasury 和 ACM 可并行拉取, 之后合并
    t1 >> [t2, t3] >> t4 >> t5 >> t6 >> t7


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
