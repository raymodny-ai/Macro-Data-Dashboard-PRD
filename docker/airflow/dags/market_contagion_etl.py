"""
市场传染组 ETL DAG
==================
T-180 全量拉取 (yfinance):
  - ^MOVE: ICE BofAML MOVE Index (债市波动率)
  - SPY: S&P 500 ETF 复权收盘价
  - TLT: 20+ Year Treasury Bond ETF 复权收盘价

计算流水线:
  价格数据 → 对数收益率 → 30d/60d 滚动相关系数 → 市场传染警报 → UPSERT

调度: 交易日 UTC 22:00 (美股收盘后)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import logging

logger = logging.getLogger(__name__)

from lib.db_utils import push_calculated_results, send_completion_webhook
from lib.calculators.rolling_correlation import (
    process_contagion_group,
    detect_contagion_alert,
)

DAG_ID = 'market_contagion_etl'
DATA_GROUP = 'contagion'
ROLLING_WINDOW_DAYS = 180

default_args = {
    'owner': 'macro_dashboard',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='T-180 市场传染: MOVE+SPY+TLT → 对数收益率 → 滚动相关 → 传染警报',
    schedule_interval='0 22 * * 1-5',  # 交易日 UTC 22:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'contagion', 't180', 'move', 'spy_tlt'],
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
    # Task 2: yfinance 拉取 SPY + TLT + ^MOVE
    # ====================================================================
    def fetch_market_data(**kwargs):
        """T-180 全量拉取 SPY, TLT, ^MOVE 日线数据"""
        import yfinance as yf
        import pandas as pd

        ti = kwargs['ti']
        dr = ti.xcom_pull(task_ids='compute_date_range', key='date_range')

        tickers = {
            'SPY': 'SPY',
            'TLT': 'TLT',
            'MOVE': '^MOVE',
        }
        result = {}

        for label, ticker in tickers.items():
            logger.info(f"Fetching {ticker} ({label}) [{dr['start_date']} -> {dr['end_date']}]")
            try:
                data = yf.download(
                    ticker,
                    start=dr['start_date'],
                    end=dr['end_date'],
                    auto_adjust=True,
                    progress=False,
                )
                if not data.empty:
                    # 取 Adj Close (auto_adjust=True 时 Close 已是复权价)
                    close_col = 'Close'
                    prices = data[close_col]
                    if isinstance(prices, pd.DataFrame):
                        prices = prices.iloc[:, 0]
                    result[label] = {
                        str(k.date()): float(v) for k, v in prices.items()
                    }
                    logger.info(f"  {label}: {len(prices)} trading days")
                else:
                    logger.warning(f"  {label}: empty data")
                    result[label] = {}
            except Exception as e:
                logger.error(f"  {label}: FAILED - {e}")
                result[label] = {}

        ti.xcom_push(key='market_data', value=result)
        return {k: len(v) for k, v in result.items()}

    # ====================================================================
    # Task 3: 计算对数收益率 + 滚动相关系数 + 传染警报
    # ====================================================================
    def compute_contagion(**kwargs):
        """调用 B2 计算模块: process_contagion_group() + detect_contagion_alert()"""
        import pandas as pd

        ti = kwargs['ti']
        market_data = ti.xcom_pull(task_ids='fetch_market_data', key='market_data')

        # 反序列化为 pd.Series
        spy = pd.Series({pd.Timestamp(k): v for k, v in market_data.get('SPY', {}).items()})
        tlt = pd.Series({pd.Timestamp(k): v for k, v in market_data.get('TLT', {}).items()})
        move = pd.Series({pd.Timestamp(k): v for k, v in market_data.get('MOVE', {}).items()})

        if spy.empty or tlt.empty:
            raise ValueError("SPY or TLT data is empty, cannot compute contagion")

        # 计算滚动相关系数
        records = process_contagion_group(
            spy_prices=spy,
            tlt_prices=tlt,
            move_index=move if not move.empty else None,
        )

        # 检测市场传染警报
        alerts = detect_contagion_alert(records)
        alert_dates = list(set(a['trade_date'] for a in alerts))

        logger.info(
            f"Contagion: {len(records)} records, "
            f"{len(alert_dates)} alert dates: {alert_dates[:5]}"
        )

        ti.xcom_push(key='contagion_records', value=records)
        ti.xcom_push(key='alert_info', value={'count': len(alert_dates), 'dates': alert_dates})
        return len(records)

    # ====================================================================
    # Task 4: 质量门禁
    # ====================================================================
    def quality_gate(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='compute_contagion', key='contagion_records')
        return bool(records and len(records) > 100)

    # ====================================================================
    # Task 5: UPSERT 入库
    # ====================================================================
    def upsert_contagion_data(**kwargs):
        ti = kwargs['ti']
        records = ti.xcom_pull(task_ids='compute_contagion', key='contagion_records')
        count = push_calculated_results(
            table_name='market_contagion',
            records=records,
            conflict_columns=['trade_date', 'symbol'],
        )
        ti.xcom_push(key='upsert_count', value=count)
        return count

    # ====================================================================
    # Task 6: Webhook
    # ====================================================================
    def emit_webhook(**kwargs):
        ti = kwargs['ti']
        count = ti.xcom_pull(task_ids='upsert_contagion_data', key='upsert_count') or 0
        alerts = ti.xcom_pull(task_ids='compute_contagion', key='alert_info') or {}
        send_completion_webhook(
            dag_id=DAG_ID,
            data_group=DATA_GROUP,
            record_count=count,
            extra={'contagion_alerts': alerts.get('count', 0)},
        )

    # ====================================================================
    # DAG 编排
    # ====================================================================
    t1 = PythonOperator(task_id='compute_date_range', python_callable=compute_date_range)
    t2 = PythonOperator(task_id='fetch_market_data', python_callable=fetch_market_data)
    t3 = PythonOperator(task_id='compute_contagion', python_callable=compute_contagion)
    t4 = ShortCircuitOperator(task_id='quality_gate', python_callable=quality_gate)
    t5 = PythonOperator(task_id='upsert_contagion_data', python_callable=upsert_contagion_data)
    t6 = PythonOperator(task_id='emit_webhook', python_callable=emit_webhook)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
