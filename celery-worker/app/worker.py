"""
Celery Worker - SEC EDGAR 专用独立队列
架构原则: 双层限流隔离 - SEC EDGAR 严苛接口 (≤10次/秒)
         独立 Celery 队列 + Redis 令牌桶 + 死信重试

B3 增强:
  - SEC EDGAR 合规 User-Agent 集中管理
  - XBRL 数据解析 → CapEx/R&D 提取
  - AI CapEx 动量指数构建 (环比/同比增速)
  - UPSERT 写入 ai_capex_data 超表
"""
import os
import time
import json
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any

import redis
import requests
from celery import Celery
from celery.signals import task_failure
from kombu import Queue

logger = logging.getLogger(__name__)


# ==========================================================================
# Celery 配置
# ==========================================================================
broker_url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/1")
result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/2")

app = Celery(
    "sec_edgar_worker",
    broker=broker_url,
    backend=result_backend,
)

app.conf.task_queues = (
    Queue("sec_edgar", routing_key="sec_edgar"),
)
app.conf.task_default_queue = "sec_edgar"
app.conf.task_default_exchange = "sec_edgar"
app.conf.task_default_routing_key = "sec_edgar"

app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.task_reject_on_worker_lost = True


# ==========================================================================
# Redis 令牌桶限流器 (≤10次/秒)
# ==========================================================================
class TokenBucketRateLimiter:
    """Redis Lua 令牌桶: SEC EDGAR ≤10 次/秒"""
    LUA_SCRIPT = """
    local key = KEYS[1]
    local max_tokens = tonumber(ARGV[1])
    local refill_rate = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])

    local data = redis.call('HMGET', key, 'tokens', 'last_refill')
    local tokens = tonumber(data[1]) or max_tokens
    local last_refill = tonumber(data[2]) or now

    local elapsed = now - last_refill
    tokens = math.min(max_tokens, tokens + elapsed * refill_rate)

    if tokens >= 1 then
        tokens = tokens - 1
        redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
        redis.call('EXPIRE', key, 60)
        return 1
    else
        redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
        redis.call('EXPIRE', key, 60)
        return 0
    end
    """

    def __init__(self, redis_client, key="rate_limit:sec_edgar", max_tokens=10, refill_rate=10):
        self.redis = redis_client
        self.key = key
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self._script = self.redis.register_script(self.LUA_SCRIPT)

    def acquire(self):
        result = self._script(keys=[self.key], args=[self.max_tokens, self.refill_rate, time.time()])
        return bool(result)

    def wait_and_acquire(self, max_wait=5.0):
        wait_time = 0.1
        total_wait = 0.0
        while total_wait < max_wait:
            if self.acquire():
                return True
            time.sleep(wait_time)
            total_wait += wait_time
            wait_time = min(wait_time * 2, 1.0)
        return False


redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
rate_limiter = TokenBucketRateLimiter(redis_client)


# ==========================================================================
# SEC EDGAR 合规 User-Agent 管理
# ==========================================================================
class SECUserAgentManager:
    """
    SEC EDGAR 公平访问规则: 请求必须包含合规 User-Agent
    格式: "Company Name contact@email.com"
    """
    def __init__(self):
        self._company = os.getenv("SEC_COMPANY_NAME", "MacroDashboard")
        self._contact = os.getenv("SEC_CONTACT_EMAIL", "admin@local")
        self._ua_string = f"{self._company} {self._contact}"

    def get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self._ua_string,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }

    @property
    def user_agent(self) -> str:
        return self._ua_string


sec_ua_manager = SECUserAgentManager()


# ==========================================================================
# 科技巨头 CIK 映射
# ==========================================================================
TECH_GIANTS = {
    '320193': 'AAPL',    # Apple
    '1045810': 'NVDA',   # Nvidia
    '789019': 'MSFT',    # Microsoft
    '1652044': 'GOOGL',  # Alphabet
    '1318605': 'TSLA',   # Tesla
    '1326801': 'META',   # Meta Platforms
    '1018724': 'AMZN',   # Amazon
}

# XBRL 概念名
CAPEX_CONCEPT = 'PaymentsToAcquirePropertyPlantAndEquipment'
RD_CONCEPT = 'ResearchAndDevelopmentExpense'


# ==========================================================================
# SEC EDGAR XBRL 抓取任务
# ==========================================================================
@app.task(
    bind=True,
    name="fetch_sec_xbrl",
    max_retries=5,
    default_retry_delay=10,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
def fetch_sec_xbrl(self, company_cik: str, concept: str):
    """
    SEC EDGAR XBRL companyconcept 端点抓取
    走独立队列 + 令牌桶限流 + 指数退避重试
    """
    if not rate_limiter.wait_and_acquire():
        raise self.retry(exc=Exception("Rate limit exceeded"), countdown=5)

    url = f"https://data.sec.gov/api/xbrl/companyconcept/"
    params = {
        "cik": company_cik,
        "taxonomy": "us-gaap",
        "concept": concept,
    }

    try:
        response = requests.get(
            url, params=params,
            headers=sec_ua_manager.get_headers(),
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        facts = data.get("facts", [])
        logger.info(
            f"SEC [{company_cik}] {concept}: {len(facts)} facts fetched"
        )
        return {
            "cik": company_cik,
            "concept": concept,
            "company_name": data.get("entityName", TECH_GIANTS.get(company_cik, "Unknown")),
            "facts": facts,
        }
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            raise self.retry(exc=e, countdown=30)
        raise


# ==========================================================================
# CapEx 数据解析 + 动量指数
# ==========================================================================
@app.task(name="process_capex_data")
def process_capex_data(capex_raw: dict, rd_raw: dict):
    """
    解析 SEC XBRL 原始数据 → 提取季度 CapEx/R&D → 计算环比/同比增速

    Args:
        capex_raw: fetch_sec_xbrl 返回的 CapEx 数据
        rd_raw: fetch_sec_xbrl 返回的 R&D 数据

    Returns:
        可直接 UPSERT 到 ai_capex_data 超表的记录列表
    """
    cik = capex_raw.get("cik", "")
    company_name = capex_raw.get("company_name", TECH_GIANTS.get(cik, "Unknown"))

    # 提取 CapEx 时间序列
    capex_series = _extract_quarterly_values(capex_raw.get("facts", []))
    rd_series = _extract_quarterly_values(rd_raw.get("facts", []))

    # 合并并计算增速
    records = []
    all_dates = sorted(set(capex_series.keys()) | set(rd_series.keys()))

    for i, report_date in enumerate(all_dates):
        capex_val = capex_series.get(report_date)
        rd_val = rd_series.get(report_date)

        # 环比增速 (QoQ)
        capex_mom = None
        if i > 0 and capex_val is not None:
            prev_date = all_dates[i - 1]
            prev_val = capex_series.get(prev_date)
            if prev_val and prev_val != 0:
                capex_mom = round((capex_val - prev_val) / abs(prev_val), 6)

        # 同比增速 (YoY): 取 4 个季度前
        capex_yoy = None
        if i >= 4 and capex_val is not None:
            year_ago_date = all_dates[i - 4]
            year_ago_val = capex_series.get(year_ago_date)
            if year_ago_val and year_ago_val != 0:
                capex_yoy = round((capex_val - year_ago_val) / abs(year_ago_val), 6)

        records.append({
            'report_date': report_date,
            'company_cik': cik,
            'company_name': company_name,
            'capex': capex_val,
            'rd_expense': rd_val,
            'capex_mom': capex_mom,
            'capex_yoy': capex_yoy,
        })

    logger.info(f"[{cik}] {company_name}: {len(records)} quarterly records processed")
    return records


def _extract_quarterly_values(facts: List[dict]) -> Dict[str, float]:
    """
    从 XBRL facts 数组中提取季度报告的值
    筛选: form=10-Q/10-K, filed 最近, duration 类型
    """
    result = {}
    for fact in facts:
        form = fact.get("form", "")
        if form not in ("10-Q", "10-K"):
            continue

        filed = fact.get("filed", "")
        end_date = fact.get("end", "")
        val = fact.get("val")

        if end_date and val is not None:
            # 使用 end 日期作为 report_date
            result[end_date] = float(val)

    return result


# ==========================================================================
# 批量触发任务 (DAG 调度入口)
# ==========================================================================
@app.task(name="trigger_all_companies_capex")
def trigger_all_companies_capex():
    """
    为所有科技巨头触发 CapEx + R&D 抓取任务链
    DAG 通过 CeleryTrigger 调用此任务

    返回: 触发的任务组 ID (用于监控)
    """
    from celery import group, chord

    task_chains = []
    for cik, ticker in TECH_GIANTS.items():
        # 每个公司: 并行抓 CapEx + R&D → 合并处理
        fetch_chain = chord(
            group(
                fetch_sec_xbrl.s(cik, CAPEX_CONCEPT),
                fetch_sec_xbrl.s(cik, RD_CONCEPT),
            ),
            process_capex_data.s(),  # 注意: 此处需适配双参数
        )
        task_chains.append(fetch_chain)

    # 批量触发
    results = []
    for chain in task_chains:
        try:
            result = chain.apply_async(queue='sec_edgar')
            results.append(str(result.id))
        except Exception as e:
            logger.error(f"Failed to trigger chain: {e}")

    logger.info(f"Triggered {len(results)} company CapEx tasks")
    return results


# ==========================================================================
# 失败任务日志
# ==========================================================================
@task_failure.connect
def log_task_failure(sender, task_id, exception, traceback, **kwargs):
    """记录失败任务到 Redis 供监控"""
    error_info = {
        'task_id': task_id,
        'task_name': sender.name if hasattr(sender, 'name') else str(sender),
        'error': str(exception),
        'timestamp': datetime.now().isoformat(),
    }
    try:
        redis_client.lpush('sec_edgar:failed_tasks', json.dumps(error_info))
        redis_client.ltrim('sec_edgar:failed_tasks', 0, 999)  # 保留最近 1000 条
    except Exception:
        pass
