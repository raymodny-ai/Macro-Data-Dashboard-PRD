"""
宏观流动性与资产定价状态识别系统 - 全局配置
架构原则: 敏感配置安全管理, 禁止硬编码
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """应用配置 - 从环境变量读取"""

    # FastAPI
    fastapi_host: str = "0.0.0.0"
    fastapi_port: int = 8000
    fastapi_env: str = "development"
    fastapi_debug: bool = True

    # TimescaleDB
    database_url: str = "postgresql+asyncpg://dashboard:password@timescaledb:5432/macro_dashboard"
    database_url_sync: str = "postgresql://dashboard:password@timescaledb:5432/macro_dashboard"

    # Redis
    redis_url: str = "redis://redis:6379/0"

    # 外部 API
    fred_api_key: str = ""
    alpha_vantage_key: str = ""
    sec_user_agent: str = ""

    # Webhook (DAG 完成 → 缓存驱逐)
    webhook_secret: str = ""

    # SSE (B4)
    sse_heartbeat_interval: int = 30  # 心跳间隔 (秒)
    sse_max_history: int = 100       # 事件历史缓冲条数

    # 缓存 TTL (秒, B4)
    cache_ttl_realtime: int = 60
    cache_ttl_trend: int = 300
    cache_ttl_aggregate: int = 3600
    cache_ttl_surface: int = 1800

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
