"""
DAG 变量与密钥安全管理
架构原则: 敏感配置集中管理, 禁止硬编码

层级优先级 (高→低):
  1. Airflow Connections / Variables (生产环境推荐)
  2. 环境变量 (.env 文件)
  3. 硬编码默认值 (仅用于开发)

使用方式:
  from lib.secrets import get_secret, get_fred_api_key, get_db_config

  # 通用密钥获取
  api_key = get_secret('FRED_API_KEY', default=None, required=True)

  # 便捷函数
  fred_key = get_fred_api_key()
  db_config = get_db_config()
"""
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# 尝试导入 Airflow Variables (仅在 Airflow 环境中可用)
_airflow_available = False
try:
    from airflow.models import Variable
    _airflow_available = True
except ImportError:
    pass


def get_secret(
    key: str,
    default: Optional[str] = None,
    required: bool = False,
) -> Optional[str]:
    """
    通用密钥获取函数 (三层优先级)

    优先级:
      1. Airflow Variables (如果可用)
      2. 环境变量
      3. default 默认值

    Args:
        key: 密钥名称 (如 'FRED_API_KEY')
        default: 默认值
        required: 是否必须存在 (缺失时抛异常)

    Returns:
        密钥值或 None

    Raises:
        ValueError: required=True 且密钥不存在
    """
    # Layer 1: Airflow Variables
    if _airflow_available:
        try:
            value = Variable.get(key, default_var=None)
            if value is not None:
                return value
        except Exception:
            pass

    # Layer 2: 环境变量
    value = os.getenv(key)
    if value is not None:
        return value

    # Layer 3: 默认值
    if required and default is None:
        raise ValueError(
            f"Required secret '{key}' not found in "
            f"{'Airflow Variables, ' if _airflow_available else ''}"
            f"environment variables, or defaults"
        )

    return default


# ==========================================================================
# 便捷函数: 常用密钥
# ==========================================================================

def get_fred_api_key() -> str:
    """获取 FRED API Key (必需)"""
    key = get_secret('FRED_API_KEY', required=True)
    if not key:
        raise ValueError("FRED_API_KEY is required but not configured")
    return key


def get_alpha_vantage_key() -> Optional[str]:
    """获取 Alpha Vantage API Key (可选)"""
    return get_secret('ALPHA_VANTAGE_KEY')


def get_sec_user_agent() -> str:
    """获取 SEC EDGAR 合规 User-Agent"""
    company = get_secret('SEC_COMPANY_NAME', default='MacroDashboard')
    contact = get_secret('SEC_CONTACT_EMAIL', default='admin@local')
    return f"{company} {contact}"


def get_webhook_config() -> Dict[str, str]:
    """获取 Webhook 配置"""
    return {
        'url': get_secret(
            'WEBHOOK_URL',
            default='http://fastapi:8000/api/v1/internal/cache-invalidate',
        ),
        'secret': get_secret('WEBHOOK_SECRET', default=''),
    }


def get_db_config() -> Dict[str, Any]:
    """获取数据库连接配置"""
    return {
        'host': get_secret('POSTGRES_HOST', default='timescaledb'),
        'port': int(get_secret('POSTGRES_PORT', default='5432')),
        'dbname': get_secret('POSTGRES_DB', default='macro_dashboard'),
        'user': get_secret('POSTGRES_USER', default='dashboard'),
        'password': get_secret('POSTGRES_PASSWORD', default=''),
    }


def get_redis_config() -> Dict[str, str]:
    """获取 Redis 连接配置"""
    return {
        'url': get_secret('REDIS_URL', default='redis://redis:6379/0'),
    }


# ==========================================================================
# 密钥健康检查 (DAG 启动前调用)
# ==========================================================================

def validate_required_secrets() -> Dict[str, bool]:
    """
    验证所有必需密钥是否已配置
    建议在 DAG 的 PythonOperator 初始化时调用

    Returns:
        {secret_name: is_configured}
    """
    required_keys = [
        'FRED_API_KEY',
        'POSTGRES_HOST',
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'WEBHOOK_SECRET',
    ]

    status = {}
    for key in required_keys:
        value = get_secret(key)
        status[key] = value is not None and value != ''

    missing = [k for k, v in status.items() if not v]
    if missing:
        logger.warning(f"Missing required secrets: {missing}")
    else:
        logger.info("All required secrets validated")

    return status
