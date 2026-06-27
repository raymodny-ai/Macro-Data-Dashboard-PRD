"""
Airflow 侧 API 限流器
架构原则: 双层限流隔离
  - 宽容接口 (FRED/Fiscal Data): Airflow Worker 进程内限流
  - 严苛接口 (SEC EDGAR): 独立 Celery + Redis 令牌桶 (见 celery-worker/)
"""
import time
import threading
from collections import defaultdict
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class SlidingWindowRateLimiter:
    """
    滑动窗口限流器 (线程安全)
    用于 FRED 等宽容接口, 防止并发请求过猛
    
    使用示例:
        fred_limiter = SlidingWindowRateLimiter(max_calls=10, window_seconds=1.0)
        fred_limiter.wait_and_acquire()  # 阻塞直到获取许可
    """

    def __init__(self, max_calls: int, window_seconds: float, name: str = "api"):
        """
        Args:
            max_calls: 窗口内最大调用次数
            window_seconds: 窗口时长 (秒)
            name: 限流器标识名 (日志用)
        """
        self.max_calls = max_calls
        self.window = window_seconds
        self.name = name
        self._timestamps: list = []
        self._lock = threading.Lock()

    def _cleanup(self, now: float):
        """清理窗口外的过期时间戳"""
        cutoff = now - self.window
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.pop(0)

    def acquire(self) -> bool:
        """尝试获取许可 (非阻塞), 返回 True/False"""
        now = time.time()
        with self._lock:
            self._cleanup(now)
            if len(self._timestamps) < self.max_calls:
                self._timestamps.append(now)
                return True
            return False

    def wait_and_acquire(self, timeout: float = 30.0) -> bool:
        """
        阻塞等待获取许可
        
        Args:
            timeout: 最大等待时间 (秒)
        
        Returns:
            True 表示获取成功, False 表示超时
        """
        start = time.time()
        while time.time() - start < timeout:
            if self.acquire():
                return True
            # 计算下次窗口释放时间
            with self._lock:
                if self._timestamps:
                    wait_until = self._timestamps[0] + self.window
                    sleep_time = max(0.01, wait_until - time.time())
                else:
                    sleep_time = 0.01
            time.sleep(min(sleep_time, 1.0))
        
        logger.warning(f"[{self.name}] Rate limiter timeout after {timeout}s")
        return False

    @property
    def available(self) -> int:
        """当前窗口内剩余可用次数"""
        now = time.time()
        with self._lock:
            self._cleanup(now)
            return max(0, self.max_calls - len(self._timestamps))


# ============================================================================
# 全局限流器实例 (FRED 宽容接口)
# ============================================================================
# FRED API: 建议 ≤120次/分钟, 保守设置 10次/秒
fred_rate_limiter = SlidingWindowRateLimiter(
    max_calls=10,
    window_seconds=1.0,
    name="FRED",
)

# Fiscal Data Treasury: 相对宽容, 20次/秒
fiscal_rate_limiter = SlidingWindowRateLimiter(
    max_calls=20,
    window_seconds=1.0,
    name="FiscalData",
)
