"""
Redis 缓存管理器
架构原则:
  - 事件驱动缓存强驱逐: DAG Webhook → Redis DEL → SSE 广播
  - 三级缓存 TTL: 短(60s 实时指标) / 中(300s 趋势) / 长(3600s 聚合)
  - 3D 收益率曲面 + 双轨状态矩阵计算成本高, 必须依赖 Redis 缓存
"""
import json
import hashlib
import logging
from typing import Any, Optional
from datetime import timedelta

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# 缓存 TTL 分级
TTL_REALTIME = timedelta(seconds=60)       # 实时指标 (SSE 降级时)
TTL_TREND = timedelta(seconds=300)         # 趋势数据 (流动性走廊/通胀曲线)
TTL_AGGREGATE = timedelta(seconds=3600)    # 聚合结果 (仪表板摘要/双轨状态)
TTL_SURFACE = timedelta(seconds=1800)      # 3D 曲面 (计算成本最高)

# 缓存 key 前缀
CACHE_PREFIX = "cache"


class CacheManager:
    """
    Redis 缓存管理器 (异步)

    用法:
        cache = CacheManager(redis_client)
        data = await cache.get("dashboard:summary")
        if data is None:
            data = await compute_dashboard()
            await cache.set("dashboard:summary", data, ttl=TTL_AGGREGATE)
    """

    def __init__(self, redis: aioredis.Redis):
        self._redis = redis

    def _key(self, namespace: str, key: str) -> str:
        """生成完整缓存 key: cache:{namespace}:{key}"""
        return f"{CACHE_PREFIX}:{namespace}:{key}"

    def _hash_key(self, params: dict) -> str:
        """对查询参数生成短哈希, 确保相同参数命中同一缓存"""
        sorted_params = json.dumps(params, sort_keys=True)
        return hashlib.md5(sorted_params.encode()).hexdigest()[:12]

    async def get(self, namespace: str, key: str) -> Optional[Any]:
        """读取缓存, 返回 None 表示缓存未命中"""
        full_key = self._key(namespace, key)
        raw = await self._redis.get(full_key)
        if raw is None:
            logger.debug(f"Cache MISS: {full_key}")
            return None
        logger.debug(f"Cache HIT: {full_key}")
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    async def set(self, namespace: str, key: str, value: Any, ttl: timedelta = TTL_TREND):
        """写入缓存, 带 TTL"""
        full_key = self._key(namespace, key)
        serialized = json.dumps(value, default=str, ensure_ascii=False)
        await self._redis.set(full_key, serialized, ex=int(ttl.total_seconds()))
        logger.debug(f"Cache SET: {full_key} (TTL={ttl.total_seconds()}s)")

    async def invalidate_pattern(self, pattern: str) -> int:
        """
        按 pattern 批量驱逐缓存 (事件驱动)
        例: "cache:liquidity:*" 驱逐所有流动性相关缓存
        """
        keys = []
        async for key in self._redis.scan_iter(match=pattern):
            keys.append(key)
        if keys:
            await self._redis.delete(*keys)
        logger.info(f"Cache INVALIDATE: pattern={pattern}, removed={len(keys)}")
        return len(keys)

    async def invalidate_group(self, data_group: str) -> int:
        """
        按数据组驱逐缓存 (Webhook 监听器入口)
        data_group: liquidity / inflation / fiscal / ai_capex / contagion / dashboard
        """
        # 驱逐目标组 + 仪表板汇总 (依赖所有组)
        patterns = [
            f"cache:{data_group}:*",
        ]
        if data_group != "dashboard":
            patterns.append("cache:dashboard:*")

        total = 0
        for pattern in patterns:
            total += await self.invalidate_pattern(pattern)
        return total

    async def get_or_compute(
        self,
        namespace: str,
        key: str,
        compute_fn,
        ttl: timedelta = TTL_TREND,
    ) -> Any:
        """
        缓存穿透保护: 命中则直接返回, 未命中则计算后写入缓存

        Args:
            namespace: 缓存分组
            key: 缓存 key
            compute_fn: async callable, 缓存未命中时调用
            ttl: 过期时间
        """
        cached = await self.get(namespace, key)
        if cached is not None:
            return cached
        result = await compute_fn()
        await self.set(namespace, key, result, ttl)
        return result

    async def health_check(self) -> dict:
        """Redis 健康检查"""
        try:
            info = await self._redis.info(section="memory")
            return {
                "status": "ok",
                "used_memory_human": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", "N/A"),
            }
        except Exception as e:
            return {"status": "error", "detail": str(e)}
