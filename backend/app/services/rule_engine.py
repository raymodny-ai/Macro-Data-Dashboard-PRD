"""
独立规则引擎 (热更新)
架构原则:
  - 判定"红/黄/绿灯"的硬编码阈值抽离为数据库字典表
  - 基准环境变化时无需重新部署代码即可热更新预警线
  - 规则版本化 + 原子切换 (双缓冲模式) 避免竞态
  - 提供管理接口支持运行时修改阈值并即时生效

热更新流程:
  1. 管理员通过 API 修改规则 → 写入 DB + 版本号+1
  2. RuleEngine 检测版本号变化 → 原子替换内存中的规则集
  3. 所有 API 端点自动使用最新规则, 无重启
"""
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# Redis 缓存 key (规则集快照)
RULES_CACHE_KEY = "rules:active"
RULES_VERSION_KEY = "rules:version"


# ==========================================================================
# 默认规则集 (硬编码 fallback, 仅当 DB 不可用时使用)
# ==========================================================================
DEFAULT_RULES: Dict[str, Dict[str, Any]] = {
    # --- 流动性走廊 ---
    "spread_tight_threshold": {
        "value": -0.03,
        "description": "SOFR-IORB 利差 < 此值 → 状态充裕(0)",
        "group": "liquidity",
    },
    "spread_stress_threshold": {
        "value": 0.0,
        "description": "SOFR-IORB 利差 > 此值 → 状态瘫痪(2)",
        "group": "liquidity",
    },
    "crisis_burst_bps": {
        "value": 0.10,
        "description": "单日利差飙升 >= 此值(bp) → 水管爆裂预警",
        "group": "liquidity",
    },
    # --- 财政增量 ---
    "bid_to_cover_threshold": {
        "value": 2.4,
        "description": "认购倍数 < 此值 → 认购不足",
        "group": "fiscal",
    },
    "acm_premium_threshold": {
        "value": 0.01,
        "description": "ACM 期限溢价 > 此值 → 期限溢价异常",
        "group": "fiscal",
    },
    # --- 宏观紧缩指数权重 ---
    "macro_weight_inflation": {
        "value": 0.40,
        "description": "宏观紧缩指数: 通胀加速度权重",
        "group": "dual_track",
    },
    "macro_weight_capex": {
        "value": 0.30,
        "description": "宏观紧缩指数: AI CapEx 增速权重",
        "group": "dual_track",
    },
    "macro_weight_wage": {
        "value": 0.30,
        "description": "宏观紧缩指数: 工资动量权重",
        "group": "dual_track",
    },
    # --- 功能瘫痪仪表盘权重 ---
    "paralysis_weight_spread": {
        "value": 0.50,
        "description": "流动性风险评分: SOFR-IORB 利差权重",
        "group": "dual_track",
    },
    "paralysis_weight_move": {
        "value": 0.30,
        "description": "流动性风险评分: MOVE 指数权重",
        "group": "dual_track",
    },
    "paralysis_weight_btc": {
        "value": 0.20,
        "description": "流动性风险评分: 认购倍数倒数权重",
        "group": "dual_track",
    },
    # --- 双轨阈值 ---
    "macro_restrictive_high": {
        "value": 75,
        "description": "宏观紧缩指数 > 此值 → '高息压制环境'",
        "group": "dual_track",
    },
    "liquidity_risk_red": {
        "value": 80,
        "description": "流动性风险评分 > 此值 → 红灯警告",
        "group": "dual_track",
    },
    # --- 市场传染 ---
    "contagion_spy_drop_pct": {
        "value": -0.02,
        "description": "SPY 单日跌幅 > 此值 → 传染条件1",
        "group": "contagion",
    },
    "contagion_corr_threshold": {
        "value": 0.5,
        "description": "30日滚动相关系数 > 此值 → 传染条件2",
        "group": "contagion",
    },
    "contagion_move_threshold": {
        "value": 120,
        "description": "MOVE 指数 > 此值 → 传染条件3",
        "group": "contagion",
    },
    # --- 数据质量 ---
    "quality_min_pass_rate": {
        "value": 0.80,
        "description": "数据质量门禁最低通过率",
        "group": "quality",
    },
}


class RuleEngine:
    """
    规则引擎 (异步, 支持热更新)

    规则来源优先级:
      1. Redis 缓存 (最快)
      2. 数据库 rules_config 表 (持久化)
      3. DEFAULT_RULES 硬编码 (fallback)

    用法:
        engine = RuleEngine(db_session, redis_client)
        await engine.refresh()  # 从 DB 加载到 Redis
        threshold = engine.get("spread_stress_threshold")  # 0.0
    """

    def __init__(self):
        self._rules: Dict[str, float] = {}
        self._version: int = 0
        self._loaded_at: Optional[datetime] = None

    def get(self, rule_name: str, default: Optional[float] = None) -> float:
        """获取规则阈值 (内存直读, 无 I/O)"""
        if rule_name in self._rules:
            return self._rules[rule_name]
        # fallback to default
        if rule_name in DEFAULT_RULES:
            return DEFAULT_RULES[rule_name]["value"]
        if default is not None:
            return default
        raise KeyError(f"Rule '{rule_name}' not found")

    def get_all(self) -> Dict[str, float]:
        """获取全部规则"""
        result = {k: v["value"] for k, v in DEFAULT_RULES.items()}
        result.update(self._rules)  # DB 覆盖 default
        return result

    def get_rules_by_group(self, group: str) -> Dict[str, float]:
        """按组获取规则"""
        result = {}
        for name, rule in DEFAULT_RULES.items():
            if rule["group"] == group:
                result[name] = self._rules.get(name, rule["value"])
        return result

    @property
    def version(self) -> int:
        return self._version

    @property
    def loaded_at(self) -> Optional[datetime]:
        return self._loaded_at

    async def load_from_db(self, db: AsyncSession):
        """
        从数据库 rules_config 表加载规则
        原子替换: 先在临时 dict 中构建, 完成后一次性赋值
        """
        try:
            result = await db.execute(text("SELECT rule_name, rule_value, version FROM rules_config ORDER BY rule_name"))
            rows = result.fetchall()
        except Exception as e:
            logger.warning(f"Failed to load rules from DB: {e}, using defaults")
            return

        if not rows:
            logger.info("No rules in DB, using defaults")
            return

        # 原子替换 (双缓冲)
        new_rules = {}
        max_version = 0
        for row in rows:
            rule_name, rule_value, version = row[0], float(row[1]), row[2]
            new_rules[rule_name] = rule_value
            max_version = max(max_version, version)

        # 仅在版本号更高时更新
        if max_version > self._version:
            self._rules = new_rules
            self._version = max_version
            self._loaded_at = datetime.utcnow()
            logger.info(f"Rules loaded from DB: {len(new_rules)} rules, v{max_version}")

    async def load_from_redis(self, redis: aioredis.Redis):
        """从 Redis 缓存加载规则 (启动加速)"""
        try:
            cached = await redis.get(RULES_CACHE_KEY)
            version = await redis.get(RULES_VERSION_KEY)
            if cached and version:
                new_rules = json.loads(cached)
                new_version = int(version)
                if new_version > self._version:
                    self._rules = new_rules
                    self._version = new_version
                    self._loaded_at = datetime.utcnow()
                    logger.info(f"Rules loaded from Redis: {len(new_rules)} rules, v{new_version}")
        except Exception as e:
            logger.debug(f"Redis rules cache miss: {e}")

    async def sync_to_redis(self, redis: aioredis.Redis):
        """将当前规则集同步到 Redis (DB 写入后调用)"""
        all_rules = self.get_all()
        await redis.set(RULES_CACHE_KEY, json.dumps(all_rules))
        await redis.set(RULES_VERSION_KEY, str(self._version))
        logger.info(f"Rules synced to Redis: {len(all_rules)} rules, v{self._version}")

    async def update_rule(
        self,
        db: AsyncSession,
        redis: aioredis.Redis,
        rule_name: str,
        new_value: float,
    ):
        """
        热更新单条规则
        流程: DB 写入 → 内存更新 → Redis 同步 → SSE 广播
        """
        new_version = self._version + 1

        # 1. 写入 DB (UPSERT)
        await db.execute(
            text("""
                INSERT INTO rules_config (rule_name, rule_value, version, updated_at)
                VALUES (:name, :value, :version, NOW())
                ON CONFLICT (rule_name) DO UPDATE
                SET rule_value = :value, version = :version, updated_at = NOW()
            """),
            {"name": rule_name, "value": new_value, "version": new_version},
        )
        await db.commit()

        # 2. 原子更新内存
        self._rules[rule_name] = new_value
        self._version = new_version
        self._loaded_at = datetime.utcnow()

        # 3. 同步 Redis
        await self.sync_to_redis(redis)

        logger.info(f"Rule updated: {rule_name} = {new_value} (v{new_version})")
        return {"rule_name": rule_name, "value": new_value, "version": new_version}

    def get_status(self) -> dict:
        """获取引擎状态快照"""
        return {
            "version": self._version,
            "rules_count": len(self.get_all()),
            "loaded_at": self._loaded_at.isoformat() if self._loaded_at else None,
            "db_overrides": len(self._rules),
        }


# 全局单例
rule_engine = RuleEngine()
