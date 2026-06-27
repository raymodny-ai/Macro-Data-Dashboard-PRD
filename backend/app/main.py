"""
宏观流动性与资产定价状态识别系统 - FastAPI 主入口
架构原则:
  - SSE 主动推送 (废弃轮询) + Last-Event-ID 断线重连
  - 事件驱动缓存强驱逐 (Airflow/Celery Webhook → Redis DEL → SSE 广播)
  - 独立规则引擎热更新 (数据库字典表 → 内存原子替换 → 无重启)
  - CORS 中间件 + OpenAPI 文档
"""
import asyncio
import json
import logging
from datetime import datetime, date, timedelta
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Depends, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

import redis.asyncio as aioredis

from app.config import get_settings, Settings
from app.database import get_db, get_redis, engine
from app.services.cache_manager import CacheManager, TTL_AGGREGATE, TTL_TREND, TTL_REALTIME, TTL_SURFACE
from app.services.rule_engine import rule_engine
from app.services.state_engine import compute_dual_track_status
from app.services import data_service
from app.services.yield_curve_service import query_yield_curve_3d

logger = logging.getLogger(__name__)
settings = get_settings()


# ==========================================================================
# SSE 事件广播器 (增强: Last-Event-ID + 事件历史缓冲)
# ==========================================================================
class SSEBroadcaster:
    """
    SSE 事件广播器
    - 维护事件历史缓冲区 (最近 100 条), 支持 Last-Event-ID 恢复
    - 30 秒心跳保活, 防止连接超时
    """
    MAX_HISTORY = 100

    def __init__(self):
        self._clients: list[asyncio.Queue] = []
        self._history: list[dict] = []
        self._event_id: int = 0

    async def connect(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=50)
        self._clients.append(queue)
        return queue

    def disconnect(self, queue: asyncio.Queue):
        if queue in self._clients:
            self._clients.remove(queue)

    async def broadcast(self, event_type: str, data: dict):
        """广播事件到所有客户端 + 写入历史缓冲区"""
        self._event_id += 1
        message = {
            "id": str(self._event_id),
            "event": event_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat(),
        }
        # 写入历史 (环形缓冲)
        self._history.append(message)
        if len(self._history) > self.MAX_HISTORY:
            self._history = self._history[-self.MAX_HISTORY:]

        # 广播到所有客户端
        disconnected = []
        for queue in self._clients:
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                disconnected.append(queue)
        for q in disconnected:
            self.disconnect(q)

    def get_missed_events(self, last_event_id: Optional[str]) -> list[dict]:
        """获取 Last-Event-ID 之后遗漏的事件 (断线重连恢复)"""
        if not last_event_id or not self._history:
            return []
        try:
            last_id = int(last_event_id)
        except (ValueError, TypeError):
            return []
        return [e for e in self._history if int(e["id"]) > last_id]


sse_broadcaster = SSEBroadcaster()


# ==========================================================================
# 应用生命周期
# ==========================================================================
async def _create_redis_client() -> aioredis.Redis:
    """创建全局 Redis 客户端"""
    return aioredis.from_url(settings.redis_url, decode_responses=True)


_redis_client: Optional[aioredis.Redis] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动/关闭事件"""
    global _redis_client
    _redis_client = await _create_redis_client()

    # 启动时加载规则引擎
    try:
        await rule_engine.load_from_redis(_redis_client)
        logger.info(f"Rules engine loaded: v{rule_engine.version}, {len(rule_engine.get_all())} rules")
    except Exception as e:
        logger.warning(f"Rules engine startup load failed: {e}")

    logger.info(f"[STARTUP] 宏观流动性状态识别系统启动 | env={settings.fastapi_env}")
    yield

    if _redis_client:
        await _redis_client.close()
    await engine.dispose()
    logger.info("[SHUTDOWN] 系统关闭")


def get_redis_client() -> aioredis.Redis:
    """获取全局 Redis 客户端"""
    if _redis_client is None:
        raise RuntimeError("Redis client not initialized")
    return _redis_client


def get_cache_manager() -> CacheManager:
    """获取缓存管理器"""
    return CacheManager(get_redis_client())


# ==========================================================================
# FastAPI 应用实例
# ==========================================================================
app = FastAPI(
    title="宏观流动性与资产定价状态识别系统",
    description=(
        "状态识别者 — 独立双轨控制模型 API\n\n"
        "## 核心端点\n"
        "- **仪表板摘要**: `GET /api/v1/dashboard/summary` — 五组数据实时状态\n"
        "- **双轨判定**: `GET /api/v1/dual-track/status` — 宏观立场 × 流动性修补\n"
        "- **流动性走廊**: `GET /api/v1/liquidity/corridor` — SOFR/IORB/利差时序\n"
        "- **通胀趋势**: `GET /api/v1/inflation/trend` — CPI加速度+薪柴复燃\n"
        "- **SSE 事件流**: `GET /api/v1/events/stream` — 实时状态推送\n"
        "- **规则管理**: `GET/PUT /api/v1/rules/*` — 热更新阈值\n"
    ),
    version="0.4.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================================================
# 健康检查
# ==========================================================================
@app.get("/health", tags=["system"], summary="服务健康检查")
async def health_check():
    return {
        "status": "healthy",
        "service": "macro-dashboard-api",
        "version": "0.4.0",
        "timestamp": datetime.utcnow().isoformat(),
        "rules_version": rule_engine.version,
    }


# ==========================================================================
# SSE 事件流端点 (增强: Last-Event-ID 断线重连)
# ==========================================================================
@app.get("/api/v1/events/stream", tags=["sse"], summary="SSE 实时事件流")
async def sse_stream(last_event_id: Optional[str] = Header(None, alias="Last-Event-ID")):
    """
    SSE 事件流端点 (Server-Sent Events)

    - 30 秒心跳保活
    - 支持 Last-Event-ID 断线重连: 重连时自动补发遗漏事件
    - 事件类型: state_change / crisis_alert / cache_invalidated / heartbeat
    """
    from sse_starlette.sse import EventSourceResponse

    queue = await sse_broadcaster.connect()

    async def event_generator():
        try:
            # 补发遗漏事件 (断线重连恢复)
            missed = sse_broadcaster.get_missed_events(last_event_id)
            for event in missed:
                yield {
                    "id": event["id"],
                    "event": event["event"],
                    "data": json.dumps(event["data"]),
                }

            # 发送连接确认
            yield {
                "id": str(sse_broadcaster._event_id + 1),
                "event": "connected",
                "data": json.dumps({
                    "message": "SSE 连接已建立",
                    "missed_events": len(missed),
                    "timestamp": datetime.utcnow().isoformat(),
                }),
            }

            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {
                        "id": message["id"],
                        "event": message["event"],
                        "data": json.dumps(message["data"]),
                    }
                except asyncio.TimeoutError:
                    yield {"event": "heartbeat", "data": json.dumps({"timestamp": datetime.utcnow().isoformat()})}
        except asyncio.CancelledError:
            pass
        finally:
            sse_broadcaster.disconnect(queue)

    return EventSourceResponse(event_generator())


# ==========================================================================
# 仪表板摘要 API (B4 实现)
# ==========================================================================
@app.get("/api/v1/dashboard/summary", tags=["dashboard"], summary="五组数据聚合摘要")
async def dashboard_summary(db: AsyncSession = Depends(get_db)):
    """
    返回通胀/财政/流动性/AI CapEx/市场传染五组最新状态摘要

    缓存策略: TTL=3600s, Webhook 写入时自动驱逐
    """
    cache = get_cache_manager()
    result = await cache.get_or_compute(
        namespace="dashboard",
        key="summary",
        compute_fn=lambda: data_service.query_dashboard_summary(db),
        ttl=TTL_AGGREGATE,
    )
    return result


# ==========================================================================
# 双轨状态判定 API (B4 实现)
# ==========================================================================
@app.get("/api/v1/dual-track/status", tags=["dual-track"], summary="独立双轨状态判定")
async def dual_track_status(db: AsyncSession = Depends(get_db)):
    """
    独立双轨控制模型:
    - 第一轨: 宏观立场 (通胀加速度40% + AI CapEx增速30% + 工资动量30%)
    - 第二轨: 流动性修补 (SOFR-IORB利差50% + MOVE指数30% + 认购倍数20%)

    交叉判定: 区分"健康疼痛" vs "功能瘫痪", 识别"局部修补非降息信号"
    """
    cache = get_cache_manager()
    result = await cache.get_or_compute(
        namespace="dual_track",
        key="status",
        compute_fn=lambda: compute_dual_track_status(db),
        ttl=TTL_AGGREGATE,
    )
    return result


# ==========================================================================
# 流动性走廊 API (B4 实现)
# ==========================================================================
@app.get("/api/v1/liquidity/corridor", tags=["liquidity"], summary="流动性走廊时序数据")
async def liquidity_corridor(
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(180, ge=1, le=1000, description="返回条数"),
    offset: int = Query(0, ge=0, description="分页偏移"),
    db: AsyncSession = Depends(get_db),
):
    """
    返回 SOFR/IORB/利差时序数据及 system_state 历史变化

    system_state: 0=充裕, 1=紧张, 2=瘫痪
    """
    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None
    cache = get_cache_manager()
    cache_key = f"corridor_{start_date}_{end_date}_{limit}_{offset}"
    return await cache.get_or_compute(
        namespace="liquidity",
        key=cache_key,
        compute_fn=lambda: data_service.query_liquidity_corridor(db, sd, ed, limit, offset),
        ttl=TTL_TREND,
    )


# ==========================================================================
# 通胀趋势 API (B4 实现)
# ==========================================================================
@app.get("/api/v1/inflation/trend", tags=["inflation"], summary="通胀二阶导趋势分析")
async def inflation_trend(
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(180, ge=1, le=1000, description="返回条数"),
    db: AsyncSession = Depends(get_db),
):
    """
    返回核心 CPI/时薪的 MoM 增速和加速度曲线, 标注"薪柴复燃"预警时间点

    预警条件: 连续两月加速度 > 0 且斜率扩大
    """
    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None
    cache = get_cache_manager()
    cache_key = f"trend_{start_date}_{end_date}_{limit}"
    return await cache.get_or_compute(
        namespace="inflation",
        key=cache_key,
        compute_fn=lambda: data_service.query_inflation_trend(db, sd, ed, limit),
        ttl=TTL_TREND,
    )


# ==========================================================================
# 规则引擎管理 API (热更新)
# ==========================================================================
@app.get("/api/v1/rules", tags=["rules"], summary="获取全部规则阈值")
async def get_rules():
    """
    返回当前所有规则阈值 (DB 覆盖 + 默认值)

    规则组: liquidity / fiscal / dual_track / contagion / quality
    """
    from app.services.rule_engine import DEFAULT_RULES

    all_rules = rule_engine.get_all()
    result = {}
    for name, value in all_rules.items():
        meta = DEFAULT_RULES.get(name, {})
        result[name] = {
            "value": value,
            "description": meta.get("description", ""),
            "group": meta.get("group", "unknown"),
        }
    return {
        "rules": result,
        "engine_status": rule_engine.get_status(),
    }


@app.get("/api/v1/rules/{group}", tags=["rules"], summary="按组获取规则")
async def get_rules_by_group(group: str):
    """按数据组获取规则: liquidity / fiscal / dual_track / contagion / quality"""
    rules = rule_engine.get_rules_by_group(group)
    if not rules:
        raise HTTPException(status_code=404, detail=f"Rule group '{group}' not found")
    return {"group": group, "rules": rules}


@app.put("/api/v1/rules/{rule_name}", tags=["rules"], summary="热更新单条规则")
async def update_rule(
    rule_name: str,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    热更新规则阈值 (无需重启服务)

    流程: DB 写入 → 内存原子替换 → Redis 同步 → SSE 广播
    body: {"value": 0.05}
    """
    new_value = payload.get("value")
    if new_value is None:
        raise HTTPException(status_code=400, detail="'value' field is required")

    redis_client = get_redis_client()
    result = await rule_engine.update_rule(db, redis_client, rule_name, float(new_value))

    # SSE 广播规则变更事件
    await sse_broadcaster.broadcast("rules_updated", {
        "rule_name": rule_name,
        "new_value": float(new_value),
        "version": result["version"],
    })

    return result


@app.post("/api/v1/rules/refresh", tags=["rules"], summary="强制刷新规则引擎")
async def refresh_rules(db: AsyncSession = Depends(get_db)):
    """从数据库重新加载全部规则 (管理员操作)"""
    redis_client = get_redis_client()
    await rule_engine.load_from_db(db)
    await rule_engine.sync_to_redis(redis_client)
    return rule_engine.get_status()


# ==========================================================================
# 内部 Webhook: 缓存强驱逐 (B4 增强: 集成 CacheManager + SSE 广播)
# ==========================================================================
# ==========================================================================
# 3D 收益率曲面 API (B6 实现)
# ==========================================================================
@app.get("/api/v1/yield-curve/3d-surface", tags=["yield-curve"], summary="3D 收益率曲面压缩二维数组")
async def yield_curve_3d_surface(
    start_date: Optional[str] = Query(None, description="起始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit_days: int = Query(180, ge=30, le=365, description="返回天数"),
    db: AsyncSession = Depends(get_db),
):
    """
    返回压缩二维数组格式的收益率曲面数据

    格式:
      - dates: ["2024-01-01", ...]
      - terms: ["1M", "3M", ..., "30Y"]
      - yields: [[y1_t1, y1_t2, ...], ...]  # [time_idx][term_idx]
      - morphology_label: "Bear Steepener" | "Bear Flattener" | "Normal"

    严禁传输庞大 JSON 网格 (vertices/faces/colors), 由浏览器本地生成 BufferGeometry
    """
    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None
    cache = get_cache_manager()
    cache_key = f"surface_{start_date}_{end_date}_{limit_days}"
    return await cache.get_or_compute(
        namespace="yield_curve",
        key=cache_key,
        compute_fn=lambda: query_yield_curve_3d(db, sd, ed, limit_days),
        ttl=TTL_SURFACE,
    )


# ==========================================================================
# 内部 Webhook: 缓存强驱逐 (B4 增强: 集成 CacheManager + SSE 广播)
# ==========================================================================
@app.post("/api/v1/internal/cache-invalidate", tags=["internal"], summary="DAG Webhook 缓存驱逐")
async def cache_invalidate(
    payload: dict,
    x_webhook_secret: str = Header(default=""),
):
    """
    Airflow DAG 完成 Webhook 监听器

    接收 DAG 写入完成信号 → 按 data_group 批量驱逐 Redis 缓存 → SSE 广播
    确保大屏下一次读取的是经过修正的最新状态
    """
    if x_webhook_secret != settings.webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    data_group = payload.get("data_group", "*")
    dag_id = payload.get("dag_id", "unknown")
    record_count = payload.get("record_count", 0)
    changed_fields = payload.get("changed_fields", None)  # 局部刷新字段列表

    cache = get_cache_manager()

    if data_group == "*":
        # 全量驱逐
        removed = await cache.invalidate_pattern("cache:*")
    elif changed_fields:
        # 局部刷新: 仅驱逐指定字段 (减少前端不必要的全屏重绘)
        removed = await cache.invalidate_partial(data_group, changed_fields)
    else:
        # 按组驱逐 (含仪表板汇总)
        removed = await cache.invalidate_group(data_group)

    # SSE 广播
    event_data = {
        "dag_id": dag_id,
        "data_group": data_group,
        "record_count": record_count,
        "keys_removed": removed,
    }

    # 判断是否需要推送状态变更事件
    if data_group in ("liquidity", "inflation", "fiscal", "contagion", "treasury_yield"):
        await sse_broadcaster.broadcast("data_updated", event_data)
    else:
        await sse_broadcaster.broadcast("cache_invalidated", event_data)

    return {"status": "ok", "dag_id": dag_id, "data_group": data_group, "keys_removed": removed}
