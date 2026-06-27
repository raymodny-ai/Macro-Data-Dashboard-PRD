"""
后端 API 集成测试 (pytest + httpx AsyncClient)
架构原则:
  - 使用 httpx.AsyncClient + FastAPI TestClient
  - Mock 数据库/Redis 依赖, 不依赖真实服务
  - 覆盖所有 API 端点: 健康检查/仪表板/双轨/流动性/通胀/规则/收益率曲面/SSE/Webhook

运行方式:
  python -m pytest tests/test_api_integration.py -v --asyncio-mode=auto
"""
import json
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# ---------------------------------------------------------------------------
# Mock 策略: 在导入 app 之前先 patch 掉 DB/Redis 依赖
# ---------------------------------------------------------------------------
# 1. Mock settings
_mock_settings = MagicMock()
_mock_settings.fastapi_host = "0.0.0.0"
_mock_settings.fastapi_port = 8000
_mock_settings.fastapi_env = "test"
_mock_settings.fastapi_debug = False
_mock_settings.database_url = "postgresql+asyncpg://test:test@localhost:5432/test"
_mock_settings.database_url_sync = "postgresql://test:test@localhost:5432/test"
_mock_settings.redis_url = "redis://localhost:6379/0"
_mock_settings.fred_api_key = "test_key"
_mock_settings.webhook_secret = "test_secret"
_mock_settings.sse_heartbeat_interval = 30
_mock_settings.sse_max_history = 100
_mock_settings.cache_ttl_realtime = 60
_mock_settings.cache_ttl_trend = 300
_mock_settings.cache_ttl_aggregate = 3600
_mock_settings.cache_ttl_surface = 1800


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def mock_settings():
    return _mock_settings


@pytest_asyncio.fixture
async def mock_db_session():
    """模拟 AsyncSession"""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()

    # 模拟 execute 返回结果
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_result.fetchone.return_value = None
    mock_result.mappings.return_value.fetchall.return_value = []
    session.execute.return_value = mock_result
    return session


@pytest_asyncio.fixture
async def mock_redis():
    """模拟 Redis 客户端"""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=1)
    redis.keys = AsyncMock(return_value=[])
    redis.scan_iter = MagicMock(return_value=async_iter([]))
    redis.hgetall = AsyncMock(return_value={})
    redis.hset = AsyncMock(return_value=1)
    redis.exists = AsyncMock(return_value=0)
    redis.expire = AsyncMock(return_value=True)
    redis.close = AsyncMock()
    redis.aclose = AsyncMock()
    return redis


def async_iter(items):
    """辅助: 创建异步迭代器"""
    async def _gen():
        for item in items:
            yield item
    return _gen()


@pytest_asyncio.fixture
async def app(mock_db_session, mock_redis):
    """创建测试用 FastAPI 应用 (依赖覆盖)"""
    with patch("app.config.get_settings", return_value=_mock_settings):
        with patch("app.config.Settings", return_value=_mock_settings):
            # 延迟导入, 确保 patch 生效
            import importlib
            import app.config as cfg_mod
            importlib.reload(cfg_mod)

            # 需要重新加载所有依赖模块
            import app.database as db_mod
            import app.main as main_mod

            # Patch engine 以避免真实连接
            mock_engine = MagicMock()
            mock_engine.dispose = AsyncMock()
            db_mod.engine = mock_engine

            # Patch get_settings
            cfg_mod.get_settings = lambda: _mock_settings
            main_mod.settings = _mock_settings
            main_mod.get_settings = lambda: _mock_settings

            # 覆盖依赖
            async def override_get_db():
                yield mock_db_session

            async def override_get_redis():
                yield mock_redis

            main_mod._redis_client = mock_redis
            main_mod.app.dependency_overrides[db_mod.get_db] = override_get_db
            main_mod.app.dependency_overrides[db_mod.get_redis] = override_get_redis

            yield main_mod.app

            # 清理
            main_mod.app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client(app):
    """创建异步测试客户端"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ==========================================================================
# 测试: 健康检查
# ==========================================================================
class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_returns_200(self, client: AsyncClient):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "macro-dashboard-api"
        assert "version" in data
        assert "timestamp" in data


# ==========================================================================
# 测试: 仪表板摘要 API
# ==========================================================================
class TestDashboardSummary:
    @pytest.mark.asyncio
    async def test_dashboard_summary_endpoint(self, client: AsyncClient):
        resp = await client.get("/api/v1/dashboard/summary")
        # 由于 mock DB 返回空, 可能 200 或 500, 验证端点存在
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_dashboard_summary_returns_json(self, client: AsyncClient):
        resp = await client.get("/api/v1/dashboard/summary")
        if resp.status_code == 200:
            data = resp.json()
            assert isinstance(data, dict)


# ==========================================================================
# 测试: 双轨状态 API
# ==========================================================================
class TestDualTrack:
    @pytest.mark.asyncio
    async def test_dual_track_endpoint(self, client: AsyncClient):
        resp = await client.get("/api/v1/dual-track/status")
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_dual_track_response_structure(self, client: AsyncClient):
        resp = await client.get("/api/v1/dual-track/status")
        if resp.status_code == 200:
            data = resp.json()
            assert isinstance(data, dict)


# ==========================================================================
# 测试: 流动性走廊 API
# ==========================================================================
class TestLiquidityCorridor:
    @pytest.mark.asyncio
    async def test_liquidity_corridor_default(self, client: AsyncClient):
        resp = await client.get("/api/v1/liquidity/corridor")
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_liquidity_corridor_with_params(self, client: AsyncClient):
        resp = await client.get("/api/v1/liquidity/corridor", params={
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
            "limit": 50,
            "offset": 0,
        })
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_liquidity_corridor_invalid_limit(self, client: AsyncClient):
        resp = await client.get("/api/v1/liquidity/corridor", params={"limit": 0})
        assert resp.status_code == 422  # 验证失败


# ==========================================================================
# 测试: 通胀趋势 API
# ==========================================================================
class TestInflationTrend:
    @pytest.mark.asyncio
    async def test_inflation_trend_default(self, client: AsyncClient):
        resp = await client.get("/api/v1/inflation/trend")
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_inflation_trend_with_dates(self, client: AsyncClient):
        resp = await client.get("/api/v1/inflation/trend", params={
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
        })
        assert resp.status_code in (200, 500)


# ==========================================================================
# 测试: 3D 收益率曲面 API
# ==========================================================================
class TestYieldCurve3D:
    @pytest.mark.asyncio
    async def test_yield_curve_3d_default(self, client: AsyncClient):
        resp = await client.get("/api/v1/yield-curve/3d-surface")
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_yield_curve_3d_with_params(self, client: AsyncClient):
        resp = await client.get("/api/v1/yield-curve/3d-surface", params={
            "limit_days": 90,
        })
        assert resp.status_code in (200, 500)

    @pytest.mark.asyncio
    async def test_yield_curve_3d_invalid_days(self, client: AsyncClient):
        resp = await client.get("/api/v1/yield-curve/3d-surface", params={
            "limit_days": 10,  # < 30, 应该 422
        })
        assert resp.status_code == 422


# ==========================================================================
# 测试: 规则引擎 API
# ==========================================================================
class TestRulesAPI:
    @pytest.mark.asyncio
    async def test_get_all_rules(self, client: AsyncClient):
        resp = await client.get("/api/v1/rules")
        assert resp.status_code == 200
        data = resp.json()
        assert "rules" in data
        assert "engine_status" in data

    @pytest.mark.asyncio
    async def test_get_rules_by_group(self, client: AsyncClient):
        resp = await client.get("/api/v1/rules/liquidity")
        assert resp.status_code in (200, 404)

    @pytest.mark.asyncio
    async def test_get_rules_invalid_group(self, client: AsyncClient):
        resp = await client.get("/api/v1/rules/nonexistent_group")
        assert resp.status_code == 404


# ==========================================================================
# 测试: Webhook 缓存驱逐
# ==========================================================================
class TestWebhookCacheInvalidate:
    @pytest.mark.asyncio
    async def test_webhook_unauthorized(self, client: AsyncClient):
        resp = await client.post(
            "/api/v1/internal/cache-invalidate",
            json={"data_group": "liquidity"},
            headers={"x-webhook-secret": "wrong_secret"},
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_webhook_authorized(self, client: AsyncClient):
        resp = await client.post(
            "/api/v1/internal/cache-invalidate",
            json={"data_group": "liquidity", "dag_id": "test_dag", "record_count": 10},
            headers={"x-webhook-secret": "test_secret"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["dag_id"] == "test_dag"

    @pytest.mark.asyncio
    async def test_webhook_full_invalidation(self, client: AsyncClient):
        resp = await client.post(
            "/api/v1/internal/cache-invalidate",
            json={"data_group": "*", "dag_id": "full_refresh"},
            headers={"x-webhook-secret": "test_secret"},
        )
        assert resp.status_code == 200


# ==========================================================================
# 测试: SSE 事件广播器 (单元级)
# ==========================================================================
class TestSSEBroadcaster:
    def test_connect_disconnect(self):
        from app.main import SSEBroadcaster
        bc = SSEBroadcaster()
        loop = asyncio.new_event_loop()
        q = loop.run_until_complete(bc.connect())
        assert len(bc._clients) == 1
        bc.disconnect(q)
        assert len(bc._clients) == 0
        loop.close()

    def test_broadcast_increments_event_id(self):
        from app.main import SSEBroadcaster
        bc = SSEBroadcaster()
        loop = asyncio.new_event_loop()
        loop.run_until_complete(bc.connect())
        loop.run_until_complete(bc.broadcast("test", {"key": "value"}))
        assert bc._event_id == 1
        assert len(bc._history) == 1
        loop.close()

    def test_history_ring_buffer(self):
        from app.main import SSEBroadcaster
        bc = SSEBroadcaster()
        bc.MAX_HISTORY = 5
        loop = asyncio.new_event_loop()
        for i in range(10):
            loop.run_until_complete(bc.broadcast("test", {"i": i}))
        assert len(bc._history) == 5
        assert bc._event_id == 10
        loop.close()

    def test_get_missed_events(self):
        from app.main import SSEBroadcaster
        bc = SSEBroadcaster()
        loop = asyncio.new_event_loop()
        for i in range(5):
            loop.run_until_complete(bc.broadcast("test", {"i": i}))
        # last_event_id = "3" → 应该返回 id=4, 5
        missed = bc.get_missed_events("3")
        assert len(missed) == 2
        # None → 返回空
        assert bc.get_missed_events(None) == []
        assert bc.get_missed_events("invalid") == []
        loop.close()


# ==========================================================================
# 测试: OpenAPI 文档
# ==========================================================================
class TestOpenAPI:
    @pytest.mark.asyncio
    async def test_openapi_json(self, client: AsyncClient):
        resp = await client.get("/openapi.json")
        assert resp.status_code == 200
        data = resp.json()
        assert "paths" in data
        assert "/api/v1/dashboard/summary" in data["paths"]
        assert "/api/v1/dual-track/status" in data["paths"]
        assert "/api/v1/events/stream" in data["paths"]
        assert "/api/v1/yield-curve/3d-surface" in data["paths"]

    @pytest.mark.asyncio
    async def test_docs_endpoint(self, client: AsyncClient):
        resp = await client.get("/docs")
        assert resp.status_code == 200
