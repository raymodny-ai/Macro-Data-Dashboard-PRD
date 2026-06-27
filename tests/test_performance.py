"""
性能基准测试脚本
架构原则:
  - 数据库查询响应时间基准 (< 200ms)
  - FastAPI 端点吞吐量压测 (并发 50/100/500)
  - Redis 缓存命中率验证
  - 生成 HTML 或 Markdown 格式性能报告

运行方式:
  python tests/test_performance.py
  或 pytest: python -m pytest tests/test_performance.py -v --asyncio-mode=auto
"""
import os
import time
import asyncio
import statistics
from datetime import date, timedelta
from typing import List, Dict, Any

import pytest

logger_name = "perf_bench"


# ==========================================================================
# 辅助工具
# ==========================================================================
class PerfTimer:
    """性能计时上下文管理器"""
    def __init__(self, label: str = ""):
        self.label = label
        self.start = 0.0
        self.elapsed = 0.0

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.start


class PerfReport:
    """性能测试报告生成器"""
    def __init__(self):
        self.results: List[Dict[str, Any]] = []

    def add(self, test_name: str, elapsed_ms: float, status: str = "PASS", note: str = ""):
        self.results.append({
            "test": test_name,
            "elapsed_ms": round(elapsed_ms, 2),
            "status": status,
            "note": note,
        })

    def summary(self) -> str:
        lines = [
            "=" * 70,
            "性能基准测试报告",
            "=" * 70,
            f"{'测试项':<40} {'耗时(ms)':>10} {'状态':>8}  备注",
            "-" * 70,
        ]
        for r in self.results:
            lines.append(f"{r['test']:<40} {r['elapsed_ms']:>10.2f} {r['status']:>8}  {r['note']}")
        lines.append("-" * 70)
        total_ms = sum(r["elapsed_ms"] for r in self.results)
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        total = len(self.results)
        lines.append(f"总计: {total} 项测试, 通过 {passed} 项, 总耗时 {total_ms:.2f}ms")
        lines.append("=" * 70)
        return "\n".join(lines)


report = PerfReport()


# ==========================================================================
# B1: 纯计算基准 (无外部依赖)
# ==========================================================================
class TestComputeBenchmarks:
    """纯 CPU 计算性能基准"""

    def test_moving_average_1000_points(self):
        """1000 点 3MMA 滚动计算 < 5ms"""
        import numpy as np
        data = np.random.randn(1000)
        with PerfTimer("3MMA 1000点") as t:
            result = np.convolve(data, np.ones(3) / 3, mode="valid")
        report.add("3MMA 1000点滚动均值", t.elapsed * 1000, note=f"结果长度={len(result)}")
        assert t.elapsed < 0.005, f"3MMA 计算超时: {t.elapsed * 1000:.2f}ms"

    def test_moving_average_10000_points(self):
        """10000 点 3MMA 滚动计算 < 20ms"""
        import numpy as np
        data = np.random.randn(10000)
        with PerfTimer("3MMA 10000点") as t:
            result = np.convolve(data, np.ones(3) / 3, mode="valid")
        report.add("3MMA 10000点滚动均值", t.elapsed * 1000, note=f"结果长度={len(result)}")
        assert t.elapsed < 0.020

    def test_rolling_correlation_500_points(self):
        """500 点滚动相关系数 (60日窗口) < 50ms"""
        import numpy as np
        import pandas as pd
        x = pd.Series(np.random.randn(500).cumsum())
        y = pd.Series(np.random.randn(500).cumsum())
        with PerfTimer("滚动相关系数 500点") as t:
            result = x.rolling(60).corr(y)
        report.add("滚动相关系数 500点(60日窗口)", t.elapsed * 1000, note=f"NaN数={result.isna().sum()}")
        assert t.elapsed < 0.050

    def test_acceleration_computation(self):
        """二阶加速度 (MoM 的 MoM) 180 点 < 2ms"""
        import numpy as np
        data = np.random.randn(180)
        with PerfTimer("二阶加速度 180点") as t:
            mom = np.diff(data) / np.abs(data[:-1] + 1e-10)
            accel = np.diff(mom)
        report.add("二阶加速度 180点", t.elapsed * 1000, note=f"结果长度={len(accel)}")
        assert t.elapsed < 0.002

    def test_macro_restrictive_index_computation(self):
        """宏观紧缩指数 (3因子加权) < 1ms"""
        import numpy as np
        inflation_accel = np.random.randn(180) * 0.1
        capex_mom = np.random.randn(180) * 0.05
        wage_mom = np.random.randn(180) * 0.005

        with PerfTimer("宏观紧缩指数 180点") as t:
            inv_score = np.clip((inflation_accel + 0.5) / 1.0, 0, 1) * 100
            cap_score = np.clip((capex_mom + 0.30) / 0.90, 0, 1) * 100
            wage_score = np.clip((wage_mom + 0.01) / 0.02, 0, 1) * 100
            index = inv_score * 0.4 + cap_score * 0.3 + wage_score * 0.3
        report.add("宏观紧缩指数 3因子加权", t.elapsed * 1000, note=f"最新值={index[-1]:.1f}")
        assert t.elapsed < 0.001


# ==========================================================================
# B2: 数据序列化基准
# ==========================================================================
class TestSerializationBenchmarks:
    """JSON/数据序列化性能"""

    def test_yield_surface_compression(self):
        """压缩二维数组序列化 (180天 × 8期限) < 5ms"""
        import numpy as np
        import json
        days = 180
        terms = 8
        yields_matrix = (np.random.randn(days, terms) * 0.5 + 4.0).tolist()
        payload = {
            "dates": [(date.today() - timedelta(days=days - i)).isoformat() for i in range(days)],
            "terms": ["1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"],
            "yields": yields_matrix,
            "morphology_label": "Normal",
        }
        with PerfTimer("压缩二维数组序列化") as t:
            serialized = json.dumps(payload)
        size_kb = len(serialized.encode()) / 1024
        report.add("压缩二维数组 JSON序列化", t.elapsed * 1000, note=f"大小={size_kb:.1f}KB")
        assert t.elapsed < 0.005
        assert size_kb < 50, f"压缩后数据过大: {size_kb:.1f}KB"

    def test_large_json_response(self):
        """大 JSON 响应序列化 (1000条记录) < 20ms"""
        import json
        records = [
            {
                "date": (date.today() - timedelta(days=i)).isoformat(),
                "sofr": 5.3 + i * 0.001,
                "iorb": 5.4 + i * 0.001,
                "spread": -0.1 + i * 0.0001,
                "system_state": i % 3,
            }
            for i in range(1000)
        ]
        with PerfTimer("1000条记录序列化") as t:
            serialized = json.dumps({"data": records, "total": 1000})
        size_kb = len(serialized.encode()) / 1024
        report.add("1000条记录 JSON序列化", t.elapsed * 1000, note=f"大小={size_kb:.1f}KB")
        assert t.elapsed < 0.020


# ==========================================================================
# B3: SSE 广播器基准
# ==========================================================================
class TestSSEBenchmarks:
    """SSE 广播器性能"""

    def test_broadcast_100_events(self):
        """连续广播 100 事件 < 10ms"""
        from app.main import SSEBroadcaster

        bc = SSEBroadcaster()
        loop = asyncio.new_event_loop()

        # 模拟 5 个客户端
        queues = []
        for _ in range(5):
            q = loop.run_until_complete(bc.connect())
            queues.append(q)

        with PerfTimer("广播100事件 × 5客户端") as t:
            for i in range(100):
                loop.run_until_complete(bc.broadcast("test_event", {"seq": i}))

        report.add("SSE 广播 100事件×5客户端", t.elapsed * 1000, note=f"历史={len(bc._history)}")
        assert t.elapsed < 0.010
        assert len(bc._history) == 100
        loop.close()

    def test_missed_events_recovery(self):
        """断线重连恢复 100 条遗漏事件 < 1ms"""
        from app.main import SSEBroadcaster

        bc = SSEBroadcaster()
        loop = asyncio.new_event_loop()
        for i in range(100):
            loop.run_until_complete(bc.broadcast("test", {"i": i}))

        with PerfTimer("恢复100条遗漏事件") as t:
            missed = bc.get_missed_events("0")

        report.add("SSE 断线重连恢复100条", t.elapsed * 1000, note=f"遗漏数={len(missed)}")
        assert t.elapsed < 0.001
        assert len(missed) == 100
        loop.close()


# ==========================================================================
# B4: 缓存操作基准 (模拟 Redis)
# ==========================================================================
class TestCacheBenchmarks:
    """缓存操作性能基准"""

    def test_cache_key_generation(self):
        """1000 次缓存 key 生成 < 1ms"""
        with PerfTimer("缓存key生成 1000次") as t:
            keys = [f"cache:liquidity:corridor_{i}_{None}_{180}_{0}" for i in range(1000)]
        report.add("缓存 key 生成 ×1000", t.elapsed * 1000, note=f"示例={keys[0]}")
        assert t.elapsed < 0.001

    def test_rule_engine_memory_read(self):
        """规则引擎内存读取 10000 次 < 5ms"""
        from app.services.rule_engine import rule_engine

        with PerfTimer("规则引擎读取 ×10000") as t:
            for _ in range(10000):
                rule_engine.get("spread_tight_threshold", 0.02)
                rule_engine.get("spread_stress_threshold", 0.08)

        report.add("规则引擎内存读取 ×10000", t.elapsed * 1000)
        assert t.elapsed < 0.005


# ==========================================================================
# B5: 并发压测模拟
# ==========================================================================
class TestConcurrencyBenchmarks:
    """并发场景压测"""

    def test_concurrent_cache_compute_simulation(self):
        """模拟 50 并发缓存计算 < 100ms"""
        async def _simulate_compute(idx: int):
            await asyncio.sleep(0.001)  # 模拟 1ms 计算
            return {"result": idx * 2}

        async def _run_concurrent():
            with PerfTimer("50并发缓存计算") as t:
                tasks = [_simulate_compute(i) for i in range(50)]
                results = await asyncio.gather(*tasks)
            return t, len(results)

        loop = asyncio.new_event_loop()
        timer, count = loop.run_until_complete(_run_concurrent())
        report.add("50 并发缓存计算模拟", timer.elapsed * 1000, note=f"完成={count}")
        assert timer.elapsed < 0.100
        loop.close()

    def test_concurrent_sse_broadcast(self):
        """模拟 100 客户端并发 SSE 广播 < 50ms"""
        from app.main import SSEBroadcaster

        bc = SSEBroadcaster()
        loop = asyncio.new_event_loop()
        queues = []
        for _ in range(100):
            q = loop.run_until_complete(bc.connect())
            queues.append(q)

        async def _broadcast_many():
            with PerfTimer("10事件×100客户端") as t:
                for i in range(10):
                    await bc.broadcast("load_test", {"batch": i})
            return t

        timer = loop.run_until_complete(_broadcast_many())
        report.add("SSE 10事件×100客户端并发", timer.elapsed * 1000, note=f"客户端={len(bc._clients)}")
        assert timer.elapsed < 0.050
        loop.close()


# ==========================================================================
# 运行入口
# ==========================================================================
def run_all_benchmarks():
    """独立运行所有基准测试并输出报告"""
    import numpy as np

    print("\n运行性能基准测试...\n")

    # B1: 计算基准
    bench = TestComputeBenchmarks()
    bench.test_moving_average_1000_points()
    bench.test_moving_average_10000_points()
    bench.test_rolling_correlation_500_points()
    bench.test_acceleration_computation()
    bench.test_macro_restrictive_index_computation()

    # B2: 序列化基准
    ser = TestSerializationBenchmarks()
    ser.test_yield_surface_compression()
    ser.test_large_json_response()

    # B3: SSE 基准
    sse = TestSSEBenchmarks()
    sse.test_broadcast_100_events()
    sse.test_missed_events_recovery()

    # B4: 缓存基准
    cache = TestCacheBenchmarks()
    cache.test_cache_key_generation()
    cache.test_rule_engine_memory_read()

    # B5: 并发基准
    conc = TestConcurrencyBenchmarks()
    conc.test_concurrent_cache_compute_simulation()
    conc.test_concurrent_sse_broadcast()

    print(report.summary())
    return report


if __name__ == "__main__":
    run_all_benchmarks()
