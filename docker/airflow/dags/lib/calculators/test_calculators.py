"""
计算模块单元测试
架构原则: 纯 NumPy/Pandas 验证, 不依赖数据库连接
验证: 3MMA, MoM 增速, 二阶加速度, 薪柴复燃预警, 对数收益率, 滚动相关系数

运行方式 (在 Airflow 容器内):
  python -m pytest /opt/airflow/dags/lib/calculators/test_calculators.py -v
"""
import sys
import os
import numpy as np
import pandas as pd
import pytest

# 将 dags 目录加入 Python 路径以便导入 lib 模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.calculators.inflation_acceleration import (
    compute_three_mma,
    compute_mom_growth,
    compute_acceleration,
    detect_firewood_rekindle,
    process_inflation_group,
    _safe_float,
)
from lib.calculators.rolling_correlation import (
    compute_log_returns,
    compute_rolling_correlation,
    process_contagion_group,
    detect_contagion_alert,
)


# ============================================================================
# 通胀计算模块测试
# ============================================================================

class TestThreeMMA:
    """三月移动平均测试"""

    def test_basic(self):
        """标准输入: [1, 2, 3, 4, 5] → 3MMA = [NaN, NaN, 2.0, 3.0, 4.0]"""
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = compute_three_mma(series)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == pytest.approx(2.0)
        assert result.iloc[3] == pytest.approx(3.0)
        assert result.iloc[4] == pytest.approx(4.0)

    def test_constant_series(self):
        """常数序列: 3MMA 应等于常数"""
        series = pd.Series([5.0] * 10)
        result = compute_three_mma(series)
        for val in result.dropna():
            assert val == pytest.approx(5.0)

    def test_custom_window(self):
        """自定义窗口大小"""
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = compute_three_mma(series, window=2)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(1.5)
        assert result.iloc[2] == pytest.approx(2.5)


class TestMomGrowth:
    """环比增速测试"""

    def test_positive_growth(self):
        """正增速: [100, 110, 121] → [NaN, 0.10, 0.10]"""
        series = pd.Series([100.0, 110.0, 121.0])
        result = compute_mom_growth(series)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(0.10, abs=1e-6)
        assert result.iloc[2] == pytest.approx(0.10, abs=1e-6)

    def test_negative_growth(self):
        """负增速 (通缩): [100, 90] → [NaN, -0.10]"""
        series = pd.Series([100.0, 90.0])
        result = compute_mom_growth(series)
        assert result.iloc[1] == pytest.approx(-0.10, abs=1e-6)

    def test_zero_growth(self):
        """零增速: [100, 100] → [NaN, 0.0]"""
        series = pd.Series([100.0, 100.0])
        result = compute_mom_growth(series)
        assert result.iloc[1] == pytest.approx(0.0)


class TestAcceleration:
    """二阶加速度测试"""

    def test_constant_momentum(self):
        """匀速增速: 加速度应为零"""
        # MoM 全部相同 → 3MMA 差值 = 0
        mom = pd.Series([0.02] * 12)
        accel = compute_acceleration(mom, window=3)
        for val in accel.dropna():
            assert val == pytest.approx(0.0, abs=1e-10)

    def test_accelerating_momentum(self):
        """加速增长: 加速度 > 0"""
        # MoM 逐步增大
        mom = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09])
        accel = compute_acceleration(mom, window=3)
        valid = accel.dropna()
        # 加速度应为正
        assert all(v > 0 for v in valid), f"Expected positive acceleration, got {valid.values}"

    def test_decelerating_momentum(self):
        """减速增长: 加速度 < 0"""
        mom = pd.Series([0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01])
        accel = compute_acceleration(mom, window=3)
        valid = accel.dropna()
        assert all(v < 0 for v in valid), f"Expected negative acceleration, got {valid.values}"


class TestFirewoodRekindle:
    """薪柴复燃预警测试"""

    def test_trigger_condition(self):
        """触发条件: 连续两月 accel > 0 且扩大"""
        accel = pd.Series([0.0, 0.01, 0.015, 0.02, 0.01, -0.01])
        result = detect_firewood_rekindle(accel)
        # index 3: accel=0.02 > 0, prev=0.015 > 0, 0.02 > 0.015 → True
        assert result.iloc[3] == True
        # index 4: accel=0.01 > 0, prev=0.02 > 0, but 0.01 < 0.02 → False
        assert result.iloc[4] == False

    def test_no_trigger_negative(self):
        """不触发: 加速度为负"""
        accel = pd.Series([-0.01, -0.02, -0.03])
        result = detect_firewood_rekindle(accel)
        assert all(~result)

    def test_no_trigger_shrinking(self):
        """不触发: 正值但在缩小"""
        accel = pd.Series([0.05, 0.03, 0.01])
        result = detect_firewood_rekindle(accel)
        assert all(~result)


class TestProcessInflationGroup:
    """通胀组完整流水线测试"""

    def test_basic_processing(self):
        """基本处理: 输入两个 series, 输出应有对应记录"""
        dates = pd.date_range('2024-01-01', periods=12, freq='MS')
        raw_data = {
            'CPILFESL': pd.Series(
                np.linspace(310, 315, 12),
                index=dates,
            ),
            'CES0500000003': pd.Series(
                np.linspace(34.0, 35.5, 12),
                index=dates,
            ),
        }
        records = process_inflation_group(raw_data)
        # 2 series × 12 dates = 24 records
        assert len(records) == 24

        # 检查记录结构
        rec = records[0]
        assert 'record_date' in rec
        assert 'symbol' in rec
        assert 'value' in rec
        assert 'mom_growth' in rec
        assert 'three_mma' in rec
        assert 'acceleration' in rec
        assert 'warning_flag' in rec

    def test_missing_series_handled(self):
        """缺失序列应跳过而非报错"""
        dates = pd.date_range('2024-01-01', periods=6, freq='MS')
        raw_data = {
            'CPILFESL': pd.Series([310, 311, 312, 313, 314, 315], index=dates),
            # 故意不提供其他 series
        }
        records = process_inflation_group(raw_data)
        assert len(records) == 6
        assert all(r['symbol'] == 'CPILFESL' for r in records)


# ============================================================================
# 市场传染计算模块测试
# ============================================================================

class TestLogReturns:
    """对数收益率测试"""

    def test_basic(self):
        """基本对数收益率: ln(110/100) ≈ 0.0953"""
        prices = pd.Series([100.0, 110.0])
        result = compute_log_returns(prices)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == pytest.approx(np.log(110 / 100), abs=1e-6)

    def test_no_change(self):
        """价格不变: 对数收益率 = 0"""
        prices = pd.Series([100.0, 100.0])
        result = compute_log_returns(prices)
        assert result.iloc[1] == pytest.approx(0.0)

    def test_decline(self):
        """价格下跌: 对数收益率为负"""
        prices = pd.Series([100.0, 90.0])
        result = compute_log_returns(prices)
        assert result.iloc[1] < 0
        assert result.iloc[1] == pytest.approx(np.log(90 / 100), abs=1e-6)


class TestRollingCorrelation:
    """滚动皮尔逊相关系数测试"""

    def test_perfect_positive(self):
        """完美正相关: ρ = 1.0"""
        np.random.seed(42)
        a = pd.Series(np.cumsum(np.random.randn(100)) + 100)
        # b 与 a 完全线性相关
        b = a * 2 + 5
        result = compute_rolling_correlation(a, b, window=30)
        valid = result.dropna()
        for val in valid:
            assert val == pytest.approx(1.0, abs=1e-10)

    def test_perfect_negative(self):
        """完美负相关: ρ = -1.0"""
        np.random.seed(42)
        a = pd.Series(np.cumsum(np.random.randn(100)) + 100)
        b = -a * 3 + 200
        result = compute_rolling_correlation(a, b, window=30)
        valid = result.dropna()
        for val in valid:
            assert val == pytest.approx(-1.0, abs=1e-10)

    def test_uncorrelated(self):
        """不相关序列: ρ ≈ 0"""
        np.random.seed(42)
        a = pd.Series(np.random.randn(1000))
        b = pd.Series(np.random.randn(1000))
        result = compute_rolling_correlation(a, b, window=100)
        valid = result.dropna()
        # 大样本下不相关序列的相关系数应接近 0
        assert abs(valid.iloc[-1]) < 0.2

    def test_min_periods(self):
        """最小有效观测数: 少于 min_periods 时返回 NaN"""
        a = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        b = pd.Series([2.0, 4.0, 6.0, 8.0, 10.0])
        result = compute_rolling_correlation(a, b, window=3, min_periods=3)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        # 第 3 个值 (index 2) 起应有数据
        assert not pd.isna(result.iloc[2])


class TestProcessContagionGroup:
    """市场传染组完整流水线测试"""

    def test_basic_processing(self):
        """基本处理: SPY+TLT+MOVE 输入, 输出多 symbol 记录"""
        dates = pd.bdate_range('2024-01-02', periods=60)  # 60 个交易日
        np.random.seed(42)

        spy = pd.Series(
            np.cumsum(np.random.randn(60)) + 450,
            index=dates,
        )
        tlt = pd.Series(
            np.cumsum(np.random.randn(60)) + 95,
            index=dates,
        )
        move = pd.Series(
            np.random.uniform(90, 130, 60),
            index=dates,
        )

        records = process_contagion_group(spy, tlt, move)

        # 每个交易日应有 SPY + TLT + MOVE = 3 条记录
        assert len(records) == 180

        # 检查 SPY 记录
        spy_records = [r for r in records if r['symbol'] == 'SPY']
        assert len(spy_records) == 60
        assert all(r['close_price'] is not None for r in spy_records)

        # 检查 30d 相关系数在前 30 天为 None
        early_corr = [r['rolling_corr_30d'] for r in spy_records[:29]]
        assert all(c is None for c in early_corr)


class TestDetectContagionAlert:
    """市场传染警报测试"""

    def test_alert_trigger(self):
        """三条件同时满足时触发警报"""
        records = [
            {
                'trade_date': '2024-06-15',
                'symbol': 'SPY',
                'close_price': 440.0,
                'log_return': -0.03,   # -3% (满足条件1: < -2%)
                'move_index': None,
                'rolling_corr_30d': 0.7,  # 0.7 (满足条件2: > 0.5)
                'rolling_corr_60d': 0.5,
                'contagion_alert': False,
            },
            {
                'trade_date': '2024-06-15',
                'symbol': 'MOVE',
                'close_price': None,
                'log_return': None,
                'move_index': 130.0,   # 130 (满足条件3: > 120)
                'rolling_corr_30d': 0.7,
                'rolling_corr_60d': 0.5,
                'contagion_alert': False,
            },
        ]
        alerts = detect_contagion_alert(records)
        assert len(alerts) == 2  # SPY + MOVE 都被标记
        assert all(a['contagion_alert'] == True for a in alerts)

    def test_no_alert_partial_conditions(self):
        """仅满足部分条件时不触发"""
        records = [
            {
                'trade_date': '2024-06-15',
                'symbol': 'SPY',
                'close_price': 450.0,
                'log_return': -0.03,   # 满足条件1
                'move_index': None,
                'rolling_corr_30d': 0.2,  # 不满足条件2 (< 0.5)
                'rolling_corr_60d': 0.1,
                'contagion_alert': False,
            },
            {
                'trade_date': '2024-06-15',
                'symbol': 'MOVE',
                'close_price': None,
                'log_return': None,
                'move_index': 130.0,   # 满足条件3
                'rolling_corr_30d': 0.2,
                'rolling_corr_60d': 0.1,
                'contagion_alert': False,
            },
        ]
        alerts = detect_contagion_alert(records)
        assert len(alerts) == 0


# ============================================================================
# 工具函数测试
# ============================================================================

class TestSafeFloat:
    """_safe_float 工具函数测试"""

    def test_none(self):
        assert _safe_float(None) is None

    def test_nan(self):
        assert _safe_float(float('nan')) is None

    def test_valid_float(self):
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_string_number(self):
        assert _safe_float('3.14') == pytest.approx(3.14)

    def test_invalid_string(self):
        assert _safe_float('abc') is None


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
