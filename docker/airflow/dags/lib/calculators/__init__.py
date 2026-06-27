"""
Python 重度计算分离模块
架构原则: 将复杂的数学逻辑 (3MMA, 二阶加速度, 滚动相关系数) 上浮至
         Pandas/NumPy 引擎计算, 算毕后将结果矩阵推回 TimescaleDB,
         避免用 SQL 强行表达复杂数学逻辑。
"""
from lib.calculators.inflation_acceleration import (
    compute_three_mma,
    compute_mom_growth,
    compute_acceleration,
    detect_firewood_rekindle,
    process_inflation_group,
)
from lib.calculators.rolling_correlation import (
    compute_rolling_correlation,
    compute_log_returns,
    process_contagion_group,
)

__all__ = [
    'compute_three_mma',
    'compute_mom_growth',
    'compute_acceleration',
    'detect_firewood_rekindle',
    'process_inflation_group',
    'compute_rolling_correlation',
    'compute_log_returns',
    'process_contagion_group',
]
