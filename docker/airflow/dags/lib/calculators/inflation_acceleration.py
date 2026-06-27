"""
通胀二阶动量加速度计算模块
架构原则: Python 重度计算分离 — 将 3MMA/MoM/加速度上浮至 Pandas/NumPy 引擎

数学模型:
  1. 三月移动平均 (3MMA): (x_{t-2} + x_{t-1} + x_t) / 3
  2. 环比增速 (MoM Growth): (x_t - x_{t-1}) / x_{t-1}
  3. 二阶加速度: 当前窗口 3MMA(MoM) - 前置窗口 3MMA(MoM)
     即: mean(mom[t], mom[t-1], mom[t-2]) - mean(mom[t-3], mom[t-4], mom[t-5])
  4. "薪柴复燃" 预警: 连续两个月 acceleration > 0 且 slope 扩大
     即: accel[t] > 0 AND accel[t-1] > 0 AND accel[t] > accel[t-1]

数据组:
  - CPILFESL: 核心 CPI (全部消费者, 剔除食品和能源)
  - CES0500000003: 总私人部门平均时薪
  - CES3000000008: 制造业平均时薪
  - CES7000000003: 休闲酒店业平均时薪
  - CES5000000003: 信息产业平均时薪

目标表: inflation_data (record_date, symbol) UPSERT
"""
import logging
from typing import Dict, List, Optional, Any
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================================
# 核心数学函数
# ============================================================================

def compute_three_mma(series: pd.Series, window: int = 3) -> pd.Series:
    """
    三月移动平均

    formula: (x_{t-2} + x_{t-1} + x_t) / window

    Args:
        series: 输入时间序列 (已按日期升序排列)
        window: 移动窗口大小, 默认 3

    Returns:
        与输入同长度的 Series, 前 (window-1) 行为 NaN
    """
    return series.rolling(window=window, min_periods=window).mean()


def compute_mom_growth(series: pd.Series) -> pd.Series:
    """
    环比增速 (Month-over-Month Growth Rate)

    formula: (x_t - x_{t-1}) / x_{t-1}
    等效于: pct_change()

    Args:
        series: 输入时间序列 (绝对值, 如 CPI 指数)

    Returns:
        MoM 增速序列, 首行为 NaN
    """
    return series.pct_change()


def compute_acceleration(
    mom_series: pd.Series,
    window: int = 3,
) -> pd.Series:
    """
    二阶加速度 (Second Derivative of Momentum)

    formula:
        current_3mma = mean(mom[t], mom[t-1], mom[t-2])
        prior_3mma   = mean(mom[t-3], mom[t-4], mom[t-5])
        acceleration = current_3mma - prior_3mma

    解读:
        accel > 0: 通胀动量在加速 (价格压力上升)
        accel < 0: 通胀动量在减速 (价格压力缓解)
        accel = 0: 动量稳定

    Args:
        mom_series: MoM 增速序列
        window: 滑动窗口大小, 默认 3

    Returns:
        二阶加速度序列
    """
    current_3mma = mom_series.rolling(window=window, min_periods=window).mean()
    # 前置窗口: shift(window) 将当前窗口整体前移
    prior_3mma = mom_series.shift(window).rolling(window=window, min_periods=window).mean()
    return current_3mma - prior_3mma


def detect_firewood_rekindle(acceleration: pd.Series) -> pd.Series:
    """
    "薪柴复燃" 预警检测

    判定逻辑 (连续两个月满足):
      1. acceleration[t] > 0  (当前月加速度为正)
      2. acceleration[t-1] > 0  (上月加速度为正)
      3. acceleration[t] > acceleration[t-1]  (斜率扩大: 加速在加剧)

    含义:
        通胀的二阶导数连续为正且扩大, 意味着通胀不仅是 "粘性",
        而是在 "复燃" — 类似添柴加火, 联储的紧缩远未见效。

    Args:
        acceleration: 二阶加速度序列

    Returns:
        Boolean Series: True = 触发 "薪柴复燃" 预警
    """
    positive_now = acceleration > 0
    positive_prev = acceleration.shift(1) > 0
    expanding = acceleration > acceleration.shift(1)

    return positive_now & positive_prev & expanding


# ============================================================================
# 主入口: 处理通胀组全部指标
# ============================================================================

# FRED Series ID → 内部 symbol 映射
INFLATION_SYMBOLS = {
    'CPILFESL': 'CPILFESL',          # 核心 CPI
    'CES0500000003': 'CES_TOTAL',     # 总私人部门时薪
    'CES3000000008': 'CES_MFG',       # 制造业时薪
    'CES7000000003': 'CES_LEISURE',   # 休闲酒店业时薪
    'CES5000000003': 'CES_INFO',      # 信息产业时薪
}


def process_inflation_group(
    raw_data: Dict[str, pd.Series],
) -> List[Dict[str, Any]]:
    """
    通胀组完整计算流水线

    输入: FRED API 原始数据字典 {series_id: pd.Series}
    输出: 可直接 UPSERT 到 inflation_data 超表的记录列表

    流程:
      1. 对每个 series 计算 3MMA → MoM → 二阶加速度
      2. 检测 "薪柴复燃" 预警
      3. 组装输出记录

    Args:
        raw_data: {
            'CPILFESL': pd.Series(index=date, values=float),
            'CES05000000003': pd.Series(...),
            ...
        }

    Returns:
        List of dicts, 每条记录包含:
        {
            'record_date': '2024-06-01',
            'symbol': 'CPILFESL',
            'value': 312.456,
            'mom_growth': 0.0023,
            'three_mma': 0.0021,
            'acceleration': 0.0005,
            'warning_flag': False,
        }
    """
    all_records = []

    for fred_id, symbol_name in INFLATION_SYMBOLS.items():
        series = raw_data.get(fred_id)
        if series is None:
            logger.warning(f"Missing series: {fred_id} ({symbol_name}), skipping")
            continue

        # 确保序列按日期升序
        series = series.sort_index()

        # 1) 三月移动平均
        three_mma = compute_three_mma(series)

        # 2) 环比增速 (MoM)
        mom_growth = compute_mom_growth(series)

        # 3) MoM 的 3MMA 平滑 (降噪)
        mom_3mma = compute_three_mma(mom_growth)

        # 4) 二阶加速度
        acceleration = compute_acceleration(mom_growth)

        # 5) "薪柴复燃" 预警
        firewood_alert = detect_firewood_rekindle(acceleration)

        # 6) 组装记录
        for date in series.index:
            record = {
                'record_date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                'symbol': symbol_name,
                'value': _safe_float(series.get(date)),
                'mom_growth': _safe_float(mom_growth.get(date)),
                'three_mma': _safe_float(three_mma.get(date)),
                'acceleration': _safe_float(acceleration.get(date)),
                'warning_flag': bool(firewood_alert.get(date, False)),
            }
            all_records.append(record)

        logger.info(
            f"[{symbol_name}] Processed {len(series)} observations, "
            f"{firewood_alert.sum()} firewood alerts"
        )

    logger.info(
        f"Inflation group: {len(all_records)} total records "
        f"({len(raw_data)} series processed)"
    )
    return all_records


# ============================================================================
# 内部工具函数
# ============================================================================

def _safe_float(val) -> Optional[float]:
    """安全转换为 float, NaN/None 返回 None"""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (ValueError, TypeError):
        return None
