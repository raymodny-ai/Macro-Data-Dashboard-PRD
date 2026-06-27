"""
滚动皮尔逊相关系数计算模块
架构原则: Python 重度计算分离 — 将 30d/60d 滚动相关系数上浮至 Pandas/NumPy

数学模型:
  1. 对数收益率: ln(P_t / P_{t-1})
  2. 滚动皮尔逊相关系数: series_a.rolling(window).corr(series_b)
  3. 市场传染组完整处理: SPY + TLT + MOVE → 合并输出

数据组:
  - SPY: S&P 500 ETF 复权收盘价
  - TLT: 20+ Year Treasury Bond ETF 复权收盘价
  - ^MOVE: ICE BofAML MOVE Index (债市波动率)

目标表: market_contagion (trade_date, symbol) UPSERT

市场传染警报判定 (B3 批次实现):
  同时满足:
    (1) SPY 单日跌幅 > 2%
    (2) 30日滚动相关系数由负转正且 > 0.5
    (3) MOVE 指数 > 120
  → contagion_alert = True
"""
import logging
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================================
# 核心数学函数
# ============================================================================

def compute_log_returns(price_series: pd.Series) -> pd.Series:
    """
    对数收益率 (Log Return)

    formula: ln(P_t / P_{t-1})

    优势:
      - 时间可加性: r_1 + r_2 = ln(P_2/P_0)
      - 对称性: 涨 10% 和跌 10% 的对数收益率绝对值相同
      - 适合计算滚动相关系数

    Args:
        price_series: 价格序列 (已按日期升序排列)

    Returns:
        对数收益率序列, 首行为 NaN
    """
    return np.log(price_series / price_series.shift(1))


def compute_rolling_correlation(
    series_a: pd.Series,
    series_b: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """
    滚动皮尔逊相关系数 (Rolling Pearson Correlation)

    formula: ρ(A, B, window) = Cov(A, B) / (σ_A × σ_B)
    实现: Pandas rolling().corr()

    解读:
      ρ > 0.7:  强正相关 (股债同涨, 流动性驱动)
      ρ ∈ [-0.3, 0.3]: 弱相关 (市场分化)
      ρ < -0.5: 强负相关 (股债跷跷板, 风险切换)
      ρ 由负转正: 市场结构转变信号 (传染警报条件之一)

    Args:
        series_a: 序列 A (如 SPY log returns)
        series_b: 序列 B (如 TLT log returns)
        window: 滚动窗口大小 (如 30, 60)
        min_periods: 最小有效观测数, 默认等于 window

    Returns:
        滚动相关系数序列
    """
    if min_periods is None:
        min_periods = window

    # 对齐两个序列的索引 (取交集日期)
    aligned_a, aligned_b = series_a.align(series_b, join='inner')

    return aligned_a.rolling(window=window, min_periods=min_periods).corr(aligned_b)


# ============================================================================
# 主入口: 处理市场传染组
# ============================================================================

def process_contagion_group(
    spy_prices: pd.Series,
    tlt_prices: pd.Series,
    move_index: Optional[pd.Series] = None,
    corr_30d_window: int = 30,
    corr_60d_window: int = 60,
) -> List[Dict[str, Any]]:
    """
    市场传染组完整计算流水线

    输入: SPY/TLT 复权收盘价 + MOVE 指数
    输出: 可直接 UPSERT 到 market_contagion 超表的记录列表

    流程:
      1. 计算 SPY/TLT 对数收益率
      2. 计算 30d/60d 滚动皮尔逊相关系数
      3. 合并 MOVE 指数
      4. 组装输出记录 (每个日期生成多条记录: SPY, TLT, CORR_30D, CORR_60D)

    Args:
        spy_prices: SPY 收盘价序列 (index=date, values=float)
        tlt_prices: TLT 收盘价序列
        move_index: MOVE 指数序列 (可选)
        corr_30d_window: 30日窗口, 默认 30
        corr_60d_window: 60日窗口, 默认 60

    Returns:
        List of dicts, 每条记录包含:
        {
            'trade_date': '2024-06-15',
            'symbol': 'SPY' | 'TLT' | 'MOVE' | 'CORR_30D' | 'CORR_60D',
            'close_price': ...,
            'log_return': ...,
            'move_index': ...,
            'rolling_corr_30d': ...,
            'rolling_corr_60d': ...,
            'contagion_alert': False,
        }
    """
    # 确保序列按日期升序
    spy_prices = spy_prices.sort_index()
    tlt_prices = tlt_prices.sort_index()
    if move_index is not None:
        move_index = move_index.sort_index()

    # 1) 对数收益率
    spy_log_ret = compute_log_returns(spy_prices)
    tlt_log_ret = compute_log_returns(tlt_prices)

    # 2) 滚动相关系数 (基于对数收益率)
    corr_30d = compute_rolling_correlation(spy_log_ret, tlt_log_ret, corr_30d_window)
    corr_60d = compute_rolling_correlation(spy_log_ret, tlt_log_ret, corr_60d_window)

    # 3) 确定公共日期范围 (以 SPY 为主键)
    all_dates = sorted(spy_prices.index)

    records = []
    for date in all_dates:
        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)

        # SPY 记录
        spy_corr_30 = _safe_float(corr_30d.get(date))
        spy_corr_60 = _safe_float(corr_60d.get(date))

        records.append({
            'trade_date': date_str,
            'symbol': 'SPY',
            'close_price': _safe_float(spy_prices.get(date)),
            'log_return': _safe_float(spy_log_ret.get(date)),
            'move_index': _safe_float(move_index.get(date)) if move_index is not None else None,
            'rolling_corr_30d': spy_corr_30,
            'rolling_corr_60d': spy_corr_60,
            'contagion_alert': False,
        })

        # TLT 记录 (仅当日有数据时)
        if date in tlt_prices.index:
            records.append({
                'trade_date': date_str,
                'symbol': 'TLT',
                'close_price': _safe_float(tlt_prices.get(date)),
                'log_return': _safe_float(tlt_log_ret.get(date)),
                'move_index': None,
                'rolling_corr_30d': spy_corr_30,
                'rolling_corr_60d': spy_corr_60,
                'contagion_alert': False,
            })

        # MOVE 记录 (仅当日有数据时)
        if move_index is not None and date in move_index.index:
            records.append({
                'trade_date': date_str,
                'symbol': 'MOVE',
                'close_price': None,
                'log_return': None,
                'move_index': _safe_float(move_index.get(date)),
                'rolling_corr_30d': spy_corr_30,
                'rolling_corr_60d': spy_corr_60,
                'contagion_alert': False,
            })

    logger.info(
        f"Contagion group: {len(records)} records "
        f"({len(all_dates)} trading days, "
        f"SPY={len(spy_prices)}, TLT={len(tlt_prices)}, "
        f"MOVE={len(move_index) if move_index is not None else 0})"
    )

    return records


def detect_contagion_alert(
    records: List[Dict[str, Any]],
    spy_drop_threshold: float = -0.02,
    corr_sign_flip_threshold: float = 0.5,
    move_threshold: float = 120.0,
) -> List[Dict[str, Any]]:
    """
    市场传染警报判定 (供 B3 批次 DAG 使用)

    同时满足三个条件时触发:
      (1) SPY 单日对数收益率 < spy_drop_threshold (默认 -2%)
      (2) 30日滚动相关系数 > corr_sign_flip_threshold (由负转正, > 0.5)
      (3) MOVE 指数 > move_threshold (默认 120)

    Args:
        records: process_contagion_group() 的输出
        spy_drop_threshold: SPY 跌幅阈值
        corr_sign_flip_threshold: 相关系数翻转阈值
        move_threshold: MOVE 指数阈值

    Returns:
        触发警报的记录列表 (contagion_alert=True 已标记)
    """
    # 按日期分组
    date_groups: Dict[str, Dict[str, Dict]] = {}
    for rec in records:
        td = rec['trade_date']
        if td not in date_groups:
            date_groups[td] = {}
        date_groups[td][rec['symbol']] = rec

    alerts = []
    for date, symbols in date_groups.items():
        spy_rec = symbols.get('SPY')
        move_rec = symbols.get('MOVE')

        if not spy_rec:
            continue

        # 条件1: SPY 大幅下跌
        spy_ret = spy_rec.get('log_return')
        if spy_ret is None or spy_ret >= spy_drop_threshold:
            continue

        # 条件2: 30d 相关系数 > 阈值
        corr_30 = spy_rec.get('rolling_corr_30d')
        if corr_30 is None or corr_30 < corr_sign_flip_threshold:
            continue

        # 条件3: MOVE > 阈值
        move_val = move_rec.get('move_index') if move_rec else None
        if move_val is None or move_val < move_threshold:
            continue

        # 三个条件同时满足 → 触发传染警报
        for sym_rec in symbols.values():
            sym_rec['contagion_alert'] = True
            alerts.append(sym_rec)

        logger.critical(
            f"CONTAGION ALERT: {date} | "
            f"SPY return={spy_ret:.4f}, "
            f"corr_30d={corr_30:.4f}, "
            f"MOVE={move_val:.1f}"
        )

    if alerts:
        logger.critical(f"CONTAGION: {len(alerts)} alerts across {len(set(a['trade_date'] for a in alerts))} days!")
    else:
        logger.info("No contagion alerts detected")

    return alerts


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
