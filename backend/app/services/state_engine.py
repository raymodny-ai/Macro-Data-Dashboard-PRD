"""
状态识别引擎
架构原则:
  - 独立双轨控制模型: 第一轨(宏观立场) + 第二轨(流动性修补)
  - "宏观紧缩指数" 聚合通胀加速度(40%) + AI CapEx增速(30%) + 工资动量(30%)
  - "功能瘫痪仪表盘" 聚合SOFR-IORB利差(50%) + MOVE指数(30%) + 认购倍数倒数(20%)
  - 规则引擎热更新阈值

核心区分:
  "健康疼痛区" vs "功能瘫痪区" → 决定操作策略
  美联储局部流动性修补 ≠ 降息信号 → 严禁盲目抄底
"""
import logging
from typing import Optional, Dict, Any
from datetime import date

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.rule_engine import rule_engine

logger = logging.getLogger(__name__)


# ==========================================================================
# 宏观紧缩指数 (Macro Restrictive Index, 0-100)
# ==========================================================================
async def compute_macro_restrictive_index(db: AsyncSession) -> Dict[str, Any]:
    """
    聚合三大因子计算宏观紧缩指数 (0-100)

    因子:
      1. 通胀加速度 (权重 40%): CPILFESL 的 acceleration 列, 标准化到 0-100
      2. AI CapEx 增速 (权重 30%): 七大科技巨头 capex_yoy 加权均值, 标准化到 0-100
      3. 工资动量扩散 (权重 30%): 4个行业 CES 的 mom_growth, 标准化到 0-100

    指数 > 75 → "高息压制环境"
    """
    weights = {
        "inflation": rule_engine.get("macro_weight_inflation", 0.40),
        "capex": rule_engine.get("macro_weight_capex", 0.30),
        "wage": rule_engine.get("macro_weight_wage", 0.30),
    }
    threshold_high = rule_engine.get("macro_restrictive_high", 75)

    # --- 因子1: 通胀加速度 ---
    inflation_score = await _compute_inflation_acceleration_score(db)

    # --- 因子2: AI CapEx 增速 ---
    capex_score = await _compute_capex_momentum_score(db)

    # --- 因子3: 工资动量扩散 ---
    wage_score = await _compute_wage_momentum_score(db)

    # 加权聚合
    index_value = round(
        inflation_score * weights["inflation"]
        + capex_score * weights["capex"]
        + wage_score * weights["wage"],
        2,
    )

    return {
        "index_value": index_value,
        "threshold_high": threshold_high,
        "is_high_restrictive": index_value > threshold_high,
        "factors": {
            "inflation_acceleration": {
                "score": inflation_score,
                "weight": weights["inflation"],
                "contribution": round(inflation_score * weights["inflation"], 2),
            },
            "ai_capex_momentum": {
                "score": capex_score,
                "weight": weights["capex"],
                "contribution": round(capex_score * weights["capex"], 2),
            },
            "wage_momentum": {
                "score": wage_score,
                "weight": weights["wage"],
                "contribution": round(wage_score * weights["wage"], 2),
            },
        },
        "interpretation": _interpret_macro_index(index_value, threshold_high),
    }


async def _compute_inflation_acceleration_score(db: AsyncSession) -> float:
    """
    通胀加速度标准化: 取最近 3 条 CPILFESL 的 acceleration 均值
    标准化: accel > 0 且 expanding → score 高; accel < 0 → score 低
    范围映射: [-0.5, +0.5] → [0, 100]
    """
    try:
        result = await db.execute(text("""
            SELECT acceleration FROM inflation_data
            WHERE symbol = 'CPILFESL' AND acceleration IS NOT NULL
            ORDER BY record_date DESC LIMIT 3
        """))
        rows = result.fetchall()
        if not rows:
            return 50.0  # 无数据 → 中性值
        avg_accel = sum(float(r[0]) for r in rows) / len(rows)
        # 线性映射: -0.5 → 0, 0 → 50, +0.5 → 100
        score = max(0, min(100, (avg_accel + 0.5) * 100))
        return round(score, 2)
    except Exception as e:
        logger.warning(f"Inflation score computation failed: {e}")
        return 50.0


async def _compute_capex_momentum_score(db: AsyncSession) -> float:
    """
    AI CapEx 增速标准化: 取最近季度七大科技巨头 capex_yoy 均值
    范围映射: [-0.30, +0.60] → [0, 100]
    """
    try:
        result = await db.execute(text("""
            SELECT capex_yoy FROM ai_capex_data
            WHERE capex_yoy IS NOT NULL
            ORDER BY report_date DESC LIMIT 7
        """))
        rows = result.fetchall()
        if not rows:
            return 50.0
        avg_yoy = sum(float(r[0]) for r in rows) / len(rows)
        # 线性映射: -0.30 → 0, 0.15 → 50, +0.60 → 100
        score = max(0, min(100, (avg_yoy + 0.30) / 0.90 * 100))
        return round(score, 2)
    except Exception as e:
        logger.warning(f"CapEx score computation failed: {e}")
        return 50.0


async def _compute_wage_momentum_score(db: AsyncSession) -> float:
    """
    工资动量扩散标准化: 取最近 4 个行业 CES 的 mom_growth 均值
    范围映射: [-0.01, +0.01] → [0, 100]
    """
    try:
        result = await db.execute(text("""
            SELECT symbol, mom_growth FROM inflation_data
            WHERE symbol LIKE 'CES%' AND mom_growth IS NOT NULL
            ORDER BY record_date DESC LIMIT 4
        """))
        rows = result.fetchall()
        if not rows:
            return 50.0
        avg_mom = sum(float(r[1]) for r in rows) / len(rows)
        # 线性映射: -0.01 → 0, 0 → 50, +0.01 → 100
        score = max(0, min(100, (avg_mom + 0.01) / 0.02 * 100))
        return round(score, 2)
    except Exception as e:
        logger.warning(f"Wage score computation failed: {e}")
        return 50.0


# ==========================================================================
# 功能瘫痪仪表盘 (Functional Paralysis Score, 0-100)
# ==========================================================================
async def compute_liquidity_risk_score(db: AsyncSession) -> Dict[str, Any]:
    """
    聚合三大因子计算流动性风险评分 (0-100)

    因子:
      1. SOFR-IORB 利差 (权重 50%): 最近 30 日利差均值标准化
      2. MOVE 指数标准化值 (权重 30%): 最近 MOVE 指数标准化
      3. 认购倍数倒数 (权重 20%): 最近拍卖 bid_to_cover 倒数标准化

    评分 > 80 → 红灯警告 (流动性功能瘫痪)
    """
    weights = {
        "spread": rule_engine.get("paralysis_weight_spread", 0.50),
        "move": rule_engine.get("paralysis_weight_move", 0.30),
        "btc": rule_engine.get("paralysis_weight_btc", 0.20),
    }
    threshold_red = rule_engine.get("liquidity_risk_red", 80)

    spread_score = await _compute_spread_risk_score(db)
    move_score = await _compute_move_risk_score(db)
    btc_score = await _compute_btc_risk_score(db)

    risk_score = round(
        spread_score * weights["spread"]
        + move_score * weights["move"]
        + btc_score * weights["btc"],
        2,
    )

    return {
        "risk_score": risk_score,
        "threshold_red": threshold_red,
        "is_red_alert": risk_score > threshold_red,
        "factors": {
            "sofr_iorb_spread": {
                "score": spread_score,
                "weight": weights["spread"],
                "contribution": round(spread_score * weights["spread"], 2),
            },
            "move_index": {
                "score": move_score,
                "weight": weights["move"],
                "contribution": round(move_score * weights["move"], 2),
            },
            "bid_to_cover": {
                "score": btc_score,
                "weight": weights["btc"],
                "contribution": round(btc_score * weights["btc"], 2),
            },
        },
        "interpretation": _interpret_risk_score(risk_score, threshold_red),
    }


async def _compute_spread_risk_score(db: AsyncSession) -> float:
    """利差风险评分: 最近 30 日 spread 均值, 映射 [-0.10, +0.10] → [0, 100]"""
    try:
        result = await db.execute(text("""
            SELECT AVG(spread) FROM liquidity_corridor
            WHERE symbol = 'SPREAD'
            AND record_date >= CURRENT_DATE - INTERVAL '30 days'
        """))
        row = result.fetchone()
        if not row or row[0] is None:
            return 50.0
        avg_spread = float(row[0])
        # 映射: -0.10 → 0 (充裕), 0 → 50 (紧张), +0.10 → 100 (瘫痪)
        score = max(0, min(100, (avg_spread + 0.10) / 0.20 * 100))
        return round(score, 2)
    except Exception as e:
        logger.warning(f"Spread risk score failed: {e}")
        return 50.0


async def _compute_move_risk_score(db: AsyncSession) -> float:
    """MOVE 指数风险评分: 最近 5 日 MOVE 均值, 映射 [60, 200] → [0, 100]"""
    try:
        result = await db.execute(text("""
            SELECT AVG(move_index) FROM market_contagion
            WHERE symbol = 'MOVE' AND move_index IS NOT NULL
            AND trade_date >= CURRENT_DATE - INTERVAL '5 days'
        """))
        row = result.fetchone()
        if not row or row[0] is None:
            return 50.0
        avg_move = float(row[0])
        # 映射: 60 → 0, 130 → 50, 200 → 100
        score = max(0, min(100, (avg_move - 60) / 140 * 100))
        return round(score, 2)
    except Exception as e:
        logger.warning(f"MOVE risk score failed: {e}")
        return 50.0


async def _compute_btc_risk_score(db: AsyncSession) -> float:
    """认购倍数倒数风险: bid_to_cover 越低 → 风险越高, 映射 [1.5, 3.5] → [100, 0]"""
    try:
        result = await db.execute(text("""
            SELECT AVG(bid_to_cover_ratio) FROM fiscal_auction_data
            WHERE bid_to_cover_ratio IS NOT NULL
            AND auction_date >= CURRENT_DATE - INTERVAL '60 days'
        """))
        row = result.fetchone()
        if not row or row[0] is None:
            return 50.0
        avg_btc = float(row[0])
        # 反向映射: 1.5 → 100 (高风险), 2.5 → 50, 3.5 → 0 (低风险)
        score = max(0, min(100, (3.5 - avg_btc) / 2.0 * 100))
        return round(score, 2)
    except Exception as e:
        logger.warning(f"BTC risk score failed: {e}")
        return 50.0


# ==========================================================================
# 双轨状态判定
# ==========================================================================
async def compute_dual_track_status(db: AsyncSession) -> Dict[str, Any]:
    """
    独立双轨控制模型

    第一轨: 宏观立场轨道 (Macro Stance Track)
      → 通胀加速度 + AI CapEx增速 + 工资动量 → 宏观紧缩指数
      → 状态: "宽松" (< 40) / "中性" (40-75) / "高息压制" (> 75)

    第二轨: 流动性修补轨道 (Liquidity Repair Track)
      → SOFR-IORB利差 + MOVE + 认购倍数 → 流动性风险评分
      → 状态: "充裕" (< 40) / "紧张" (40-80) / "功能瘫痪" (> 80)

    关键判定:
      当第一轨=高息压制 且 第二轨=功能瘫痪 → "系统性危机"
      当第一轨=高息压制 且 第二轨=紧张 → "局部修补, 非降息信号"
    """
    macro = await compute_macro_restrictive_index(db)
    liquidity = await compute_liquidity_risk_score(db)

    # 第一轨状态
    macro_idx = macro["index_value"]
    if macro_idx < 40:
        track1_state = "accommodative"
        track1_label = "宽松环境"
    elif macro_idx < 75:
        track1_state = "neutral"
        track1_label = "中性区间"
    else:
        track1_state = "restrictive"
        track1_label = "高息压制"

    # 第二轨状态
    risk_idx = liquidity["risk_score"]
    if risk_idx < 40:
        track2_state = "abundant"
        track2_label = "流动性充裕"
    elif risk_idx < 80:
        track2_state = "stressed"
        track2_label = "流动性紧张"
    else:
        track2_state = "paralyzed"
        track2_label = "功能瘫痪"

    # 交叉判定
    cross_verdict = _cross_track_verdict(track1_state, track2_state)

    return {
        "track_1_macro_stance": {
            "state": track1_state,
            "label": track1_label,
            "index": macro,
        },
        "track_2_liquidity_repair": {
            "state": track2_state,
            "label": track2_label,
            "score": liquidity,
        },
        "cross_verdict": cross_verdict,
        "rules_version": rule_engine.version,
    }


def _cross_track_verdict(track1: str, track2: str) -> Dict[str, Any]:
    """双轨交叉判定"""
    if track1 == "restrictive" and track2 == "paralyzed":
        return {
            "level": "critical",
            "message": "系统性危机: 宏观高息压制 + 流动性功能瘫痪",
            "action": "严格防守, 禁止加仓",
        }
    if track1 == "restrictive" and track2 == "stressed":
        return {
            "level": "warning",
            "message": "局部修补, 非降息信号: 美联储在修补水管, 宏观立场未变",
            "action": "严禁盲目抄底, 等待第一轨转向确认",
        }
    if track1 == "restrictive" and track2 == "abundant":
        return {
            "level": "info",
            "message": "高息压制但流动性充裕: 宏观紧缩效应尚未传导至流动性",
            "action": "关注利差变化, 等待流动性恶化信号",
        }
    if track1 == "neutral":
        return {
            "level": "info",
            "message": "中性区间: 宏观立场模糊, 等待方向确认",
            "action": "控制仓位, 等待明确信号",
        }
    return {
        "level": "safe",
        "message": "宽松环境: 宏观立场友好",
        "action": "正常操作",
    }


# ==========================================================================
# 解读文本
# ==========================================================================
def _interpret_macro_index(value: float, threshold: float) -> str:
    if value > threshold:
        return f"宏观紧缩指数 {value:.1f} > {threshold:.0f}, 处于高息压制环境, 权益资产承压"
    if value > 50:
        return f"宏观紧缩指数 {value:.1f}, 紧缩压力中等, 关注加速度方向"
    return f"宏观紧缩指数 {value:.1f}, 环境相对宽松"


def _interpret_risk_score(value: float, threshold: float) -> str:
    if value > threshold:
        return f"流动性风险评分 {value:.1f} > {threshold:.0f}, 触发红灯警告, 流动性功能接近瘫痪"
    if value > 50:
        return f"流动性风险评分 {value:.1f}, 流动性紧张, 关注 SOFR 利差和 MOVE 指数"
    return f"流动性风险评分 {value:.1f}, 流动性状态健康"
