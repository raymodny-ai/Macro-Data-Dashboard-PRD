"""
数据查询服务层
架构原则: 异步 SQL 查询 + Redis 缓存穿透保护 + 分页支持
"""
import logging
from typing import Optional, Dict, Any, List
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ==========================================================================
# 流动性走廊查询
# ==========================================================================
async def query_liquidity_corridor(
    db: AsyncSession,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 180,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    查询流动性走廊时序数据 (SOFR/IORB/利差/系统状态)
    支持分页和时间范围过滤
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=180)

    # 查询主数据 (SPREAD 行)
    data_sql = text("""
        SELECT record_date, sofr_rate, iorb_rate, spread, system_state, crisis_alert
        FROM liquidity_corridor
        WHERE symbol = 'SPREAD'
          AND record_date BETWEEN :start_date AND :end_date
        ORDER BY record_date DESC
        LIMIT :limit OFFSET :offset
    """)
    result = await db.execute(data_sql, {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
        "offset": offset,
    })
    rows = result.fetchall()

    # 统计信息
    stats_sql = text("""
        SELECT
            COUNT(*) AS total_count,
            AVG(spread) AS avg_spread,
            MIN(spread) AS min_spread,
            MAX(spread) AS max_spread,
            SUM(CASE WHEN crisis_alert = TRUE THEN 1 ELSE 0 END) AS crisis_count
        FROM liquidity_corridor
        WHERE symbol = 'SPREAD'
          AND record_date BETWEEN :start_date AND :end_date
    """)
    stats_result = await db.execute(stats_sql, {
        "start_date": start_date,
        "end_date": end_date,
    })
    stats_row = stats_result.fetchone()

    records = []
    for row in rows:
        records.append({
            "date": row[0].isoformat(),
            "sofr": float(row[1]) if row[1] is not None else None,
            "iorb": float(row[2]) if row[2] is not None else None,
            "spread": float(row[3]) if row[3] is not None else None,
            "system_state": int(row[4]) if row[4] is not None else None,
            "system_state_label": {0: "充裕", 1: "紧张", 2: "瘫痪"}.get(
                int(row[4]) if row[4] is not None else -1, "未知"
            ),
            "crisis_alert": bool(row[5]),
        })

    # 反转使时间序列正序 (旧→新)
    records.reverse()

    return {
        "records": records,
        "total_count": int(stats_row[0]) if stats_row else 0,
        "statistics": {
            "avg_spread": round(float(stats_row[1]), 6) if stats_row and stats_row[1] else None,
            "min_spread": round(float(stats_row[2]), 6) if stats_row and stats_row[2] else None,
            "max_spread": round(float(stats_row[3]), 6) if stats_row and stats_row[3] else None,
            "crisis_alert_count": int(stats_row[4]) if stats_row and stats_row[4] else 0,
        },
        "pagination": {
            "limit": limit,
            "offset": offset,
            "returned": len(records),
        },
    }


# ==========================================================================
# 通胀趋势查询
# ==========================================================================
async def query_inflation_trend(
    db: AsyncSession,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 180,
) -> Dict[str, Any]:
    """
    查询通胀二阶导趋势数据 (CPI + 时薪 MoM/加速度)
    返回各 symbol 的时序 + "薪柴复燃" 预警时间点
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=180)

    sql = text("""
        SELECT record_date, symbol, value, mom_growth, acceleration, three_mma, warning_flag
        FROM inflation_data
        WHERE record_date BETWEEN :start_date AND :end_date
        ORDER BY symbol, record_date ASC
    """)
    result = await db.execute(sql, {
        "start_date": start_date,
        "end_date": end_date,
    })
    rows = result.fetchall()

    # 按 symbol 分组
    by_symbol: Dict[str, List[Dict]] = {}
    firewood_dates = []  # "薪柴复燃" 预警时间点

    for row in rows:
        symbol = row[1]
        record = {
            "date": row[0].isoformat(),
            "value": float(row[2]) if row[2] is not None else None,
            "mom_growth": float(row[3]) if row[3] is not None else None,
            "acceleration": float(row[4]) if row[4] is not None else None,
            "three_mma": float(row[5]) if row[5] is not None else None,
            "warning_flag": bool(row[6]),
        }
        if symbol not in by_symbol:
            by_symbol[symbol] = []
        by_symbol[symbol].append(record)

        if row[6] and symbol == "CPILFESL":
            firewood_dates.append(row[0].isoformat())

    # 取最近值
    latest = {}
    for symbol, records in by_symbol.items():
        if records:
            latest[symbol] = records[-1]

    return {
        "by_symbol": by_symbol,
        "latest": latest,
        "firewood_rekindle_alerts": firewood_dates,
        "symbol_labels": {
            "CPILFESL": "核心 CPI (剔除食品和能源)",
            "CES0500000003": "总私人部门平均时薪",
            "CES3000000008": "制造业时薪",
            "CES7000000003": "休闲酒店业时薪",
            "CES5000000003": "信息产业时薪",
        },
        "date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        },
    }


# ==========================================================================
# 仪表板摘要 (五组合并)
# ==========================================================================
async def query_dashboard_summary(db: AsyncSession) -> Dict[str, Any]:
    """五组数据聚合摘要, 返回各组最新状态"""

    inflation = await _summarize_inflation(db)
    fiscal = await _summarize_fiscal(db)
    liquidity = await _summarize_liquidity(db)
    ai_capex = await _summarize_ai_capex(db)
    contagion = await _summarize_contagion(db)

    return {
        "generated_at": date.today().isoformat(),
        "groups": {
            "inflation": inflation,
            "fiscal": fiscal,
            "liquidity": liquidity,
            "ai_capex": ai_capex,
            "contagion": contagion,
        },
    }


async def _summarize_inflation(db: AsyncSession) -> Dict[str, Any]:
    """通胀组摘要: 最新 CPI acceleration + 预警状态"""
    try:
        result = await db.execute(text("""
            SELECT record_date, acceleration, warning_flag
            FROM inflation_data
            WHERE symbol = 'CPILFESL' AND acceleration IS NOT NULL
            ORDER BY record_date DESC LIMIT 1
        """))
        row = result.fetchone()
        if not row:
            return {"status": "no_data"}
        return {
            "latest_date": row[0].isoformat(),
            "cpi_acceleration": float(row[1]),
            "warning_active": bool(row[2]),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _summarize_fiscal(db: AsyncSession) -> Dict[str, Any]:
    """财政组摘要: 最近拍卖 + 熊陡预警"""
    try:
        result = await db.execute(text("""
            SELECT auction_date, security_type, bid_to_cover_ratio, fiscal_warning_flag
            FROM fiscal_auction_data
            ORDER BY auction_date DESC LIMIT 2
        """))
        rows = result.fetchall()
        if not rows:
            return {"status": "no_data"}

        auctions = []
        warning = False
        for r in rows:
            auctions.append({
                "date": r[0].isoformat(),
                "type": r[1],
                "bid_to_cover": float(r[2]) if r[2] else None,
            })
            if r[3]:
                warning = True
        return {"latest_auctions": auctions, "bear_steepener_warning": warning}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _summarize_liquidity(db: AsyncSession) -> Dict[str, Any]:
    """流动性组摘要: 最新利差 + 系统状态 + 危机计数"""
    try:
        result = await db.execute(text("""
            SELECT record_date, spread, system_state, crisis_alert
            FROM liquidity_corridor
            WHERE symbol = 'SPREAD'
            ORDER BY record_date DESC LIMIT 1
        """))
        row = result.fetchone()
        if not row:
            return {"status": "no_data"}

        # 最近 30 日危机计数
        crisis_result = await db.execute(text("""
            SELECT COUNT(*) FROM liquidity_corridor
            WHERE symbol = 'SPREAD' AND crisis_alert = TRUE
            AND record_date >= CURRENT_DATE - INTERVAL '30 days'
        """))
        crisis_count = crisis_result.fetchone()[0]

        return {
            "latest_date": row[0].isoformat(),
            "spread": float(row[1]) if row[1] is not None else None,
            "system_state": int(row[2]) if row[2] is not None else None,
            "system_state_label": {0: "充裕", 1: "紧张", 2: "瘫痪"}.get(
                int(row[2]) if row[2] is not None else -1, "未知"
            ),
            "crisis_alert": bool(row[3]),
            "crisis_count_30d": int(crisis_count),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _summarize_ai_capex(db: AsyncSession) -> Dict[str, Any]:
    """AI CapEx 摘要: 最新季度七大巨头 CapEx 合计 + 增速"""
    try:
        result = await db.execute(text("""
            SELECT report_date, SUM(capex) AS total_capex, AVG(capex_yoy) AS avg_yoy
            FROM ai_capex_data
            WHERE capex IS NOT NULL
            GROUP BY report_date
            ORDER BY report_date DESC LIMIT 1
        """))
        row = result.fetchone()
        if not row:
            return {"status": "no_data"}
        return {
            "latest_quarter": row[0].isoformat(),
            "total_capex_billions": round(float(row[1]) / 1e9, 2) if row[1] else None,
            "avg_yoy_growth": round(float(row[2]), 4) if row[2] else None,
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


async def _summarize_contagion(db: AsyncSession) -> Dict[str, Any]:
    """市场传染摘要: 最近传染警报 + 30d相关系数"""
    try:
        # 最近 SPY-TLT 30d 相关系数
        corr_result = await db.execute(text("""
            SELECT trade_date, rolling_corr_30d, move_index, contagion_alert
            FROM market_contagion
            WHERE symbol = 'SPY' AND rolling_corr_30d IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        """))
        corr_row = corr_result.fetchone()

        # 最近 30 日传染警报计数
        alert_result = await db.execute(text("""
            SELECT COUNT(DISTINCT trade_date) FROM market_contagion
            WHERE contagion_alert = TRUE
            AND trade_date >= CURRENT_DATE - INTERVAL '30 days'
        """))
        alert_count = alert_result.fetchone()[0]

        if not corr_row:
            return {"status": "no_data"}

        return {
            "latest_date": corr_row[0].isoformat(),
            "rolling_corr_30d": round(float(corr_row[1]), 4),
            "move_index": float(corr_row[2]) if corr_row[2] is not None else None,
            "contagion_alert_active": bool(corr_row[3]),
            "contagion_alerts_30d": int(alert_count),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}
