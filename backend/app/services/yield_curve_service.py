"""
3D 收益率曲面数据服务
架构原则:
  - 压缩二维数组传输: [time_index][term_index] = yield_value
  - 禁止传输庞大 JSON 网格 (vertices/faces/colors)
  - 浏览器本地生成 BufferGeometry
"""
import logging
from typing import Optional, Dict, Any, List
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# FRED 国债收益率序列 ID 映射
TREASURY_YIELDS = {
    '1M': 'DGS1MO',
    '3M': 'DGS3MO',
    '6M': 'DGS6MO',
    '1Y': 'DGS1',
    '2Y': 'DGS2',
    '5Y': 'DGS5',
    '10Y': 'DGS10',
    '30Y': 'DGS30',
}

TERM_ORDER = ['1M', '3M', '6M', '1Y', '2Y', '5Y', '10Y', '30Y']


async def query_yield_curve_3d(
    db: AsyncSession,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit_days: int = 180,
) -> Dict[str, Any]:
    """
    查询 3D 收益率曲面数据 (压缩二维数组格式)

    Returns:
        {
            "dates": ["2024-01-01", ...],
            "terms": ["1M", "3M", ..., "30Y"],
            "yields": [[y1_t1, y1_t2, ...], [y2_t1, y2_t2, ...], ...],
            "morphology_label": "Bear Steepener" | "Bear Flattener" | "Twist Flattener" | "Normal",
        }
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=limit_days)

    # 查询所有期限的收益率时序数据
    yields_by_term: Dict[str, List[tuple]] = {}

    for term, series_id in TREASURY_YIELDS.items():
        sql = text("""
            SELECT record_date, value
            FROM treasury_yields
            WHERE symbol = :symbol
              AND record_date BETWEEN :start_date AND :end_date
            ORDER BY record_date ASC
        """)
        result = await db.execute(sql, {
            "symbol": series_id,
            "start_date": start_date,
            "end_date": end_date,
        })
        rows = result.fetchall()
        yields_by_term[term] = [(r[0], float(r[1])) for r in rows if r[1] is not None]

    if not yields_by_term:
        return {
            "dates": [],
            "terms": TERM_ORDER,
            "yields": [],
            "morphology_label": "No Data",
        }

    # 对齐日期 (取所有期限的交集)
    all_dates = set()
    for term_data in yields_by_term.values():
        all_dates.update(d for d, _ in term_data)

    sorted_dates = sorted(all_dates)

    if not sorted_dates:
        return {
            "dates": [],
            "terms": TERM_ORDER,
            "yields": [],
            "morphology_label": "No Aligned Data",
        }

    # 构建二维数组: yields[time_idx][term_idx]
    date_to_idx = {d: i for i, d in enumerate(sorted_dates)}
    term_to_idx = {t: i for i, t in enumerate(TERM_ORDER)}

    # 初始化二维数组 (NaN 填充)
    yields_matrix = [[None] * len(TERM_ORDER) for _ in range(len(sorted_dates))]

    for term, term_data in yields_by_term.items():
        term_idx = term_to_idx.get(term)
        if term_idx is None:
            continue
        for record_date, value in term_data:
            time_idx = date_to_idx.get(record_date)
            if time_idx is not None:
                yields_matrix[time_idx][term_idx] = round(value, 6)

    # 形态判定
    morphology = _detect_yield_curve_morphology(yields_matrix, TERM_ORDER)

    return {
        "dates": [d.isoformat() for d in sorted_dates],
        "terms": TERM_ORDER,
        "yields": yields_matrix,
        "morphology_label": morphology["label"],
        "morphology_detail": morphology,
    }


def _detect_yield_curve_morphology(
    yields_matrix: List[List[Optional[float]]],
    terms: List[str],
) -> Dict[str, Any]:
    """
    基于短端 (2Y) 与长端 (10Y) 利差变化率判定形态

    形态分类:
      - Bear Steepener (熊陡): 长短端利差扩大, 长端上升更快
      - Bear Flattener (熊平): 长短端利差收窄, 短端上升更快
      - Twist Flattener (扭曲平整): 中期利率相对两端异常
      - Normal (正常): 无明显异常形态
    """
    if not yields_matrix or len(yields_matrix) < 2:
        return {"label": "Insufficient Data"}

    term_indices = {t: i for i, t in enumerate(terms)}
    idx_2y = term_indices.get('2Y')
    idx_10y = term_indices.get('10Y')

    if idx_2y is None or idx_10y is None:
        return {"label": "Missing Key Terms"}

    # 取最近两个时间点的 2Y/10Y 收益率
    recent_dates = yields_matrix[-2:]

    spreads = []
    for date_data in recent_dates:
        y_2y = date_data[idx_2y]
        y_10y = date_data[idx_10y]
        if y_2y is not None and y_10y is not None:
            spreads.append(y_10y - y_2y)

    if len(spreads) < 2:
        return {"label": "Insufficient Spread Data"}

    spread_change = spreads[-1] - spreads[-2]
    current_spread = spreads[-1]

    # 判定逻辑
    if spread_change > 0.001:  # 利差扩大 > 10bp
        if current_spread > 0:
            label = "Bear Steepener"
            description = "熊陡: 长端利率上升快于短端, 市场预期通胀或增长加速"
        else:
            label = "Inversion Deepening"
            description = "倒挂加深: 收益率曲线倒挂程度加剧"
    elif spread_change < -0.001:  # 利差收窄 > 10bp
        if current_spread > 0:
            label = "Bear Flattener"
            description = "熊平: 短端利率上升快于长端, 美联储收紧货币政策"
        else:
            label = "Inversion Easing"
            description = "倒挂缓解: 收益率曲线倒挂程度收窄"
    else:
        label = "Normal"
        description = "正常形态: 长短端利差稳定"

    return {
        "label": label,
        "description": description,
        "current_spread_bps": round(current_spread * 10000, 1),
        "spread_change_bps": round(spread_change * 10000, 1),
    }
