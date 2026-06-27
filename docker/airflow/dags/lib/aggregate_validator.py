"""
连续聚合刷新后一致性校验脚本
架构原则: 按需刷新后自动校验聚合结果与原始数据的一致性,
         确保 refresh_continuous_aggregate 不引入数据偏差。

校验逻辑:
  1. 从源表聚合计算 COUNT / AVG / MIN / MAX
  2. 从连续聚合视图读取对应窗口的计算结果
  3. 比对: 行数差异 ≤ ±5%, 均值差异 ≤ 0.01%

使用方式 (DAG task 内调用):
  from lib.aggregate_validator import validate_aggregate_consistency
  report = validate_aggregate_consistency(
      view_name='spread_percentiles_30d',
      source_table='liquidity_corridor',
      start_date='2024-01-01',
      end_date='2024-06-30',
  )
  if not report.is_consistent:
      raise ValueError(f"Consistency check failed: {report.summary()}")
"""
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from lib.db_utils import get_db_connection

logger = logging.getLogger(__name__)


@dataclass
class ConsistencyReport:
    """一致性校验报告"""
    view_name: str = ""
    source_table: str = ""
    start_date: str = ""
    end_date: str = ""
    # 源表统计
    source_count: int = 0
    source_avg: Optional[float] = None
    source_min: Optional[float] = None
    source_max: Optional[float] = None
    # 视图统计
    view_count: int = 0
    view_avg: Optional[float] = None
    # 偏差
    count_diff_pct: float = 0.0
    avg_diff_pct: float = 0.0
    # 阈值
    count_tolerance_pct: float = 5.0
    avg_tolerance_pct: float = 0.01
    # 错误
    errors: list = field(default_factory=list)

    @property
    def is_consistent(self) -> bool:
        """一致性判定: 行数和均值偏差均在容忍范围内"""
        if self.errors:
            return False
        return (
            abs(self.count_diff_pct) <= self.count_tolerance_pct
            and abs(self.avg_diff_pct) <= self.avg_tolerance_pct
        )

    def summary(self) -> str:
        status = "PASS" if self.is_consistent else "FAIL"
        return (
            f"[{status}] {self.view_name} vs {self.source_table} "
            f"[{self.start_date} → {self.end_date}]: "
            f"source={self.source_count} rows (avg={self.source_avg}), "
            f"view={self.view_count} rows (avg={self.view_avg}), "
            f"count_diff={self.count_diff_pct:.2f}%, "
            f"avg_diff={self.avg_diff_pct:.4f}%"
        )


def validate_aggregate_consistency(
    view_name: str,
    source_table: str,
    start_date: str,
    end_date: str,
    date_column: str = 'record_date',
    value_column: str = 'spread',
    filter_condition: str = "symbol = 'SPREAD'",
    count_tolerance_pct: float = 5.0,
    avg_tolerance_pct: float = 0.01,
) -> ConsistencyReport:
    """
    校验连续聚合视图与源表的数据一致性

    Args:
        view_name: 连续聚合视图名称 (如 'spread_percentiles_30d')
        source_table: 源超表名称 (如 'liquidity_corridor')
        start_date: 校验窗口起始 (YYYY-MM-DD)
        end_date: 校验窗口结束 (YYYY-MM-DD)
        date_column: 日期列名
        value_column: 值列名
        filter_condition: 额外过滤条件 (如 "symbol = 'SPREAD'")
        count_tolerance_pct: 行数偏差容忍 (%)
        avg_tolerance_pct: 均值偏差容忍 (%)

    Returns:
        ConsistencyReport 校验报告
    """
    report = ConsistencyReport(
        view_name=view_name,
        source_table=source_table,
        start_date=start_date,
        end_date=end_date,
        count_tolerance_pct=count_tolerance_pct,
        avg_tolerance_pct=avg_tolerance_pct,
    )

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # ---- 1. 源表聚合统计 ----
                where_clause = (
                    f"{date_column} BETWEEN '{start_date}' AND '{end_date}'"
                )
                if filter_condition:
                    where_clause += f" AND {filter_condition}"

                source_sql = f"""
                    SELECT
                        COUNT(*) AS cnt,
                        AVG({value_column}) AS avg_val,
                        MIN({value_column}) AS min_val,
                        MAX({value_column}) AS max_val
                    FROM {source_table}
                    WHERE {where_clause}
                """
                cur.execute(source_sql)
                row = cur.fetchone()
                if row:
                    report.source_count = row[0] or 0
                    report.source_avg = float(row[1]) if row[1] is not None else None
                    report.source_min = float(row[2]) if row[2] is not None else None
                    report.source_max = float(row[3]) if row[3] is not None else None

                # ---- 2. 视图聚合统计 ----
                view_sql = f"""
                    SELECT
                        COUNT(*) AS cnt,
                        AVG(spread_p50_30d) AS avg_val
                    FROM {view_name}
                    WHERE bucket BETWEEN '{start_date}' AND '{end_date}'
                """
                cur.execute(view_sql)
                row = cur.fetchone()
                if row:
                    report.view_count = row[0] or 0
                    report.view_avg = float(row[1]) if row[1] is not None else None

                # ---- 3. 计算偏差 ----
                if report.source_count > 0:
                    report.count_diff_pct = (
                        (report.view_count - report.source_count) / report.source_count * 100
                    )
                if report.source_avg is not None and report.source_avg != 0 and report.view_avg is not None:
                    report.avg_diff_pct = (
                        abs(report.view_avg - report.source_avg) / abs(report.source_avg) * 100
                    )

    except Exception as e:
        report.errors.append(f"DB query failed: {e}")
        logger.error(f"Consistency validation error: {e}")

    # ---- 4. 日志输出 ----
    logger.info(report.summary())
    if not report.is_consistent:
        for err in report.errors:
            logger.error(f"  Error: {err}")

    return report


def validate_spread_percentiles(
    start_date: str,
    end_date: str,
) -> ConsistencyReport:
    """
    便捷函数: 校验 spread_percentiles_30d 视图一致性

    Args:
        start_date: 校验窗口起始
        end_date: 校验窗口结束
    """
    return validate_aggregate_consistency(
        view_name='spread_percentiles_30d',
        source_table='liquidity_corridor',
        start_date=start_date,
        end_date=end_date,
        date_column='record_date',
        value_column='spread',
        filter_condition="symbol = 'SPREAD'",
    )
