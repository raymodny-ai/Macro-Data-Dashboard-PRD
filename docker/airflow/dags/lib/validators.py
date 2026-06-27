"""
数据质量验证模块
架构原则: 入库前强校验, 防止脏数据污染下游
"""
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataQualityReport:
    """数据质量报告"""

    def __init__(self):
        self.total_records = 0
        self.passed_records = 0
        self.failed_records = 0
        self.warnings: List[str] = []
        self.errors: List[str] = []

    @property
    def pass_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.passed_records / self.total_records

    @property
    def is_acceptable(self) -> bool:
        """通过率 >= 80% 视为可接受 (允许部分数据源短暂异常)"""
        return self.pass_rate >= 0.80

    def summary(self) -> str:
        return (
            f"QualityReport: {self.passed_records}/{self.total_records} passed "
            f"({self.pass_rate:.1%}), "
            f"{len(self.errors)} errors, {len(self.warnings)} warnings"
        )


def validate_liquidity_records(
    records: List[Dict[str, Any]],
    spread_min: float = -2.0,
    spread_max: float = 2.0,
    max_consecutive_nulls: int = 3,
) -> Tuple[List[Dict[str, Any]], DataQualityReport]:
    """
    流动性走廊数据质量验证
    
    验证规则:
    1. record_date 必须为有效日期字符串
    2. SOFR/IORB 不能同时为 None (否则无意义)
    3. spread 必须在合理范围 [spread_min, spread_max] 内
    4. system_state 必须为 0/1/2 之一
    5. 连续空值天数不得超过 max_consecutive_nulls
    
    Args:
        records: 原始记录列表
        spread_min: 利差合理下限
        spread_max: 利差合理上限
        max_consecutive_nulls: 最大连续空值天数
    
    Returns:
        (验证通过的记录列表, 质量报告)
    """
    report = DataQualityReport()
    report.total_records = len(records)
    valid_records = []
    consecutive_nulls = 0

    for i, rec in enumerate(records):
        errors = []

        # 规则1: 日期格式验证
        try:
            datetime.strptime(rec.get('record_date', ''), '%Y-%m-%d')
        except ValueError:
            errors.append(f"Row {i}: invalid date '{rec.get('record_date')}'")

        # 规则2: SOFR/IORB 不能同时为空
        if rec.get('sofr_rate') is None and rec.get('iorb_rate') is None:
            consecutive_nulls += 1
            if consecutive_nulls > max_consecutive_nulls:
                errors.append(
                    f"Row {i}: {consecutive_nulls} consecutive nulls "
                    f"(max allowed: {max_consecutive_nulls})"
                )
        else:
            consecutive_nulls = 0

        # 规则3: 利差范围验证
        spread = rec.get('spread')
        if spread is not None:
            if not (spread_min <= spread <= spread_max):
                errors.append(
                    f"Row {i}: spread {spread} out of range "
                    f"[{spread_min}, {spread_max}]"
                )

        # 规则4: 系统状态值验证
        state = rec.get('system_state')
        if state is not None and state not in (0, 1, 2):
            errors.append(f"Row {i}: invalid system_state {state} (must be 0/1/2)")

        # 汇总
        if errors:
            report.failed_records += 1
            report.errors.extend(errors)
        else:
            report.passed_records += 1
            valid_records.append(rec)

    logger.info(report.summary())
    if report.errors:
        for err in report.errors[:10]:
            logger.warning(f"  DQ error: {err}")
        if len(report.errors) > 10:
            logger.warning(f"  ... and {len(report.errors) - 10} more errors")

    return valid_records, report


def detect_crisis_burst(
    records: List[Dict[str, Any]],
    burst_threshold_bps: float = 0.10,
) -> List[Dict[str, Any]]:
    """
    "水管爆裂" 预警检测
    
    判定逻辑: 单日 SOFR-IORB 利差飙升 >= burst_threshold_bps (默认10bp = 0.10%)
    表明银行体系流动性瞬间枯竭, 类似2019年9月回购市场危机
    
    Args:
        records: 按日期排序的记录列表
        burst_threshold_bps: 单日飙升阈值 (百分点, 默认0.10即10bp)
    
    Returns:
        触发预警的记录列表 (crisis_alert=True 已标记)
    """
    alerts = []
    prev_spread = None

    for rec in records:
        current_spread = rec.get('spread')
        if current_spread is None:
            prev_spread = None
            continue

        if prev_spread is not None:
            delta = current_spread - prev_spread
            if delta >= burst_threshold_bps:
                rec['crisis_alert'] = True
                alerts.append(rec)
                logger.critical(
                    f"PIPE BURST ALERT: {rec['record_date']} "
                    f"spread jumped +{delta:.4f} ({delta*100:.1f}bp) "
                    f"from {prev_spread:.4f} to {current_spread:.4f}"
                )

        prev_spread = current_spread

    if alerts:
        logger.critical(f"PIPE BURST: {len(alerts)} alerts triggered!")
    else:
        logger.info("No pipe burst alerts detected")

    return alerts


def validate_record_count(
    expected_min: int,
    actual_count: int,
    context: str = "",
) -> bool:
    """
    记录数合理性校验
    T-180 窗口预期 ~120个交易日, 允许 ±20% 偏差
    
    Args:
        expected_min: 预期最小记录数
        actual_count: 实际记录数
        context: 上下文描述 (日志用)
    
    Returns:
        True 表示通过
    """
    if actual_count < expected_min:
        logger.error(
            f"Record count too low: {actual_count} < {expected_min} "
            f"(context: {context})"
        )
        return False
    return True


class CircuitBreakerTripped(Exception):
    """熔断器触发异常: 数据漂移超出安全边界"""
    def __init__(self, message: str, quarantined: list):
        super().__init__(message)
        self.quarantined = quarantined


class DataDriftCircuitBreaker:
    """
    数据漂移/异常熔断器

    架构原则:
      - 宏观数据 (SOFR/MOVE/利差) 直接决定风险判定 (绿灯/黄灯/红灯)
      - 外部 API 返回脏数据 (空值/小数点错误) 可能导致“流动性瘫痪”误报
      - 入库前强边界校验: 超出安全阈值的记录先进入“人工复核池” (隔离队列)

    安全边界参考 (历史极值 × 1.5 宽限):
      - SOFR: [-0.50%, 6.00%]  (2020负值 + 2023高位)
      - IORB: [-0.50%, 6.00%]
      - SOFR-IORB spread: [-1.00%, 1.00%]  (正常 ±20bp, 极端 ±100bp)
      - MOVE index: [30, 300]  (历史区间 30~200, 极端 250+)
      - CPI MoM: [-2.0%, +2.0%]  (月环比极端值)
      - Bid-to-cover: [0.5, 10.0]  (拍卖认购倍数)
    """

    # 安全边界: {field_name: (min, max)}
    DEFAULT_BOUNDS = {
        'sofr_rate': (-0.50, 6.00),
        'iorb_rate': (-0.50, 6.00),
        'spread': (-1.00, 1.00),
        'move_index': (30.0, 300.0),
        'mom_growth': (-2.0, 2.0),
        'acceleration': (-2.0, 2.0),
        'bid_to_cover_ratio': (0.5, 10.0),
        'close_price': (0.01, 100000.0),
        'value': (-10.0, 100.0),  # 通用收益率字段
    }

    def __init__(self, custom_bounds: Optional[dict] = None):
        self.bounds = {**self.DEFAULT_BOUNDS}
        if custom_bounds:
            self.bounds.update(custom_bounds)
        self._trip_count = 0

    def check_record(
        self,
        record: Dict[str, Any],
        strict_fields: Optional[List[str]] = None,
    ) -> Optional[str]:
        """
        单条记录边界校验

        Args:
            record: 数据记录字典
            strict_fields: 仅校验指定字段 (默认全部已定义字段)

        Returns:
            None 表示通过; 否则返回违规描述
        """
        violations = []
        fields = strict_fields or list(self.bounds.keys())

        for field in fields:
            if field not in self.bounds:
                continue
            val = record.get(field)
            if val is None:
                continue
            try:
                val = float(val)
            except (TypeError, ValueError):
                violations.append(f"{field}={record.get(field)} is not numeric")
                continue

            lo, hi = self.bounds[field]
            if not (lo <= val <= hi):
                violations.append(
                    f"{field}={val:.6f} outside [{lo}, {hi}]"
                )

        if violations:
            return "; ".join(violations)
        return None

    def check_batch(
        self,
        records: List[Dict[str, Any]],
        max_drift_pct: float = 0.05,
        strict_fields: Optional[List[str]] = None,
    ) -> tuple:
        """
        批量记录边界校验 + 熔断

        Args:
            records: 记录列表
            max_drift_pct: 最大允许漂移比例 (超出则熔断)
            strict_fields: 仅校验指定字段

        Returns:
            (通过记录, 隔离记录, 报告字典)

        Raises:
            CircuitBreakerTripped: 当漂移比例超过 max_drift_pct 时触发
        """
        passed = []
        quarantined = []

        for rec in records:
            violation = self.check_record(rec, strict_fields)
            if violation is None:
                passed.append(rec)
            else:
                rec['_drift_reason'] = violation
                quarantined.append(rec)
                logger.warning(
                    f"DRIFT QUARANTINE: {violation} "
                    f"| date={rec.get('record_date', rec.get('trade_date', 'N/A'))}"
                )

        total = len(records)
        drift_pct = len(quarantined) / total if total > 0 else 0.0

        report = {
            'total': total,
            'passed': len(passed),
            'quarantined': len(quarantined),
            'drift_pct': round(drift_pct, 4),
            'circuit_tripped': drift_pct > max_drift_pct,
        }

        if drift_pct > max_drift_pct:
            self._trip_count += 1
            msg = (
                f"CIRCUIT BREAKER TRIPPED: {len(quarantined)}/{total} "
                f"({drift_pct:.1%}) records drifted (threshold: {max_drift_pct:.1%}). "
                f"Cumulative trips: {self._trip_count}. "
                f"Quarantined records sent to manual review pool."
            )
            logger.critical(msg)
            raise CircuitBreakerTripped(msg, quarantined)

        logger.info(
            f"Drift check passed: {len(passed)}/{total} OK, "
            f"{len(quarantined)} quarantined ({drift_pct:.1%})"
        )
        return passed, quarantined, report

    @property
    def trip_count(self) -> int:
        """累计熔断次数 (监控指标)"""
        return self._trip_count


# 全局熔断器实例
liquidity_circuit_breaker = DataDriftCircuitBreaker(
    custom_bounds={
        # 流动性走廊专用: 更严格的利差边界
        'spread': (-0.50, 0.50),  # ±50bp (100bp突变则熔断)
    }
)

market_circuit_breaker = DataDriftCircuitBreaker()
inflation_circuit_breaker = DataDriftCircuitBreaker()
