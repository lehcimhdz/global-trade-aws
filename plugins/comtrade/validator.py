"""
Data quality validator for Comtrade bronze-layer responses.

Design
------
Each check is a plain function that receives the list of records and returns a
``CheckResult``.  The ``run_checks`` helper calls them all and collects results.
``assert_quality`` raises ``DataQualityError`` if any ERROR-severity check failed.

This module has **no Airflow dependency** — it is pure Python so it can be
unit-tested without an Airflow install.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────


class Severity(str, Enum):
    ERROR = "error"      # fails the DAG run
    WARNING = "warning"  # logs but does not fail


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    severity: Severity = Severity.ERROR
    details: Dict[str, Any] = field(default_factory=dict)


class DataQualityError(Exception):
    """Raised by ``assert_quality`` when one or more ERROR checks fail."""

    def __init__(self, failures: List[CheckResult]) -> None:
        self.failures = failures
        summary = "; ".join(f"{r.name}: {r.message}" for r in failures)
        super().__init__(f"Data quality check(s) failed — {summary}")


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────


def check_envelope(data: Any) -> CheckResult:
    """
    The Comtrade API must return a dict (the response envelope).
    A bare list is acceptable for some endpoints; anything else is not.
    """
    if isinstance(data, (dict, list)):
        return CheckResult(
            name="check_envelope",
            passed=True,
            message="Response is a dict or list.",
        )
    return CheckResult(
        name="check_envelope",
        passed=False,
        message=f"Expected dict or list, got {type(data).__name__}.",
    )


def check_has_data_key(data: Any) -> CheckResult:
    """
    When the response is a dict it must have a 'data' key whose value is a list.
    If it is already a list this check passes unconditionally.
    """
    if isinstance(data, list):
        return CheckResult(
            name="check_has_data_key",
            passed=True,
            message="Response is a bare list — no envelope expected.",
        )
    if not isinstance(data, dict):
        return CheckResult(
            name="check_has_data_key",
            passed=False,
            message="Response is not a dict.",
        )
    if "data" not in data:
        return CheckResult(
            name="check_has_data_key",
            passed=False,
            message="Response dict is missing the 'data' key.",
            details={"keys_present": list(data.keys())},
        )
    if not isinstance(data["data"], list):
        return CheckResult(
            name="check_has_data_key",
            passed=False,
            message=f"'data' value is {type(data['data']).__name__}, expected list.",
        )
    return CheckResult(
        name="check_has_data_key",
        passed=True,
        message=f"Response envelope valid. count={data.get('count', '?')}",
    )


def check_row_count(records: List[Dict], min_rows: int = 1) -> CheckResult:
    """Records list must contain at least *min_rows* entries."""
    count = len(records)
    if count >= min_rows:
        return CheckResult(
            name="check_row_count",
            passed=True,
            message=f"{count} row(s) returned (minimum {min_rows}).",
            details={"row_count": count},
        )
    return CheckResult(
        name="check_row_count",
        passed=False,
        message=f"Got {count} row(s), expected at least {min_rows}.",
        details={"row_count": count, "min_rows": min_rows},
    )


def check_no_nulls(records: List[Dict], columns: List[str]) -> CheckResult:
    """
    None of the *columns* may have null / None / empty-string values
    in any record.
    """
    if not columns or not records:
        return CheckResult(
            name="check_no_nulls",
            passed=True,
            message="No columns to check or no records.",
        )

    violations: Dict[str, int] = {}
    for col in columns:
        null_count = sum(
            1 for r in records if r.get(col) is None or r.get(col) == ""
        )
        if null_count:
            violations[col] = null_count

    if not violations:
        return CheckResult(
            name="check_no_nulls",
            passed=True,
            message=f"No nulls in required columns: {columns}.",
        )
    return CheckResult(
        name="check_no_nulls",
        passed=False,
        message=f"Null values found in {list(violations.keys())}.",
        details={"null_counts": violations},
    )


def check_numeric_non_negative(
    records: List[Dict],
    columns: List[str],
    severity: Severity = Severity.WARNING,
) -> CheckResult:
    """
    Numeric columns must be >= 0.  Missing values are ignored (they are
    caught by ``check_no_nulls`` if that column is required).
    Defaults to WARNING because the Comtrade API occasionally returns
    negative correction values.
    """
    if not columns or not records:
        return CheckResult(
            name="check_numeric_non_negative",
            passed=True,
            message="No columns to check or no records.",
            severity=severity,
        )

    violations: Dict[str, int] = {}
    for col in columns:
        neg_count = 0
        for r in records:
            val = r.get(col)
            if val is not None:
                try:
                    if float(val) < 0:
                        neg_count += 1
                except (TypeError, ValueError):
                    pass
        if neg_count:
            violations[col] = neg_count

    if not violations:
        return CheckResult(
            name="check_numeric_non_negative",
            passed=True,
            message=f"All values >= 0 in: {columns}.",
            severity=severity,
        )
    return CheckResult(
        name="check_numeric_non_negative",
        passed=False,
        message=f"Negative values found in {list(violations.keys())}.",
        severity=severity,
        details={"negative_counts": violations},
    )


def check_period_format(records: List[Dict], freq_code: str) -> CheckResult:
    """
    Period values must match the expected format for the given frequency:
      - Annual  (A): YYYY          e.g. "2023"
      - Monthly (M): YYYYMM        e.g. "202301"
    Records missing the 'period' key are ignored (caught by check_no_nulls).
    """
    pattern = re.compile(r"^\d{4}$") if freq_code == "A" else re.compile(r"^\d{6}$")
    expected = "YYYY" if freq_code == "A" else "YYYYMM"

    bad: List[str] = []
    for r in records:
        val = r.get("period")
        if val is not None and not pattern.match(str(val)):
            bad.append(str(val))

    if not bad:
        return CheckResult(
            name="check_period_format",
            passed=True,
            message=f"All period values match {expected} format.",
        )
    sample = bad[:5]
    return CheckResult(
        name="check_period_format",
        passed=False,
        message=f"Period values do not match {expected}. Samples: {sample}",
        details={"bad_values_sample": sample, "total_bad": len(bad)},
    )


def check_no_duplicates(
    records: List[Dict],
    key_columns: List[str],
    severity: Severity = Severity.WARNING,
) -> CheckResult:
    """
    No two records should share identical values for all *key_columns*.
    Defaults to WARNING — duplicates are unusual but the API preview endpoint
    can return them for multi-year queries.
    """
    if not key_columns or not records:
        return CheckResult(
            name="check_no_duplicates",
            passed=True,
            message="No key columns specified or no records.",
            severity=severity,
        )

    seen: set = set()
    duplicates = 0
    for r in records:
        key = tuple(r.get(c) for c in key_columns)
        if key in seen:
            duplicates += 1
        seen.add(key)

    if duplicates == 0:
        return CheckResult(
            name="check_no_duplicates",
            passed=True,
            message=f"No duplicate keys on {key_columns}.",
            severity=severity,
        )
    return CheckResult(
        name="check_no_duplicates",
        passed=False,
        message=f"{duplicates} duplicate record(s) on key {key_columns}.",
        severity=severity,
        details={"duplicate_count": duplicates},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────


def run_checks(
    data: Any,
    required_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
    dedup_columns: Optional[List[str]] = None,
    min_rows: int = 1,
    freq_code: Optional[str] = None,
) -> List[CheckResult]:
    """
    Execute the full check suite against *data* (the raw API response).
    Returns all CheckResults regardless of pass/fail — the caller decides
    what to do with them.
    """
    results: List[CheckResult] = []

    # Structural checks
    results.append(check_envelope(data))
    results.append(check_has_data_key(data))

    # Extract records for content checks
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = data.get("data", [])
    else:
        records = []

    if not isinstance(records, list):
        records = []

    # Content checks
    results.append(check_row_count(records, min_rows=min_rows))

    if records:
        if required_columns:
            results.append(check_no_nulls(records, required_columns))
        if numeric_columns:
            results.append(check_numeric_non_negative(records, numeric_columns))
        if freq_code:
            results.append(check_period_format(records, freq_code))
        if dedup_columns:
            results.append(check_no_duplicates(records, dedup_columns))

    return results


def assert_quality(results: List[CheckResult]) -> None:
    """
    Log every result then raise ``DataQualityError`` if any ERROR-severity
    check failed.  WARNING failures are logged but do not raise.
    """
    failures = []
    for r in results:
        if r.passed:
            logger.info("  PASS  [%s] %s", r.name, r.message)
        elif r.severity == Severity.WARNING:
            logger.warning("  WARN  [%s] %s | details=%s", r.name, r.message, r.details)
        else:
            logger.error("  FAIL  [%s] %s | details=%s", r.name, r.message, r.details)
            failures.append(r)

    if failures:
        raise DataQualityError(failures)
