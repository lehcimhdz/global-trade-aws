"""
Unit tests for plugins/comtrade/validator.py

No Airflow dependency — the validator module is pure Python.
"""
from __future__ import annotations

import pytest

from comtrade.validator import (
    CheckResult,
    DataQualityError,
    Severity,
    assert_quality,
    check_envelope,
    check_has_data_key,
    check_no_duplicates,
    check_no_nulls,
    check_numeric_non_negative,
    check_period_format,
    check_row_count,
    run_checks,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _record(**kwargs):
    return kwargs


def _envelope(records, count=None):
    return {"data": records, "count": count if count is not None else len(records)}


# ── check_envelope ─────────────────────────────────────────────────────────────


class TestCheckEnvelope:
    def test_dict_passes(self):
        result = check_envelope({"data": []})
        assert result.passed

    def test_list_passes(self):
        result = check_envelope([{"a": 1}])
        assert result.passed

    def test_string_fails(self):
        result = check_envelope("bad")
        assert not result.passed
        assert "str" in result.message

    def test_none_fails(self):
        result = check_envelope(None)
        assert not result.passed

    def test_int_fails(self):
        result = check_envelope(42)
        assert not result.passed

    def test_failed_result_is_error_severity(self):
        result = check_envelope(None)
        assert result.severity == Severity.ERROR


# ── check_has_data_key ────────────────────────────────────────────────────────


class TestCheckHasDataKey:
    def test_bare_list_passes(self):
        result = check_has_data_key([{"a": 1}])
        assert result.passed

    def test_dict_with_data_list_passes(self):
        result = check_has_data_key({"data": [{"a": 1}], "count": 1})
        assert result.passed

    def test_dict_missing_data_key_fails(self):
        result = check_has_data_key({"records": []})
        assert not result.passed
        assert "keys_present" in result.details

    def test_data_key_not_list_fails(self):
        result = check_has_data_key({"data": "not-a-list"})
        assert not result.passed

    def test_non_dict_non_list_fails(self):
        result = check_has_data_key("oops")
        assert not result.passed

    def test_count_shown_in_message(self):
        result = check_has_data_key({"data": [], "count": 0})
        assert result.passed
        assert "0" in result.message


# ── check_row_count ───────────────────────────────────────────────────────────


class TestCheckRowCount:
    def test_exact_minimum_passes(self):
        assert check_row_count([{}], min_rows=1).passed

    def test_above_minimum_passes(self):
        assert check_row_count([{}, {}], min_rows=1).passed

    def test_zero_rows_fails_default(self):
        result = check_row_count([], min_rows=1)
        assert not result.passed
        assert result.details["row_count"] == 0

    def test_zero_rows_passes_min_zero(self):
        assert check_row_count([], min_rows=0).passed

    def test_details_contain_counts(self):
        result = check_row_count([], min_rows=5)
        assert result.details["min_rows"] == 5
        assert result.details["row_count"] == 0


# ── check_no_nulls ────────────────────────────────────────────────────────────


class TestCheckNoNulls:
    def test_no_nulls_passes(self):
        records = [_record(reporterCode="840", period="2023")]
        assert check_no_nulls(records, ["reporterCode", "period"]).passed

    def test_none_value_fails(self):
        records = [_record(reporterCode=None, period="2023")]
        result = check_no_nulls(records, ["reporterCode"])
        assert not result.passed
        assert result.details["null_counts"]["reporterCode"] == 1

    def test_empty_string_fails(self):
        records = [_record(reporterCode="", period="2023")]
        result = check_no_nulls(records, ["reporterCode"])
        assert not result.passed

    def test_missing_key_counted_as_null(self):
        records = [{"period": "2023"}]  # no reporterCode key
        result = check_no_nulls(records, ["reporterCode"])
        assert not result.passed

    def test_empty_columns_list_passes(self):
        assert check_no_nulls([{"a": None}], []).passed

    def test_empty_records_passes(self):
        assert check_no_nulls([], ["reporterCode"]).passed

    def test_multiple_columns_partial_failure(self):
        records = [_record(reporterCode=None, period="2023")]
        result = check_no_nulls(records, ["reporterCode", "period"])
        assert not result.passed
        assert "reporterCode" in result.details["null_counts"]
        assert "period" not in result.details["null_counts"]


# ── check_numeric_non_negative ────────────────────────────────────────────────


class TestCheckNumericNonNegative:
    def test_positive_values_pass(self):
        records = [_record(primaryValue=100.0), _record(primaryValue=0)]
        assert check_numeric_non_negative(records, ["primaryValue"]).passed

    def test_negative_value_fails(self):
        records = [_record(primaryValue=-1)]
        result = check_numeric_non_negative(records, ["primaryValue"])
        assert not result.passed
        assert result.details["negative_counts"]["primaryValue"] == 1

    def test_default_severity_is_warning(self):
        records = [_record(primaryValue=-1)]
        result = check_numeric_non_negative(records, ["primaryValue"])
        assert result.severity == Severity.WARNING

    def test_none_values_ignored(self):
        records = [_record(primaryValue=None)]
        assert check_numeric_non_negative(records, ["primaryValue"]).passed

    def test_string_numbers_coerced(self):
        records = [_record(primaryValue="50.5")]
        assert check_numeric_non_negative(records, ["primaryValue"]).passed

    def test_non_numeric_strings_ignored(self):
        records = [_record(primaryValue="N/A")]
        assert check_numeric_non_negative(records, ["primaryValue"]).passed

    def test_empty_columns_passes(self):
        assert check_numeric_non_negative([_record(v=1)], []).passed

    def test_custom_severity_propagated(self):
        records = [_record(v=-5)]
        result = check_numeric_non_negative(records, ["v"], severity=Severity.ERROR)
        assert result.severity == Severity.ERROR


# ── check_period_format ───────────────────────────────────────────────────────


class TestCheckPeriodFormat:
    def test_annual_valid(self):
        records = [_record(period="2023"), _record(period="2000")]
        assert check_period_format(records, "A").passed

    def test_annual_invalid(self):
        result = check_period_format([_record(period="202301")], "A")
        assert not result.passed

    def test_monthly_valid(self):
        records = [_record(period="202301"), _record(period="200012")]
        assert check_period_format(records, "M").passed

    def test_monthly_invalid(self):
        result = check_period_format([_record(period="2023")], "M")
        assert not result.passed

    def test_missing_period_key_ignored(self):
        records = [{"reporterCode": "840"}]  # no period key
        assert check_period_format(records, "A").passed

    def test_bad_sample_in_details(self):
        records = [_record(period="bad"), _record(period="2023")]
        result = check_period_format(records, "A")
        assert not result.passed
        assert "bad" in result.details["bad_values_sample"]

    def test_integer_period_coerced_to_string(self):
        records = [_record(period=2023)]
        assert check_period_format(records, "A").passed


# ── check_no_duplicates ───────────────────────────────────────────────────────


class TestCheckNoDuplicates:
    def test_unique_records_pass(self):
        records = [
            _record(reporterCode="840", period="2023"),
            _record(reporterCode="276", period="2023"),
        ]
        assert check_no_duplicates(records, ["reporterCode", "period"]).passed

    def test_duplicate_records_fail(self):
        records = [
            _record(reporterCode="840", period="2023"),
            _record(reporterCode="840", period="2023"),
        ]
        result = check_no_duplicates(records, ["reporterCode", "period"])
        assert not result.passed
        assert result.details["duplicate_count"] == 1

    def test_default_severity_is_warning(self):
        records = [_record(a=1), _record(a=1)]
        result = check_no_duplicates(records, ["a"])
        assert result.severity == Severity.WARNING

    def test_empty_key_columns_passes(self):
        records = [_record(a=1), _record(a=1)]
        assert check_no_duplicates(records, []).passed

    def test_empty_records_passes(self):
        assert check_no_duplicates([], ["reporterCode"]).passed

    def test_three_records_two_duplicates(self):
        records = [_record(x=1), _record(x=1), _record(x=1)]
        result = check_no_duplicates(records, ["x"])
        assert result.details["duplicate_count"] == 2


# ── run_checks ────────────────────────────────────────────────────────────────


class TestRunChecks:
    def _good_response(self):
        return _envelope([_record(reporterCode="840", period="2023", primaryValue=100)])

    def test_returns_list_of_check_results(self):
        results = run_checks(self._good_response())
        assert isinstance(results, list)
        assert all(isinstance(r, CheckResult) for r in results)

    def test_structural_checks_always_run(self):
        results = run_checks(self._good_response())
        names = [r.name for r in results]
        assert "check_envelope" in names
        assert "check_has_data_key" in names

    def test_row_count_check_included(self):
        results = run_checks(self._good_response())
        names = [r.name for r in results]
        assert "check_row_count" in names

    def test_required_columns_check_runs_when_provided(self):
        results = run_checks(self._good_response(), required_columns=["reporterCode"])
        names = [r.name for r in results]
        assert "check_no_nulls" in names

    def test_numeric_check_runs_when_provided(self):
        results = run_checks(self._good_response(), numeric_columns=["primaryValue"])
        names = [r.name for r in results]
        assert "check_numeric_non_negative" in names

    def test_period_check_runs_when_freq_code_provided(self):
        results = run_checks(self._good_response(), freq_code="A")
        names = [r.name for r in results]
        assert "check_period_format" in names

    def test_dedup_check_runs_when_provided(self):
        results = run_checks(self._good_response(), dedup_columns=["reporterCode"])
        names = [r.name for r in results]
        assert "check_no_duplicates" in names

    def test_content_checks_skipped_on_bad_envelope(self):
        results = run_checks("not-a-dict-or-list")
        names = [r.name for r in results]
        assert "check_row_count" in names  # still runs (empty records)
        assert not any(n == "check_no_nulls" for n in names)

    def test_bare_list_response_handled(self):
        results = run_checks([_record(a=1)])
        assert all(r.passed for r in results if r.name in ("check_envelope", "check_has_data_key"))

    def test_all_checks_pass_on_good_data(self):
        results = run_checks(
            self._good_response(),
            required_columns=["reporterCode", "period"],
            numeric_columns=["primaryValue"],
            dedup_columns=["reporterCode", "period"],
            freq_code="A",
        )
        error_failures = [r for r in results if not r.passed and r.severity == Severity.ERROR]
        assert error_failures == []

    def test_min_rows_respected(self):
        results = run_checks(_envelope([]), min_rows=5)
        row_check = next(r for r in results if r.name == "check_row_count")
        assert not row_check.passed


# ── assert_quality ────────────────────────────────────────────────────────────


class TestAssertQuality:
    def _pass(self, name="ok"):
        return CheckResult(name=name, passed=True, message="fine")

    def _fail_error(self, name="bad"):
        return CheckResult(name=name, passed=False, message="fail", severity=Severity.ERROR)

    def _fail_warning(self, name="warn"):
        return CheckResult(name=name, passed=False, message="warn", severity=Severity.WARNING)

    def test_all_pass_no_exception(self):
        assert_quality([self._pass(), self._pass()])  # no raise

    def test_error_failure_raises(self):
        with pytest.raises(DataQualityError) as exc_info:
            assert_quality([self._pass(), self._fail_error()])
        assert len(exc_info.value.failures) == 1
        assert exc_info.value.failures[0].name == "bad"

    def test_warning_failure_does_not_raise(self):
        assert_quality([self._fail_warning()])  # no raise

    def test_exception_message_includes_check_names(self):
        with pytest.raises(DataQualityError) as exc_info:
            assert_quality([self._fail_error("check_row_count")])
        assert "check_row_count" in str(exc_info.value)

    def test_multiple_error_failures_all_in_exception(self):
        with pytest.raises(DataQualityError) as exc_info:
            assert_quality([self._fail_error("a"), self._fail_error("b")])
        assert len(exc_info.value.failures) == 2

    def test_mixed_results_only_errors_in_failures(self):
        with pytest.raises(DataQualityError) as exc_info:
            assert_quality([self._fail_warning(), self._fail_error("only_this")])
        names = [r.name for r in exc_info.value.failures]
        assert names == ["only_this"]

    def test_empty_results_no_exception(self):
        assert_quality([])  # no raise
