from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Severity(Enum):
    WARN = "WARN"
    BLOCK = "BLOCK"


class Decision(Enum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCK = "BLOCK"


@dataclass(frozen=True)
class Evidence:
    passed: bool
    observed_value: object
    expected_value: object
    message: str


@dataclass(frozen=True)
class RuleResult:
    rule_name: str
    severity: Severity
    passed: bool
    observed_value: object
    expected_value: object
    message: str


@dataclass(frozen=True)
class DQRule:
    name: str
    severity: Severity
    check: Callable[[Any], Evidence]
    threshold: float | int | None = None

    def evaluate(self, dataset: object) -> RuleResult:
        evidence = self.check(dataset)
        return RuleResult(self.name, self.severity, **vars(evidence))


@dataclass(frozen=True)
class QualityOutcome:
    decision: Decision
    results: tuple[RuleResult, ...]
    published: bool


def required_columns_rule(required: set[str]) -> DQRule:
    def check(dataset: object) -> Evidence:
        present = set(dataset.columns)
        missing = sorted(required - present)
        return Evidence(
            passed=not missing,
            observed_value=sorted(required & present),
            expected_value=sorted(required),
            message="required columns present" if not missing else f"missing columns: {missing}",
        )

    return DQRule("required_columns", Severity.BLOCK, check)


def decide(results: Iterable[RuleResult]) -> Decision:
    failures = {result.severity for result in results if not result.passed}
    if Severity.BLOCK in failures:
        return Decision.BLOCK
    if Severity.WARN in failures:
        return Decision.WARN
    return Decision.PASS


def publish_if_allowed(
    dataset: object,
    results: Iterable[RuleResult],
    *,
    trusted_writer: Callable[[object], None],
    warning_audit: Callable[[tuple[RuleResult, ...]], None],
    notify: Callable[[tuple[RuleResult, ...]], None],
) -> QualityOutcome:
    materialized = tuple(results)
    decision = decide(materialized)
    if decision is Decision.BLOCK:
        return QualityOutcome(decision, materialized, published=False)

    # This branch is the hard gate: no BLOCK result can reach the trusted writer.
    trusted_writer(dataset)
    if decision is Decision.WARN:
        warnings = tuple(
            result
            for result in materialized
            if not result.passed and result.severity is Severity.WARN
        )
        warning_audit(warnings)
        notify(warnings)
    return QualityOutcome(decision, materialized, published=True)


def run_quality_gate(
    dataset: object,
    *,
    preflight_rules: Iterable[DQRule],
    data_rules: Iterable[DQRule],
    expensive_processing: Callable[[object], object],
    trusted_writer: Callable[[object], None],
    warning_audit: Callable[[tuple[RuleResult, ...]], None],
    notify: Callable[[tuple[RuleResult, ...]], None],
) -> QualityOutcome:
    preflight = tuple(rule.evaluate(dataset) for rule in preflight_rules)
    if decide(preflight) is Decision.BLOCK:
        return publish_if_allowed(
            dataset,
            preflight,
            trusted_writer=trusted_writer,
            warning_audit=warning_audit,
            notify=notify,
        )

    prepared = expensive_processing(dataset)
    results = preflight + tuple(rule.evaluate(prepared) for rule in data_rules)
    return publish_if_allowed(
        prepared,
        results,
        trusted_writer=trusted_writer,
        warning_audit=warning_audit,
        notify=notify,
    )


def payment_id_completeness_rule(minimum_ratio: float = 1.0) -> DQRule:
    def check(dataframe: Any) -> Evidence:
        total = dataframe.count()
        valid = dataframe.filter(dataframe.payment_id.isNotNull()).count()
        ratio = valid / total if total else 1.0
        return Evidence(ratio >= minimum_ratio, ratio, minimum_ratio, "payment_id completeness")

    return DQRule("payment_id_complete", Severity.BLOCK, check, minimum_ratio)


def unique_payment_id_rule() -> DQRule:
    def check(dataframe: Any) -> Evidence:
        from pyspark.sql import functions as F

        duplicates = dataframe.groupBy("payment_id").count().filter(F.col("count") > 1).count()
        return Evidence(duplicates == 0, duplicates, 0, "duplicate payment IDs")

    return DQRule("payment_id_unique", Severity.BLOCK, check, 0)


def valid_amount_rule(severity: Severity = Severity.WARN) -> DQRule:
    def check(dataframe: Any) -> Evidence:
        from pyspark.sql import functions as F

        invalid = dataframe.filter(
            F.col("amount_paise").isNull() | (F.col("amount_paise") < 0)
        ).count()
        return Evidence(invalid == 0, invalid, 0, "null or negative amount_paise")

    return DQRule("amount_paise_valid", severity, check, 0)
