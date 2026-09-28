from dataclasses import dataclass

from paytm_deep_dives.data_quality.quality_gate import (
    Decision,
    DQRule,
    Evidence,
    Severity,
    required_columns_rule,
    run_quality_gate,
)


@dataclass
class Dataset:
    columns: list[str]
    duplicate_count: int = 0
    invalid_amount_count: int = 0


class Recorder:
    def __init__(self) -> None:
        self.values: list[object] = []

    def __call__(self, value: object) -> None:
        self.values.append(value)


def test_broken_contract_skips_expensive_processing_and_publication() -> None:
    expensive_calls: list[Dataset] = []
    writer = Recorder()

    outcome = run_quality_gate(
        Dataset(columns=["customer_id", "amount_paise"]),
        preflight_rules=[required_columns_rule({"payment_id", "amount_paise"})],
        data_rules=[],
        expensive_processing=lambda data: expensive_calls.append(data) or data,
        trusted_writer=writer,
        warning_audit=Recorder(),
        notify=Recorder(),
    )

    assert outcome.decision is Decision.BLOCK
    assert expensive_calls == []
    assert writer.values == []
    assert outcome.results[0].observed_value == ["amount_paise"]


def test_critical_runtime_failure_is_never_published() -> None:
    writer = Recorder()
    duplicate_rule = DQRule(
        name="payment_id_unique",
        severity=Severity.BLOCK,
        check=lambda data: Evidence(
            passed=data.duplicate_count == 0,
            observed_value=data.duplicate_count,
            expected_value=0,
            message="duplicate payment IDs",
        ),
    )

    outcome = run_quality_gate(
        Dataset(columns=["payment_id", "amount_paise"], duplicate_count=2),
        preflight_rules=[required_columns_rule({"payment_id", "amount_paise"})],
        data_rules=[duplicate_rule],
        expensive_processing=lambda data: data,
        trusted_writer=writer,
        warning_audit=Recorder(),
        notify=Recorder(),
    )

    assert outcome.decision is Decision.BLOCK
    assert writer.values == []


def test_warning_is_published_with_audit_evidence_and_notification() -> None:
    writer = Recorder()
    warning_audit = Recorder()
    notifications = Recorder()
    amount_rule = DQRule(
        name="amount_paise_valid",
        severity=Severity.WARN,
        check=lambda data: Evidence(
            passed=data.invalid_amount_count == 0,
            observed_value=data.invalid_amount_count,
            expected_value=0,
            message="negative payment amounts",
        ),
    )
    dataset = Dataset(
        columns=["payment_id", "amount_paise"],
        invalid_amount_count=1,
    )

    outcome = run_quality_gate(
        dataset,
        preflight_rules=[required_columns_rule({"payment_id", "amount_paise"})],
        data_rules=[amount_rule],
        expensive_processing=lambda data: data,
        trusted_writer=writer,
        warning_audit=warning_audit,
        notify=notifications,
    )

    assert outcome.decision is Decision.WARN
    assert writer.values == [dataset]
    assert [result.rule_name for result in warning_audit.values[0]] == ["amount_paise_valid"]
    assert notifications.values == [warning_audit.values[0]]
