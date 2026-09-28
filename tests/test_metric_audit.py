import pytest

from paytm_deep_dives.auditability.metric_audit import (
    Contribution,
    ControlMismatchError,
    EvidenceRecordingError,
    IncompleteAuditEvidence,
    MetricAuditStore,
    MetricRun,
    MetricState,
    publish_critical_metric,
    trace_metric_to_transactions,
)


def metric_run(*, total: int = 600) -> MetricRun:
    return MetricRun(
        metric_run_id="M9001",
        metric_name="successful_payments",
        metric_version="v1",
        pipeline_run_id="PIPE-42",
        input_table="gold.payment_fact",
        input_snapshot_id="81001",
        output_table="finance.daily_payments",
        output_snapshot_id="81002",
        code_version="abc123",
        contributor_count=3,
        control_total_paise=total,
        dq_status="PASS",
        reconciliation_status="PASS",
    )


CONTRIBUTIONS = [
    Contribution("merchant=M1,date=2026-09-27", "P1", 100),
    Contribution("merchant=M1,date=2026-09-27", "P2", 200),
    Contribution("merchant=M1,date=2026-09-27", "P3", 300),
]


def test_completed_metric_traces_exact_persisted_contributors() -> None:
    store = MetricAuditStore()
    writes: list[str] = []

    published = publish_critical_metric(
        metric_run(),
        CONTRIBUTIONS,
        write_aggregate=lambda run: writes.append(run.metric_run_id),
        audit_store=store,
    )

    assert published.state is MetricState.COMPLETE
    assert writes == ["M9001"]
    assert store.is_consumer_visible("M9001")
    assert trace_metric_to_transactions("M9001", store) == {"P1", "P2", "P3"}


def test_metric_without_contribution_evidence_is_not_visible() -> None:
    store = MetricAuditStore(fail_evidence_recording=True)

    with pytest.raises(EvidenceRecordingError):
        publish_critical_metric(
            metric_run(),
            CONTRIBUTIONS,
            write_aggregate=lambda run: None,
            audit_store=store,
        )

    assert store.get_run("M9001").state is MetricState.AUDIT_FAILED
    assert not store.is_consumer_visible("M9001")
    with pytest.raises(IncompleteAuditEvidence):
        trace_metric_to_transactions("M9001", store)


def test_control_mismatch_prevents_completion() -> None:
    store = MetricAuditStore()

    with pytest.raises(ControlMismatchError):
        publish_critical_metric(
            metric_run(total=601),
            CONTRIBUTIONS,
            write_aggregate=lambda run: None,
            audit_store=store,
        )

    failed = store.get_run("M9001")
    assert failed.state is MetricState.AUDIT_FAILED
    assert not store.is_consumer_visible("M9001")
