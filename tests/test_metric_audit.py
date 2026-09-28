from dataclasses import replace

import pytest

from paytm_deep_dives.auditability.metric_audit import (
    AggregateReceipt,
    Contribution,
    ControlMismatchError,
    EvidenceRecordingError,
    IncompleteAuditEvidence,
    InvalidStateTransition,
    MetricAuditStore,
    MetricRun,
    MetricState,
    is_metric_consumer_visible,
    publish_critical_metric,
    trace_metric_to_transactions,
)


def metric_run(*, total: int = 600) -> MetricRun:
    return MetricRun(
        metric_run_id="M9001",
        aggregate_key="merchant=M1,date=2026-09-27",
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


class RecordingAggregateWriter:
    def __init__(self, *, committed_total_paise: int = 600) -> None:
        self.committed_total_paise = committed_total_paise
        self.staged: list[str] = []
        self.promoted: set[str] = set()

    def stage(self, run: MetricRun) -> AggregateReceipt:
        self.staged.append(run.metric_run_id)
        return AggregateReceipt(
            metric_run_id=run.metric_run_id,
            aggregate_key=run.aggregate_key,
            contributor_count=3,
            control_total_paise=self.committed_total_paise,
            output_snapshot_id="81002",
        )

    def promote(self, metric_run_id: str) -> None:
        self.promoted.add(metric_run_id)

    def is_promoted(self, metric_run_id: str) -> bool:
        return metric_run_id in self.promoted


def test_completed_metric_traces_exact_persisted_contributors() -> None:
    store = MetricAuditStore()
    writer = RecordingAggregateWriter()

    published = publish_critical_metric(
        metric_run(),
        CONTRIBUTIONS,
        aggregate_writer=writer,
        audit_store=store,
    )

    assert published.state is MetricState.COMPLETE
    assert writer.staged == ["M9001"]
    assert is_metric_consumer_visible("M9001", store, writer)
    assert trace_metric_to_transactions("M9001", "merchant=M1,date=2026-09-27", store) == {
        "P1",
        "P2",
        "P3",
    }

    with pytest.raises(IncompleteAuditEvidence):
        trace_metric_to_transactions("M9001", "merchant=M2,date=2026-09-27", store)


def test_metric_without_contribution_evidence_is_not_visible() -> None:
    store = MetricAuditStore(fail_evidence_recording=True)
    writer = RecordingAggregateWriter()

    with pytest.raises(EvidenceRecordingError):
        publish_critical_metric(
            metric_run(),
            CONTRIBUTIONS,
            aggregate_writer=writer,
            audit_store=store,
        )

    assert store.get_run("M9001").state is MetricState.AUDIT_FAILED
    assert not writer.is_promoted("M9001")
    assert not is_metric_consumer_visible("M9001", store, writer)
    with pytest.raises(IncompleteAuditEvidence):
        trace_metric_to_transactions("M9001", "merchant=M1,date=2026-09-27", store)


def test_control_mismatch_prevents_completion() -> None:
    store = MetricAuditStore()
    writer = RecordingAggregateWriter()

    with pytest.raises(ControlMismatchError):
        publish_critical_metric(
            metric_run(total=601),
            CONTRIBUTIONS,
            aggregate_writer=writer,
            audit_store=store,
        )

    failed = store.get_run("M9001")
    assert failed.state is MetricState.AUDIT_FAILED
    assert not is_metric_consumer_visible("M9001", store, writer)


def test_committed_aggregate_mismatch_prevents_completion() -> None:
    store = MetricAuditStore()
    writer = RecordingAggregateWriter(committed_total_paise=700)

    with pytest.raises(ControlMismatchError):
        publish_critical_metric(
            metric_run(),
            CONTRIBUTIONS,
            aggregate_writer=writer,
            audit_store=store,
        )

    assert store.get_run("M9001").state is MetricState.AUDIT_FAILED
    assert not writer.is_promoted("M9001")


def test_invalid_state_transition_is_rejected() -> None:
    store = MetricAuditStore()
    store.start(metric_run())

    with pytest.raises(InvalidStateTransition):
        store.transition("M9001", MetricState.COMPLETE)

    with pytest.raises(InvalidStateTransition):
        MetricAuditStore().start(replace(metric_run(), state=MetricState.COMPLETE))
