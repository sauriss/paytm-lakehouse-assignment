from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from enum import Enum


class MetricState(Enum):
    STARTED = "STARTED"
    DATA_WRITTEN = "DATA_WRITTEN"
    EVIDENCE_RECORDED = "EVIDENCE_RECORDED"
    CONTROLS_VERIFIED = "CONTROLS_VERIFIED"
    COMPLETE = "COMPLETE"
    AUDIT_FAILED = "AUDIT_FAILED"


class EvidenceRecordingError(RuntimeError):
    pass


class ControlMismatchError(RuntimeError):
    pass


class IncompleteAuditEvidence(RuntimeError):
    pass


@dataclass(frozen=True)
class Contribution:
    aggregate_key: str
    payment_id: str
    amount_paise: int


@dataclass(frozen=True)
class MetricRun:
    metric_run_id: str
    metric_name: str
    metric_version: str
    pipeline_run_id: str
    input_table: str
    input_snapshot_id: str
    output_table: str
    output_snapshot_id: str
    code_version: str
    contributor_count: int
    control_total_paise: int
    dq_status: str
    reconciliation_status: str
    state: MetricState = MetricState.STARTED


class MetricAuditStore:
    """In-memory stand-in for a durable audit store with atomic evidence writes."""

    def __init__(self, *, fail_evidence_recording: bool = False) -> None:
        self._runs: dict[str, MetricRun] = {}
        self._contributions: dict[str, tuple[Contribution, ...]] = {}
        self._fail_evidence_recording = fail_evidence_recording

    def start(self, run: MetricRun) -> None:
        if run.metric_run_id in self._runs:
            raise ValueError(f"metric run already exists: {run.metric_run_id}")
        self._runs[run.metric_run_id] = run

    def set_state(self, metric_run_id: str, state: MetricState) -> MetricRun:
        updated = replace(self.get_run(metric_run_id), state=state)
        self._runs[metric_run_id] = updated
        return updated

    def record_contributions(
        self, metric_run_id: str, contributions: Iterable[Contribution]
    ) -> None:
        if self._fail_evidence_recording:
            raise EvidenceRecordingError("contribution evidence write failed")
        # A production store must make this batch all-or-nothing for one metric run.
        self._contributions[metric_run_id] = tuple(contributions)

    def get_run(self, metric_run_id: str) -> MetricRun:
        return self._runs[metric_run_id]

    def get_contributions(self, metric_run_id: str) -> tuple[Contribution, ...]:
        return self._contributions.get(metric_run_id, ())

    def is_consumer_visible(self, metric_run_id: str) -> bool:
        return self.get_run(metric_run_id).state is MetricState.COMPLETE


def publish_critical_metric(
    run: MetricRun,
    contributions: Iterable[Contribution],
    *,
    write_aggregate: Callable[[MetricRun], None],
    audit_store: MetricAuditStore,
) -> MetricRun:
    """Publish through explicit states; data and audit stores are not one transaction."""
    materialized = tuple(contributions)
    audit_store.start(run)
    write_aggregate(run)
    audit_store.set_state(run.metric_run_id, MetricState.DATA_WRITTEN)

    try:
        audit_store.record_contributions(run.metric_run_id, materialized)
        audit_store.set_state(run.metric_run_id, MetricState.EVIDENCE_RECORDED)
        count = len(materialized)
        total = sum(item.amount_paise for item in materialized)
        controls_ok = (
            count == run.contributor_count
            and total == run.control_total_paise
            and run.dq_status == "PASS"
            and run.reconciliation_status == "PASS"
        )
        if not controls_ok:
            raise ControlMismatchError(
                f"expected count/total {run.contributor_count}/{run.control_total_paise}; "
                f"recorded {count}/{total}"
            )
        audit_store.set_state(run.metric_run_id, MetricState.CONTROLS_VERIFIED)
        return audit_store.set_state(run.metric_run_id, MetricState.COMPLETE)
    except (EvidenceRecordingError, ControlMismatchError):
        audit_store.set_state(run.metric_run_id, MetricState.AUDIT_FAILED)
        raise


def trace_metric_to_transactions(metric_run_id: str, audit_store: MetricAuditStore) -> set[str]:
    if not audit_store.is_consumer_visible(metric_run_id):
        raise IncompleteAuditEvidence(f"metric run is not complete: {metric_run_id}")
    evidence = audit_store.get_contributions(metric_run_id)
    if not evidence:
        raise IncompleteAuditEvidence(f"no contribution evidence: {metric_run_id}")
    return {item.payment_id for item in evidence}
