# ADR 0003: Persist Exact Contribution Lineage for Critical Metrics

**Status:** Accepted

## Context

Finance and regulatory consumers may need the exact transactions behind a published aggregate.
Rerunning an historical query is not sufficient evidence: code, source data, or operational context
may have changed, and an Iceberg snapshot identifies table state rather than the exact contribution
set used for a particular published number.

## Decision

For critical financial metrics, persist contribution rows keyed by metric run and aggregate key at
publication time. Reconcile contributor count and control total against the committed aggregate,
then promote the result only after data, evidence, and controls succeed. Trace requests read the
persisted evidence rather than rerunning the pipeline.

## Alternatives considered

- Snapshot-only reconstruction remains appropriate for ordinary analytics but was rejected as the
  sole control for critical published metrics.
- Persisting row-level mappings for every BI metric was rejected because of storage and operational
  cost.
- Treating data and audit-store writes as one atomic transaction was rejected because the selected
  systems do not provide that cross-system transaction.

## Consequences

- A completed critical metric can return its exact contributing payment IDs.
- Evidence storage grows with critical metric contributions and requires retention controls.
- Failure after data write leaves the run incomplete and invisible rather than silently published.
- Normal analytical metrics continue to use versioned facts, snapshots, and metric definitions.

## Boundaries

The durable audit store must record one run's evidence atomically, and the serving layer must expose
only promoted rows whose audit state is `COMPLETE`. Those external enforcement points are modeled
as explicit interfaces, not implemented infrastructure.

## Related evidence

- [Publication lifecycle](../../src/paytm_deep_dives/auditability/metric_audit.py)
- [Auditability tests](../../tests/test_metric_audit.py)
- [Deep-dive explanation](../../DEEP_DIVES.md#auditable-financial-metrics)
