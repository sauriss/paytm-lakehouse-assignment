# ADR 0001: Use Immutable Raw Storage and an Apache Iceberg Lakehouse

**Status:** Accepted

## Context

The platform ingests databases, event streams, partner files, APIs, and operational spreadsheets.
These sources expose different progress markers and failure modes, but all require replayable
source evidence, controlled schema evolution, historical correction, and common governance.

## Decision

Land source evidence immutably in Amazon S3, then process it through Apache Iceberg Bronze, Silver,
and Gold tables using Databricks/Spark. Source-specific adapters retain native progress state while
sharing validation, idempotency, audit, reconciliation, and recovery controls.

## Alternatives considered

- Direct source-to-serving writes reduce latency but weaken replay and auditability.
- A warehouse-only design simplifies the platform but is less suitable for immutable source
  evidence, large backfills, and multi-engine access.
- Delta Lake or Hudi can provide related table capabilities; Iceberg was selected for open,
  multi-engine interoperability. The choice is not a claim that it is universally faster.

## Consequences

- Raw evidence can be replayed without re-reading mutable upstream systems.
- Bronze, Silver, and Gold provide explicit trust and consumer boundaries.
- The platform must operate file sizing, compaction, snapshot retention, catalog integration, and
  reconciliation controls.
- Selected low-latency workloads still require a separate streaming serving path.

## Boundaries

The repository does not deploy S3, Spark, Iceberg, Databricks, or Unity Catalog. Atomicity exists
within an Iceberg table commit; it does not make external audit stores or serving systems part of
the same transaction.

## Related evidence

- [Design document](../design/paytm-design-doc.md)
- [Repository specification](../../SPEC.md)
