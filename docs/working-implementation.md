# Working Implementation Guide

## Current executable scope

The repository currently runs deterministic unit tests for the three guarantee-bearing logic
boundaries. It does **not** provision Kafka, Flink, ClickHouse, Spark, Iceberg, Databricks, Airflow,
S3, Unity Catalog, or a durable audit store.

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
```

The steps below describe what must be added for a working integration. They are implementation
requirements, not claims about the current repository.

## Required platform components

| Area | Required components | Production responsibility |
|---|---|---|
| Streaming | Kafka, Schema Registry, Flink, durable checkpoint storage | Event identity, replay, state recovery, watermark and TTL tuning |
| Serving | ClickHouse cluster and explicit table/query contract | Idempotent logical results, retention, concurrency, and read semantics |
| Lakehouse | S3, Iceberg catalog, Databricks/Spark | Immutable landing, table commits, compaction, snapshots, and backfills |
| Orchestration | Airflow and secret-backed connections | Scheduling, retries, reconciliation, and recovery workflows |
| Governance | Unity Catalog or equivalent | Ownership, access control, metadata, and lineage |
| Auditability | Durable audit and contribution store | Atomic evidence recording per run and COMPLETE-only visibility |

## Configuration contract

Configuration should be supplied through deployment configuration or a secrets manager. A working
implementation needs values equivalent to:

- Kafka bootstrap servers, topic, consumer group, and Schema Registry endpoint;
- Flink checkpoint URI, checkpoint interval, restart strategy, and supported lateness/TTL values;
- ClickHouse endpoint, database, target table, authentication secret reference, and deduplication or
  deterministic-upsert contract;
- S3 Raw and warehouse locations, Iceberg catalog configuration, and cloud identity;
- Spark/Databricks workspace and job configuration;
- audit-store connection and serving-promotion configuration.

Do not put credentials or real infrastructure endpoints in repository files.

## Streaming path

1. Create a versioned payment-event schema with a producer-supplied stable `event_id`.
2. Provision Kafka and configure the PyFlink Kafka connector version compatible with the deployed
   Flink runtime.
3. Configure durable Flink checkpoint storage and validate restore from a completed checkpoint.
4. Create the ClickHouse table and serving query together. The chosen engine, deduplication token or
   deterministic replacement key, and query semantics must make repeated logical result IDs appear
   once to consumers.
5. Package the topology in `streaming_correctness/job.py` with deployment-specific dependencies and
   connect its output to the sink implementation.

Flink checkpoint consistency covers Kafka source position and processing state. ClickHouse
idempotency protects the external write. These mechanisms do not form one distributed transaction.

## Lakehouse and Data Quality path

1. Create immutable, versioned S3 landing locations and an Iceberg catalog.
2. Configure Spark with the Iceberg runtime matching the deployed Spark version.
3. Implement source-specific landing adapters that persist native progress state and audit evidence.
4. Run cheap schema/contract checks before Spark scans, then execute the PySpark rules represented in
   `data_quality/quality_gate.py`.
5. Route trusted writes only through `publish_if_allowed`; a `BLOCK` result must terminate before the
   writer is invoked.
6. Orchestrate retries and reconciliation from immutable inputs so reruns are deterministic.

## Critical financial metric path

1. Stage aggregate output under a non-visible run identity.
2. Record the metric run and exact contributions in a durable audit store.
3. Compare contributor count and amount with the actual committed aggregate receipt.
4. Promote the aggregate and transition the audit run to `COMPLETE` only after controls succeed.
5. Configure serving access to require both promotion and `COMPLETE` state.

An Iceberg commit and an external audit-store write are not one atomic transaction. A failure after
the data write must leave the run incomplete and invisible until recovered or explicitly failed.

## Integration tests required before deployment

- Restore Flink from a completed checkpoint after processing additional records and compare the
  result with an uninterrupted baseline.
- Replay the same logical ClickHouse result and verify the consumer query returns one result.
- Send duplicate, out-of-order, accepted-late, and too-late events through the deployed topology.
- Break a required input contract and verify Spark processing and trusted publication do not run.
- Fail contribution recording after aggregate staging and verify the metric remains invisible.
- Reconcile known contributor count and amount against the committed aggregate and stored evidence.
- Exercise Iceberg replay, schema evolution, compaction, and snapshot-retention procedures.

## Suggested delivery sequence

Implement one vertical slice at a time: local guarantee tests, container or development integration,
non-production cloud environment, failure injection, and only then production rollout. Record any
decision that changes a correctness boundary as a new ADR.
