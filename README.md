# Paytm Lakehouse — Hard Problems

This repository isolates three correctness guarantees from the Paytm lakehouse design. It is
deliberately not an end-to-end platform: infrastructure is stubbed where a local example would
otherwise imply guarantees that Kafka, Flink, ClickHouse, Spark, Iceberg, and an audit store do
not share automatically.

```text
src/paytm_deep_dives/
├── streaming_correctness/  # PyFlink topology + ClickHouse boundary
├── data_quality/           # Rules, decision engine, publish gate
└── auditability/           # Critical-metric lifecycle + exact lineage
tests/                      # Behaviour tests for the failure modes
```

## Streaming Correctness

**Problem:** Kafka can duplicate, reorder, delay, or replay events after failure.

**Architecture:** `KafkaSource → event-time watermarks → keyed TTL dedup → event-time window →
IdempotentClickHouseSink`.

**Guarantee:** A stable `event_id` contributes once within the supported TTL, and replaying the
same deterministic window result does not create a second logical business result. The guarantee
lives in Flink keyed state/checkpoints plus the sink's deterministic upsert contract.

**Boundary / assumption:** Flink checkpoints source position and operator state; they do not form
a distributed transaction with ClickHouse. Production must configure durable checkpoint storage
and a ClickHouse table/write/query contract that makes repeated result IDs idempotent. With
`ReplacingMergeTree`, background replacement is asynchronous, so immediate reads require a
correct query strategy such as `FINAL` (or an equivalent serving contract).

**Tests:** `test_duplicate_and_out_of_order_events_use_event_time_once` and
`test_replayed_result_does_not_double_count`. Real recovery remains an integration scenario:
complete a checkpoint, process more records, kill a task, restore, replay, and compare with an
uninterrupted baseline.

## Scalable Data Quality Gate

**Problem:** Expensive scans should not run after an obvious contract break, and failed critical
rules must never reach the trusted dataset.

**Architecture:** `pre-flight rules → PySpark rules → decision engine → publish_if_allowed`.

**Guarantee:** Any BLOCK result returns before `trusted_writer`; WARN publishes while recording
warning evidence and invoking notification. The guarantee lives in `publish_if_allowed`.

**Boundary / assumption:** Spark execution and the trusted writer are external. The included rule
builders use DataFrame expressions, while unit tests exercise policy with small domain fakes rather
than a fake Spark runtime.

**Tests:** `test_broken_contract_skips_expensive_processing_and_publication`,
`test_critical_runtime_failure_is_never_published`, and
`test_warning_is_published_with_audit_evidence_and_notification`.

## Auditable Financial Metrics

**Problem:** A critical financial aggregate must identify the exact transactions used at publish
time; rerunning an old query is not sufficient audit evidence.

**Architecture:** `STARTED → DATA_WRITTEN → EVIDENCE_RECORDED → CONTROLS_VERIFIED → COMPLETE`.
Only COMPLETE runs are visible, and tracing reads persisted contribution rows.

**Guarantee:** Missing evidence or count/total/control failure transitions the run to
`AUDIT_FAILED`; it cannot become visible or return partial lineage. The guarantee lives in
`publish_critical_metric` and `trace_metric_to_transactions`.

**Boundary / assumption:** The data commit and audit-store write are not one atomic transaction.
The durable audit store must atomically record one run's evidence, and consumers must filter on
COMPLETE. Normal BI metrics avoid row-level mappings: they use detailed Gold facts, an immutable
Iceberg snapshot, and a versioned metric definition for on-demand reconstruction.

**Tests:** `test_completed_metric_traces_exact_persisted_contributors`,
`test_metric_without_contribution_evidence_is_not_visible`, and
`test_control_mismatch_prevents_completion`.

## Local checks

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check .
```

PyFlink, Kafka, ClickHouse, Spark, Iceberg, and durable object/audit stores are intentionally not
started by these unit tests. Their deployment configuration, credentials, checkpoint recovery,
and cross-system failure testing belong in an integration environment.

## Official behaviour checked

- [Flink checkpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/),
  [keyed state and TTL](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/),
  [watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/),
  and [Kafka source](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [PySpark DataFrame operations](https://spark.apache.org/docs/latest/api/python/user_guide/dataframes.html)
- [ClickHouse replacement/deduplication behaviour](https://clickhouse.com/resources/engineering/clickhouse-optimize-table-final)
- [Iceberg metadata and snapshots](https://iceberg.apache.org/docs/latest/spark-queries/)
