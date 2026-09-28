# ADR 0002: Separate Flink Recovery from ClickHouse Idempotency

**Status:** Accepted

## Context

Kafka events may be duplicated, arrive out of event-time order, or be replayed after failure. Flink
can checkpoint Kafka source position and operator state, but a ClickHouse write is an external side
effect and does not automatically join that checkpoint transaction.

## Decision

Use stable producer-supplied event IDs, keyed state with bounded TTL, event-time watermarks, and
Flink checkpoints for source and processing recovery. Write aggregates with a deterministic logical
result ID through an idempotent ClickHouse boundary whose production table and query contract makes
repeated writes represent one logical result.

## Alternatives considered

- Claiming end-to-end exactly-once across Kafka, Flink, and ClickHouse was rejected because the
  design has no distributed transaction across those systems.
- Relying only on Kafka offset commits was rejected because offsets do not restore keyed dedup or
  window state.
- Unbounded event-ID retention was rejected because state must have an operational limit.

## Consequences

- Replayed events are suppressed within the supported TTL.
- Recovery restores source position and Flink state from a completed checkpoint.
- The sink contract, table engine/settings, and serving query must be validated together.
- Very late corrections outside the streaming window use the historical lakehouse path.

## Boundaries

Deterministic identity alone is insufficient. Production must configure and test the selected
ClickHouse replacement/deduplication behavior and its read semantics. This repository does not
claim a Kafka-offset-plus-ClickHouse distributed transaction.

## Related evidence

- [Flink topology](../../src/paytm_deep_dives/streaming_correctness/job.py)
- [ClickHouse boundary](../../src/paytm_deep_dives/streaming_correctness/sink.py)
- [Failure-mode tests](../../tests/test_streaming_correctness.py)
- [Deep-dive explanation](../../DEEP_DIVES.md#streaming-correctness)
