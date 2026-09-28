# Paytm Lakehouse Assignment

> **Reviewing the guarantee-bearing implementation? Start with
> [Deep Dives](DEEP_DIVES.md).** It maps each hard problem to its implementation boundary,
> assumptions, and failure-mode tests.

This repository accompanies the Paytm lakehouse design submission. It contains three deliberately
small Python deep dives that demonstrate streaming correctness, a scalable data-quality publish
gate, and exact contribution lineage for critical financial metrics.

The repository is not a deployable data platform. Kafka, Flink, ClickHouse, Spark, Iceberg, object
storage, and the audit store remain explicit external boundaries rather than misleading local
simulations.

## Repository map

```text
src/paytm_deep_dives/
├── streaming_correctness/  # Event-time deduplication and idempotent sink boundary
├── data_quality/           # Contract checks, decisions, and trusted publish gate
└── auditability/           # Critical-metric evidence and publication lifecycle
tests/                      # Behaviour tests for the stated guarantees
DEEP_DIVES.md               # Detailed guarantee, boundary, and test explanations
SPEC.md                     # Lightweight repository requirements
docs/                       # Design, decisions, and implementation guidance
```

## Local setup

Requires Python 3.11 or newer.

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

On Windows, activate the environment and use the equivalent commands under `.venv\\Scripts`.

## Run the checks

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/pytest -q
```

The unit tests execute the guarantee-bearing domain and policy logic directly. They intentionally
do not start local substitutes for Kafka, Flink, ClickHouse, Spark, Iceberg, or cloud services.

## Documentation

- [Deep dives and guarantees](DEEP_DIVES.md)
- [Repository specification](SPEC.md)

The final design, architecture decisions, agent guidance, and working-infrastructure path are kept
under `docs/` and are linked here as they are added.

## Making it operational

Turning these deep dives into a running platform requires real checkpoint storage, source and sink
configuration, durable catalog and audit stores, production table contracts, credentials, and
failure-injection integration tests. The repository documentation describes that path without
claiming those external guarantees are implemented locally.
