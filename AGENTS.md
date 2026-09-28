# Repository Agent Guide

This repository demonstrates three architecture guarantees from the Paytm lakehouse assignment.
Automated contributors should optimize for correctness, reviewability, and honest system boundaries,
not for framework breadth.

## Read first

1. [README.md](README.md) for setup and repository navigation.
2. [SPEC.md](SPEC.md) for scope and acceptance criteria.
3. [DEEP_DIVES.md](DEEP_DIVES.md) for guarantee ownership and failure-mode tests.
4. [Architecture decisions](docs/adr/README.md) before changing a protected boundary.

## Protected guarantees

- Flink owns Kafka source position and processing state through checkpoints. Never add custom offset
  persistence or claim that a checkpoint and ClickHouse write share a distributed transaction.
- A Data Quality `BLOCK` result must return before the trusted writer can run.
- A critical financial metric is consumer-visible only after aggregate data, contribution evidence,
  and controls reconcile successfully.
- Critical-metric tracing reads persisted contribution evidence; it does not rerun the original
  pipeline to infer contributors.

Any change that affects one of these guarantees must update its behavioural test and the relevant
deep-dive or ADR in the same change.

## Change rules

- Keep framework wiring, correctness logic, decision policy, and external side effects separate.
- Prefer small typed functions and explicit domain names over generic abstractions.
- Do not add production frameworks, deployment manifests, or fake infrastructure unless the task
  explicitly requires a working integration.
- External systems must remain explicit interfaces when their guarantees cannot be reproduced
  locally.
- Do not commit credentials, tokens, private endpoints, generated caches, or virtual environments.
- Preserve the repository's approximately 200-400 meaningful production-line scope unless a new
  requirement justifies expansion.

## Required checks

Run from the repository root:

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/pytest -q
```

Review `git diff` and confirm that documentation links resolve and that no statement overclaims an
external guarantee.

## Definition of done

- The smallest relevant implementation demonstrates the stated guarantee.
- A failure-mode test would fail if that guarantee were removed.
- Documentation names the assumption and external boundary.
- Formatting, lint, tests, and secret detection pass.
- No unrelated files or generated artifacts are included.
