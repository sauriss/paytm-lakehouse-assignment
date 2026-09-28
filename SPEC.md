# Repository Specification

## Purpose

This repository presents the Paytm lakehouse design and three focused code deep dives in a form
that an interviewer, contributor, or coding agent can understand and verify in one sitting.

## Required artifacts

- The final design document in PDF and reviewable Markdown form.
- Concise architecture decision records for the important correctness boundaries.
- A root README that explains repository navigation, local validation, and implementation scope.
- A dedicated deep-dives document containing the detailed guarantee descriptions currently in the
  root README.
- A practical guide describing what is required to turn the examples into working infrastructure.
- A public `AGENTS.md` containing repository-specific rules for automated contributors.
- GitHub CI for linting, formatting, unit tests, and lightweight secret detection.

## Non-negotiable guarantees

Documentation and automation must preserve the existing implementation boundaries:

1. Flink checkpoints protect source position and processing state; they do not form a distributed
   transaction with ClickHouse.
2. A Data Quality `BLOCK` decision must prevent the trusted writer from running.
3. A critical financial metric must not become visible without complete, reconciled contribution
   evidence.
4. Traceability for critical metrics uses persisted contributor evidence rather than rerunning the
   historical pipeline.

## Scope boundaries

- Existing source code and tests will not be redesigned or modified.
- No end-to-end Kafka, Flink, Spark, Iceberg, ClickHouse, or cloud environment will be added.
- The working-implementation guide will describe required infrastructure honestly instead of
  presenting stubbed services as production-ready.
- Security automation is limited to accidental secret detection; heavyweight SAST, dependency,
  container, and infrastructure scanning are out of scope.
- Documentation will stay concise and will not repeat the full design document.

## Acceptance criteria

- The root README links prominently to the deep-dives document and all supporting documentation.
- The design PDF and Markdown source are present under `docs/design/`.
- ADRs document context, decision, consequences, boundaries, and related proof.
- A new contributor can install the project and run its checks from documented commands.
- CI runs Ruff linting and formatting checks, pytest, and lightweight secret scanning.
- `AGENTS.md` tells automated contributors which guarantees must not be weakened and which checks
  are required before completion.
- All existing tests and lint checks pass without source-code changes.
