# AI-Native Repository Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a concise, agent-readable documentation system and GitHub CI without changing the existing implementation or tests.

**Architecture:** The root README becomes the navigation and execution entry point, while the existing guarantee narrative moves intact to `DEEP_DIVES.md`. Supporting documents capture the final design, decisions, working-infrastructure path, and agent rules; GitHub Actions enforce existing Ruff, pytest, and lightweight secret checks.

**Tech Stack:** Markdown, PDF, Python 3.11, Ruff, pytest, GitHub Actions, Gitleaks

**Spec:** `SPEC.md`

## Global Constraints

- Do not modify files under `src/` or `tests/`.
- Do not add or imply end-to-end infrastructure that is not implemented.
- Preserve the four non-negotiable guarantees in `SPEC.md`.
- Keep security automation limited to accidental secret detection.
- Keep documentation concise and avoid repeating the full design document.

## Review Focus

- Case-sensitive or broken documentation links must fail local link validation.
- The design PDF and Markdown source must both be present and clearly identified.
- CI must use the repository's existing Python and Ruff configuration.
- Secret scanning must inspect repository history without adding broad SAST tooling.
- The final diff must contain no changes under `src/` or `tests/`.

---

### Task 1: Repository entry point and deep-dive guide

**Files:**
- Rename: `README.md` to `DEEP_DIVES.md`
- Create: `README.md`

**Interfaces:**
- Consumes: existing guarantee narrative and commands from `README.md`
- Produces: a root navigation page linking to all repository documentation

- [ ] Rename the existing README to `DEEP_DIVES.md`, preserving its guarantee content.
- [ ] Create a concise root README with a top-level deep-dives link, repository map, setup, execution, validation, and infrastructure-boundary sections.
- [ ] Verify every local Markdown link resolves with a case-sensitive path check.
- [ ] Commit the repository navigation changes.

### Task 2: Design document and architecture decisions

**Files:**
- Create: `docs/design/paytm-design-doc.pdf`
- Create: `docs/design/paytm-design-doc.md`
- Create: `docs/adr/README.md`
- Create: `docs/adr/0001-immutable-raw-and-iceberg-lakehouse.md`
- Create: `docs/adr/0002-flink-clickhouse-correctness-boundary.md`
- Create: `docs/adr/0003-critical-metric-contribution-lineage.md`

**Interfaces:**
- Consumes: approved design artifacts and existing guarantee-bearing code/tests
- Produces: submission-ready architecture documentation and decision history

- [ ] Copy the approved PDF and its Markdown source into `docs/design/` without rewriting technical content.
- [ ] Add three concise ADRs containing status, context, decision, alternatives, consequences, boundaries, and proof links.
- [ ] Add an ADR index explaining the one-decision-per-file convention.
- [ ] Verify the PDF opens, remains at most six pages, and all ADR code/test links resolve.
- [ ] Commit the design and ADR artifacts.

### Task 3: Agent and working-implementation guidance

**Files:**
- Create: `AGENTS.md`
- Create: `CONTRIBUTING.md`
- Create: `docs/working-implementation.md`

**Interfaces:**
- Consumes: `SPEC.md`, `DEEP_DIVES.md`, and current project commands
- Produces: public modification rules and an honest productionisation path

- [ ] Add a repository-specific `AGENTS.md` covering navigation, protected guarantees, allowed scope, required checks, and definition of done.
- [ ] Add a short contributor guide with branch, commit, validation, and pull-request expectations.
- [ ] Document the infrastructure, configuration, deployment order, and integration tests needed to run Kafka/Flink/ClickHouse and Spark/Iceberg paths.
- [ ] Verify no document claims the external systems share an atomic transaction or that stubbed infrastructure is currently runnable.
- [ ] Commit the guidance documents.

### Task 4: Continuous integration

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/secret-scan.yml`

**Interfaces:**
- Consumes: `pyproject.toml` dev dependencies and existing tests
- Produces: pull-request and push checks for format, lint, unit tests, and accidental secrets

- [ ] Verify current official GitHub and Gitleaks action usage before authoring workflows.
- [ ] Add Python 3.11 CI that installs `.[dev]`, runs `ruff format --check .`, `ruff check .`, and `pytest -q`.
- [ ] Add a separate Gitleaks workflow with full-history checkout; do not add Bandit, dependency audit, container scan, or SAST.
- [ ] Validate workflow YAML syntax and run the equivalent lint/test commands locally.
- [ ] Commit the CI workflows.

### Task 5: Final review, push, and merge

**Files:**
- Modify only documentation if link or wording defects are found during review.

**Interfaces:**
- Consumes: completed documentation and CI tasks
- Produces: verified `main` branch on the remote repository

- [ ] Confirm `git diff origin/main -- src tests` is empty.
- [ ] Verify all Markdown links, the PDF page count, workflow YAML, Ruff checks, and pytest results.
- [ ] Review the complete diff for generated files, secrets, unsupported guarantees, and documentation duplication.
- [ ] Push `feature/ai-native-repository`, merge it into `main` without force-pushing, and push `main`.
- [ ] Confirm local `main`, `origin/main`, and the merge commit agree.
