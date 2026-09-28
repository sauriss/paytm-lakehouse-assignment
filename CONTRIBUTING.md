# Contributing

Keep changes focused on the three hard problems described in [DEEP_DIVES.md](DEEP_DIVES.md).

## Setup

```bash
python -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

Create a short-lived branch from the current `main`. Do not commit directly to `main`, rewrite
shared history, or include unrelated cleanup.

## Before opening a pull request

```bash
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/pytest -q
```

Also confirm:

- guarantee-bearing behaviour has a failure-mode test;
- relevant ADRs and deep-dive documentation still match the implementation;
- infrastructure assumptions are explicit rather than simulated;
- no credentials, local environments, caches, or generated output are committed.

Use concise commits that describe the outcome, for example `docs: record streaming sink boundary`
or `test: cover blocked publication path`. A pull request should explain the guarantee affected,
the failure mode covered, and the commands used for verification.

Read [AGENTS.md](AGENTS.md) for the non-negotiable correctness rules that also apply to automated
contributors.
