# Architecture Decision Records

Each ADR records one decision whose trade-offs or correctness boundary materially affects the
design. Accepted ADRs are not implementation claims: their **Boundaries** sections identify what
the repository demonstrates and what production infrastructure must still provide.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-immutable-raw-and-iceberg-lakehouse.md) | Immutable Raw and Apache Iceberg lakehouse | Accepted |
| [0002](0002-flink-clickhouse-correctness-boundary.md) | Flink and ClickHouse correctness boundary | Accepted |
| [0003](0003-critical-metric-contribution-lineage.md) | Exact contribution lineage for critical metrics | Accepted |

Create a new numbered ADR when a decision changes. Do not silently rewrite the outcome of an
accepted record; supersede it and link the replacement.
