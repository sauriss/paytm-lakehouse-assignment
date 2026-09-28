from __future__ import annotations

from typing import Protocol

from paytm_deep_dives.streaming_correctness.job import WindowResult


class DeterministicResultWriter(Protocol):
    def upsert(self, result_id: str, result: WindowResult) -> None: ...


class IdempotentClickHouseSink:
    """Defines the external idempotency contract; it is not a distributed transaction.

    Production must implement ``upsert`` so repeated result IDs replace/deduplicate the
    logical row. For ClickHouse that requires an explicit table/insert-token/query contract;
    a deterministic ID alone is insufficient, and ReplacingMergeTree merges are asynchronous.
    """

    def __init__(self, writer: DeterministicResultWriter) -> None:
        self._writer = writer

    def write(self, result: WindowResult) -> None:
        self._writer.upsert(result.result_id, result)
