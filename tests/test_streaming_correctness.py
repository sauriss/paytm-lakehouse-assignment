from datetime import UTC, datetime, timedelta

from paytm_deep_dives.streaming_correctness.job import PaymentEvent, aggregate_events
from paytm_deep_dives.streaming_correctness.sink import IdempotentClickHouseSink


def event(
    event_id: str,
    minute: int,
    amount_paise: int,
    status: str = "SUCCESS",
) -> PaymentEvent:
    return PaymentEvent(
        event_id=event_id,
        payment_id=f"P-{event_id}",
        customer_id="C1",
        event_time=datetime(2026, 9, 27, 10, minute, tzinfo=UTC),
        amount_paise=amount_paise,
        status=status,
    )


def test_duplicate_and_out_of_order_events_use_event_time_once() -> None:
    arrival_order = [
        event("E1", 1, 100),
        event("E2", 4, 200, status="FAILED"),
        event("E1", 1, 100),
        event("E4", 6, 400),
        event("E3", 3, 300),  # Out of order, but its event-time window is still open.
    ]

    results = aggregate_events(
        arrival_order,
        window_size=timedelta(minutes=5),
        watermark=datetime(2026, 9, 27, 10, 4, tzinfo=UTC),
    )

    assert [
        (result.window_start.minute, result.successful_count, result.amount_paise)
        for result in results
    ] == [(0, 2, 400), (5, 1, 400)]

    reordered = aggregate_events(
        list(reversed(arrival_order)),
        window_size=timedelta(minutes=5),
        watermark=datetime(2026, 9, 27, 10, 4, tzinfo=UTC),
    )
    assert reordered == results


class RecordingUpsert:
    def __init__(self) -> None:
        self.rows: dict[str, object] = {}

    def upsert(self, result_id: str, result: object) -> None:
        self.rows[result_id] = result


def test_replayed_result_does_not_double_count() -> None:
    result = aggregate_events(
        [event("E1", 1, 100), event("E2", 2, 200)],
        window_size=timedelta(minutes=5),
    )[0]
    table = RecordingUpsert()
    sink = IdempotentClickHouseSink(table)

    sink.write(result)
    sink.write(result)

    assert len(table.rows) == 1
    persisted = next(iter(table.rows.values()))
    assert persisted.successful_count == 2
    assert persisted.amount_paise == 300
