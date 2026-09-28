from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class PaymentEvent:
    event_id: str
    payment_id: str
    customer_id: str
    event_time: datetime
    amount_paise: int
    status: str

    @classmethod
    def from_json(cls, value: str) -> PaymentEvent:
        payload = json.loads(value)
        return cls(
            event_id=payload["event_id"],
            payment_id=payload["payment_id"],
            customer_id=payload["customer_id"],
            event_time=datetime.fromisoformat(payload["event_time"]).astimezone(UTC),
            amount_paise=int(payload["amount_paise"]),
            status=payload["status"],
        )


@dataclass(frozen=True)
class WindowResult:
    customer_id: str
    window_start: datetime
    window_end: datetime
    successful_count: int
    amount_paise: int

    @property
    def result_id(self) -> str:
        return f"{self.customer_id}|{self.window_start.isoformat()}|{self.window_end.isoformat()}"


@dataclass(frozen=True)
class FlinkJobConfig:
    bootstrap_servers: str
    topic: str
    consumer_group: str
    checkpoint_interval_ms: int = 30_000
    watermark_delay_seconds: int = 120
    window_minutes: int = 5
    dedup_ttl_hours: int = 24


def aggregate_events(
    events: Iterable[PaymentEvent],
    *,
    window_size: timedelta,
    watermark: datetime | None = None,
) -> list[WindowResult]:
    """Reference logic for the business guarantee, independent of Flink internals."""
    seen: set[str] = set()
    totals: dict[tuple[str, datetime], tuple[int, int]] = {}
    window_seconds = int(window_size.total_seconds())
    if window_seconds <= 0:
        raise ValueError("window_size must be positive")

    for event in events:
        if event.event_id in seen:
            continue
        epoch = int(event.event_time.timestamp())
        start = datetime.fromtimestamp(epoch - epoch % window_seconds, tz=UTC)
        if watermark is not None and start + window_size <= watermark:
            continue
        seen.add(event.event_id)
        if event.status != "SUCCESS":
            continue
        count, amount = totals.get((event.customer_id, start), (0, 0))
        totals[(event.customer_id, start)] = (count + 1, amount + event.amount_paise)

    return [
        WindowResult(customer, start, start + window_size, count, amount)
        for (customer, start), (count, amount) in sorted(totals.items())
    ]


def build_flink_job(env: object, config: FlinkJobConfig) -> object:
    """Build the real topology; deployment supplies checkpoint storage and the sink."""
    from pyflink.common import Duration, SimpleStringSchema, Types
    from pyflink.common.time import Time
    from pyflink.datastream import CheckpointingMode
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )
    from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction
    from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
    from pyflink.datastream.watermark_strategy import TimestampAssigner, WatermarkStrategy
    from pyflink.datastream.window import TumblingEventTimeWindows

    class EventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value: PaymentEvent, record_timestamp: int) -> int:
            return int(value.event_time.timestamp() * 1000)

    class DeduplicateEvents(KeyedProcessFunction):
        def open(self, runtime_context: object) -> None:
            descriptor = ValueStateDescriptor("seen-event", Types.BOOLEAN())
            ttl = (
                StateTtlConfig.new_builder(Time.hours(config.dedup_ttl_hours))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            descriptor.enable_time_to_live(ttl)
            self.seen = runtime_context.get_state(descriptor)

        def process_element(self, value: PaymentEvent, ctx: object):
            if self.seen.value() is None:
                self.seen.update(True)
                yield value

    class SuccessfulPaymentsWindow(ProcessWindowFunction):
        def process(self, customer_id: str, context: object, elements: Iterable[PaymentEvent]):
            successful = [event for event in elements if event.status == "SUCCESS"]
            if successful:
                yield WindowResult(
                    customer_id=customer_id,
                    window_start=datetime.fromtimestamp(context.window().start / 1000, tz=UTC),
                    window_end=datetime.fromtimestamp(context.window().end / 1000, tz=UTC),
                    successful_count=len(successful),
                    amount_paise=sum(event.amount_paise for event in successful),
                )

    env.enable_checkpointing(config.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    checkpoints = env.get_checkpoint_config()
    checkpoints.set_min_pause_between_checkpoints(10_000)
    checkpoints.set_checkpoint_timeout(120_000)
    checkpoints.set_max_concurrent_checkpoints(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.topic)
        .set_group_id(config.consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(config.watermark_delay_seconds)
        )
        .with_timestamp_assigner(EventTimestampAssigner())
        .with_idleness(Duration.of_minutes(1))
    )
    events = env.from_source(source, WatermarkStrategy.no_watermarks(), "payments-kafka").map(
        PaymentEvent.from_json
    )
    return (
        events.assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda event: event.event_id)
        .process(DeduplicateEvents())
        .key_by(lambda event: event.customer_id)
        .window(TumblingEventTimeWindows.of(Time.minutes(config.window_minutes)))
        .process(SuccessfulPaymentsWindow())
    )
