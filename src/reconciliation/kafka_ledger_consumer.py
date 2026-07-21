"""Background Kafka consumer for the ``ledger.events.v1`` topic.

Each committed ledger posting is upserted into the ``external_events``
table with ``source="LEDGER"`` so the matching engine can pull it as
ledger-side entries via the reconciler's external-events view. Using
the existing ingest path keeps idempotency and DLQ handling consistent.

The consumer is optional: it only starts when ``KAFKA_BROKERS`` is set
and Kafka is enabled in settings. Tests inject a fake consumer.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .kafka import Consumer, ConsumerRecord, InMemoryConsumer
from .reconciler import Reconciler

logger = logging.getLogger(__name__)


class KafkaLedgerConsumer:
    """Wraps a :class:`Consumer` and ingests ledger events into the repo."""

    def __init__(
        self,
        consumer: Consumer,
        reconciler: Reconciler,
        *,
        topic: str = "ledger.events.v1",
        source: str = "LEDGER",
    ) -> None:
        self.consumer = consumer
        self.reconciler = reconciler
        self.topic = topic
        self.source = source
        self._task: asyncio.Task[Any] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        """Start the background consume loop."""
        await self.consumer.start()
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Signal the background loop to stop and wait for it."""
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        await self.consumer.stop()

    async def _run(self) -> None:
        try:
            async for record in self._iter():
                await self._handle(record)
        except asyncio.CancelledError:  # pragma: no cover - graceful shutdown
            raise

    async def _iter(self):
        drain = getattr(self.consumer, "drain", None)
        if drain is not None:
            while not self._stopped.is_set():
                async for record in drain():
                    yield record
                await asyncio.sleep(0.01)
        else:
            async for record in self.consumer.consume():
                if self._stopped.is_set():
                    break
                yield record

    async def _handle(self, record: ConsumerRecord) -> None:
        payload = record.value if isinstance(record.value, dict) else {}
        external_event_id = (
            payload.get("external_event_id")
            or payload.get("posting_id")
            or payload.get("entry_id")
            or f"{record.topic}:{record.partition}:{record.offset}"
        )
        normalised = dict(payload)
        normalised.setdefault("source", self.source)
        normalised["external_event_id"] = external_event_id
        try:
            await self.reconciler.ingest(source=self.source, payload=normalised)
        except Exception as e:  # noqa: BLE001 - poison message
            logger.warning(
                "failed to ingest ledger event from %s: %s", record.topic, e
            )

    async def process_once(self) -> int:
        """Drain one batch of currently queued records. Intended for tests."""
        count = 0
        drain = getattr(self.consumer, "drain", None)
        if drain is None:
            return 0
        async for record in drain():
            await self._handle(record)
            count += 1
        return count


def build_ledger_consumer(
    reconciler: Reconciler, settings: Any
) -> KafkaLedgerConsumer | None:
    """Factory: build a KafkaLedgerConsumer when Kafka is configured."""
    brokers = getattr(settings, "kafka_brokers", "")
    enabled = getattr(settings, "enable_kafka", False)
    topic = getattr(settings, "recon_ledger_topic", "ledger.events.v1")
    if not brokers or not enabled:
        return None
    from .kafka import AioKafkaConsumer

    consumer: Consumer = AioKafkaConsumer(
        [topic], brokers=brokers, group_id="reconciliation-ledger"
    )
    return KafkaLedgerConsumer(consumer, reconciler, topic=topic)


__all__ = [
    "KafkaLedgerConsumer",
    "build_ledger_consumer",
    "InMemoryConsumer",
]
