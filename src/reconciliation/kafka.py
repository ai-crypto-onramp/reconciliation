"""Kafka consumer/producer abstractions.

Provides a small protocol-based interface with two implementations:
- ``InMemoryConsumer``/``InMemoryProducer``: backed by in-memory queues,
  used in unit tests and for local development.
- ``AioKafkaConsumer``/``AioKafkaProducer``: thin wrappers around aiokafka
  used in production. Tests never require a real Kafka broker.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Iterable, Sequence
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class ConsumerRecord:
    """Minimal Kafka record representation."""

    __slots__ = ("topic", "partition", "offset", "key", "value", "headers")

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: str | None,
        value: Any,
        headers: dict[str, bytes] | None = None,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.headers = headers or {}

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<ConsumerRecord {self.topic}:{self.partition}:{self.offset}>"


class Consumer(Protocol):
    """At-least-once Kafka consumer interface."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def consume(self) -> AsyncIterator[ConsumerRecord]: ...
    async def commit(self) -> None: ...


class Producer(Protocol):
    """At-least-once Kafka producer interface."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, topic: str, payload: dict[str, Any], key: str | None = None) -> None: ...


class InMemoryConsumer:
    """Consumer backed by in-memory queues grouped by topic.

    Tests pre-populate the queue via ``enqueue``. The consumer yields records
    until the queue is empty, then blocks (cooperatively) until more arrive or
    ``stop()`` is called.
    """

    def __init__(self, topics: Iterable[str]) -> None:
        self.topics = list(topics)
        self._queues: dict[str, deque[ConsumerRecord]] = {t: deque() for t in self.topics}
        self._stopped = asyncio.Event()
        self._offsets: dict[str, int] = defaultdict(int)

    def enqueue(self, topic: str, value: Any, key: str | None = None, headers: dict[str, bytes] | None = None) -> None:
        queue = self._queues.setdefault(topic, deque())
        record = ConsumerRecord(
            topic=topic,
            partition=0,
            offset=len(queue),
            key=key,
            value=value,
            headers=headers,
        )
        queue.append(record)

    async def start(self) -> None:
        self._stopped.clear()

    async def stop(self) -> None:
        self._stopped.set()

    async def consume(self) -> AsyncIterator[ConsumerRecord]:
        while not self._stopped.is_set():
            yielded = False
            for topic in self.topics:
                queue = self._queues.get(topic)
                if queue:
                    record = queue.popleft()
                    self._offsets[topic] = record.offset + 1
                    yielded = True
                    yield record
            if not yielded:
                await asyncio.sleep(0.01)

    async def drain(self) -> AsyncIterator[ConsumerRecord]:
        """Yield all currently queued records, then return.

        Intended for tests: drains the in-memory queues without blocking.
        """
        while not self._stopped.is_set():
            yielded = False
            for topic in self.topics:
                queue = self._queues.get(topic)
                if queue:
                    record = queue.popleft()
                    self._offsets[topic] = record.offset + 1
                    yielded = True
                    yield record
            if not yielded:
                return

    async def commit(self) -> None:
        # In-memory consumer tracks offsets but does not require a real commit.
        return None


class InMemoryProducer:
    """Producer that collects emitted messages in a list keyed by topic."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str | None, dict[str, Any]]] = []
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def send(self, topic: str, payload: dict[str, Any], key: str | None = None) -> None:
        self.messages.append((topic, key, payload))

    def emitted(self, topic: str) -> list[dict[str, Any]]:
        return [p for (t, _, p) in self.messages if t == topic]


class AioKafkaConsumer:
    """Real Kafka consumer backed by ``aiokafka``.

    ``aiokafka`` is an optional dependency: it is imported lazily so unit
    tests do not need it installed.
    """

    def __init__(
        self,
        topics: Iterable[str],
        brokers: str,
        group_id: str,
        concurrency: int = 1,
    ) -> None:
        self.topics = list(topics)
        self.brokers = brokers
        self.group_id = group_id
        self.concurrency = concurrency
        self._consumer: Any = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            enable_auto_commit=False,
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def consume(self) -> AsyncIterator[ConsumerRecord]:
        assert self._consumer is not None, "consumer not started"
        async for msg in self._consumer:
            yield ConsumerRecord(
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
                key=msg.key.decode() if msg.key else None,
                value=_safe_load(msg.value),
            )

    async def commit(self) -> None:
        if self._consumer is not None:
            await self._consumer.commit()


class AioKafkaProducer:
    """Real Kafka producer backed by ``aiokafka``."""

    def __init__(self, brokers: str) -> None:
        self.brokers = brokers
        self._producer: Any = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=self.brokers)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def send(self, topic: str, payload: dict[str, Any], key: str | None = None) -> None:
        assert self._producer is not None, "producer not started"
        value = json.dumps(payload, default=str).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None
        await self._producer.send_and_wait(topic, value=value, key=key_bytes)


def _safe_load(raw: Any) -> Any:
    if isinstance(raw, bytes):
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw
    return raw


def build_consumer(settings: Any, topics: Sequence[str]) -> Consumer:
    """Factory: real aiokafka consumer when ``KAFKA_BROKERS`` is configured,
    otherwise an in-memory consumer."""
    if getattr(settings, "kafka_brokers", "") and getattr(settings, "enable_kafka", False):
        return AioKafkaConsumer(
            topics,
            brokers=settings.kafka_brokers,
            group_id="reconciliation",
            concurrency=settings.consumer_concurrency,
        )
    return InMemoryConsumer(topics)


def build_producer(settings: Any) -> Producer:
    """Factory: real aiokafka producer when ``KAFKA_BROKERS`` is configured,
    otherwise an in-memory producer."""
    if getattr(settings, "kafka_brokers", "") and getattr(settings, "enable_kafka", False):
        return AioKafkaProducer(brokers=settings.kafka_brokers)
    return InMemoryProducer()
