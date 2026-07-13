"""Stage 2 tests: Kafka consumers and idempotent event ingestion."""

from __future__ import annotations

import pytest

from reconciliation.config import Settings
from reconciliation.kafka import InMemoryConsumer, InMemoryProducer, build_consumer, build_producer


@pytest.mark.asyncio
async def test_in_memory_consumer_yields_enqueued_records():
    consumer = InMemoryConsumer(["ledger-accounting"])
    await consumer.start()
    consumer.enqueue("ledger-accounting", {"external_event_id": "e1", "amount": 100})
    records = []
    async for record in consumer.drain():
        records.append(record)
        break
    await consumer.stop()
    assert len(records) == 1
    assert records[0].value["external_event_id"] == "e1"


@pytest.mark.asyncio
async def test_in_memory_producer_collects_messages():
    producer = InMemoryProducer()
    await producer.start()
    await producer.send("break-alert", {"break_id": 1}, key="1")
    await producer.send("break-event", {"break_id": 1, "action": "detected"}, key="1")
    assert len(producer.emitted("break-alert")) == 1
    assert len(producer.emitted("break-event")) == 1
    await producer.stop()


def test_build_consumer_returns_in_memory_when_no_brokers():
    settings = Settings(kafka_brokers="", enable_kafka=False)
    consumer = build_consumer(settings, ["topic"])
    assert isinstance(consumer, InMemoryConsumer)


def test_build_producer_returns_in_memory_when_no_brokers():
    settings = Settings(kafka_brokers="", enable_kafka=False)
    producer = build_producer(settings)
    assert isinstance(producer, InMemoryProducer)


@pytest.mark.asyncio
async def test_idempotent_ingest_does_not_duplicate(fake_repo):
    from reconciliation.reconciler import Reconciler

    producer = InMemoryProducer()
    settings = Settings(auto_resolve_timing_breaks=False)
    recon = Reconciler(fake_repo, producer, settings)
    payload = {"external_event_id": "e1", "source": "rails", "asset": "USD", "amount": "100", "reference": "ref1"}
    _, created1 = await recon.ingest(source="rails", payload=payload)
    _, created2 = await recon.ingest(source="rails", payload=payload)
    assert created1 is True
    assert created2 is False
    events = await fake_repo.list_external_events(source="rails")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_consume_once_dead_letters_poison_messages(fake_repo, monkeypatch):
    from reconciliation.reconciler import Reconciler

    producer = InMemoryProducer()
    settings = Settings(auto_resolve_timing_breaks=False)
    recon = Reconciler(fake_repo, producer, settings)

    async def boom(*args, **kwargs):
        raise ValueError("poison")

    monkeypatch.setattr(recon, "ingest", boom)
    consumer = InMemoryConsumer(["rail-connectors"])
    consumer.enqueue("rail-connectors", {"external_event_id": "e1", "source": "rails"})
    await recon.consume_once(consumer)
    dlq = producer.emitted("recon-dlq")
    assert len(dlq) == 1
    assert dlq[0]["error"] == "poison"
