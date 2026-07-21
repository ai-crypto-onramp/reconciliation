"""Tests for the KafkaLedgerConsumer background ingestion path."""

from __future__ import annotations

import pytest

from reconciliation.config import Settings
from reconciliation.kafka import InMemoryProducer
from reconciliation.kafka_ledger_consumer import (
    KafkaLedgerConsumer,
    build_ledger_consumer,
)
from reconciliation.reconciler import Reconciler


@pytest.mark.asyncio
async def test_process_once_ingests_ledger_event(fake_repo):
    from reconciliation.kafka import InMemoryConsumer

    producer = InMemoryProducer()
    settings = Settings(auto_resolve_timing_breaks=False)
    recon = Reconciler(fake_repo, producer, settings)
    topic = "ledger.events.v1"
    in_consumer = InMemoryConsumer([topic])
    ledger_consumer = KafkaLedgerConsumer(in_consumer, recon, topic=topic)
    in_consumer.enqueue(
        topic,
        {
            "posting_id": "p1",
            "asset": "USD",
            "amount": "100",
            "reference": "ref1",
            "direction": "DEBIT",
        },
    )
    count = await ledger_consumer.process_once()
    assert count == 1
    events = await fake_repo.list_external_events(source="LEDGER")
    assert len(events) == 1
    assert events[0].external_event_id == "p1"


@pytest.mark.asyncio
async def test_process_once_swallows_poison_messages(fake_repo, monkeypatch):
    from reconciliation.kafka import InMemoryConsumer

    producer = InMemoryProducer()
    settings = Settings(auto_resolve_timing_breaks=False)
    recon = Reconciler(fake_repo, producer, settings)

    async def boom(*args, **kwargs):
        raise ValueError("poison")

    monkeypatch.setattr(recon, "ingest", boom)
    topic = "ledger.events.v1"
    in_consumer = InMemoryConsumer([topic])
    ledger_consumer = KafkaLedgerConsumer(in_consumer, recon, topic=topic)
    in_consumer.enqueue(topic, {"posting_id": "p1", "asset": "USD", "amount": "1"})
    count = await ledger_consumer.process_once()
    assert count == 1
    events = await fake_repo.list_external_events(source="LEDGER")
    assert len(events) == 0


@pytest.mark.asyncio
async def test_process_once_synthesises_event_id_from_offset(fake_repo):
    from reconciliation.kafka import InMemoryConsumer

    producer = InMemoryProducer()
    settings = Settings(auto_resolve_timing_breaks=False)
    recon = Reconciler(fake_repo, producer, settings)
    topic = "ledger.events.v1"
    in_consumer = InMemoryConsumer([topic])
    ledger_consumer = KafkaLedgerConsumer(in_consumer, recon, topic=topic)
    in_consumer.enqueue(topic, {"asset": "USD", "amount": "1"})
    count = await ledger_consumer.process_once()
    assert count == 1
    events = await fake_repo.list_external_events(source="LEDGER")
    assert events[0].external_event_id == "ledger.events.v1:0:0"


def test_build_ledger_consumer_returns_none_when_kafka_disabled(fake_repo):
    producer = InMemoryProducer()
    settings = Settings(kafka_brokers="", enable_kafka=False)
    recon = Reconciler(fake_repo, producer, settings)
    assert build_ledger_consumer(recon, settings) is None


def test_build_ledger_consumer_returns_none_when_no_brokers(fake_repo):
    producer = InMemoryProducer()
    settings = Settings(kafka_brokers="", enable_kafka=True)
    recon = Reconciler(fake_repo, producer, settings)
    assert build_ledger_consumer(recon, settings) is None
