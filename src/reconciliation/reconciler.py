"""Reconciler: orchestrates ingestion, matching, break detection, and runs.

The :class:`Reconciler` is the service-layer façade used by the REST API, the
CLI, and the Kafka consumer. It accepts a :class:`Repository` and a
:class:`Producer` (both can be in-memory fakes in tests) and a
:class:`Settings` instance.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Any

from .breaks import detect_and_persist_breaks
from .breaks.aging import (
    escalate_stale_breaks,
    manually_escalate_break,
    update_ages,
)
from .breaks.auto_resolve import attempt_auto_resolve
from .config import AUDIT_TOPIC, CONSUMER_TOPICS, Settings, audit_envelope, get_settings
from .db.repository import Repository, SqlRepository
from .db.session import async_engine_factory, async_session_factory, init_db
from .kafka import (
    Consumer,
    Producer,
    build_producer,
)
from .ledger_fetcher import LedgerFetcher, build_ledger_fetcher
from .matching import ExternalEntry, LedgerEntry, get_strategy
from .schemas import BreakAlertEvent, BreakAuditEvent

logger = logging.getLogger(__name__)


class Reconciler:
    """Coordinates ingestion, matching, break detection, auto-resolution, and runs."""

    def __init__(
        self,
        repo: Repository,
        producer: Producer,
        settings: Settings,
        *,
        engine: Any | None = None,
        ledger_fetcher: LedgerFetcher | None = None,
    ) -> None:
        self.repo = repo
        self.producer = producer
        self.settings = settings
        self.engine = engine
        self.ledger_fetcher = ledger_fetcher

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> Reconciler:
        """Build a reconciler using the default (real or in-memory) backends."""
        settings = settings or get_settings()
        producer = build_producer(settings)
        return cls(
            repo=_LazyRepo(settings),
            producer=producer,
            settings=settings,
            ledger_fetcher=build_ledger_fetcher(settings),
        )

    async def ingest(self, source: str, payload: dict[str, Any]) -> tuple[Any, bool]:
        """Idempotently ingest an external event and (maybe) auto-resolve."""
        event, created = await self.repo.upsert_external_event(
            source=source,
            external_event_id=payload.get("external_event_id", ""),
            payload=payload,
        )
        if not created:
            return event, False
        # Attempt auto-resolution of open timing breaks for this source/asset.
        asset = payload.get("asset") or ""
        reference = payload.get("reference")
        amount = payload.get("amount")
        ts = payload.get("timestamp")
        if isinstance(amount, (int, float, str, Decimal)):
            amount = Decimal(str(amount))
        else:
            amount = None
        if isinstance(ts, str):
            try:
                from datetime import datetime as _dt

                ts = _dt.fromisoformat(ts)
            except ValueError:
                ts = None
        resolutions = await attempt_auto_resolve(
            self.repo,
            source=source,
            asset=asset,
            reference=reference,
            external_amount=amount,
            external_timestamp=ts,
            tolerance_seconds=self.settings.break_tolerance_seconds,
            auto_resolve_enabled=self.settings.auto_resolve_timing_breaks,
        )
        for res in resolutions:
            await self._emit_audit(res)
        return event, True

    async def execute(
        self,
        *,
        source: str,
        scope: str,
        mode: str = "eod",
        ledger_entries: list[LedgerEntry] | None = None,
        external_entries: list[ExternalEntry] | None = None,
    ) -> Any:
        """Run a recon cycle: create a run, match, detect breaks, complete."""
        run = await self.repo.create_recon_run(source=source, scope=scope)
        if ledger_entries is not None:
            ledger = ledger_entries
        elif self.ledger_fetcher is not None:
            since = (
                run.started_at - timedelta(seconds=self.settings.break_tolerance_seconds)
                if run.started_at
                else None
            )
            try:
                ledger = await self.ledger_fetcher.fetch_all(since=since)
            except Exception as e:  # noqa: BLE001 - degrade gracefully
                logger.warning(
                    "ledger fetch failed; running recon without ledger data: %s", e
                )
                ledger = []
        else:
            logger.warning(
                "recon run %s for source=%s started without a ledger fetcher; "
                "every external entry will be flagged MISSING_ENTRY",
                run.id,
                source,
            )
            ledger = []
        external = external_entries
        if external is None:
            external = await self._external_entries_for(source)
        strategy_name = await self._strategy_for(source)
        strategy = get_strategy(strategy_name)
        result = strategy.match(
            ledger, external, tolerance_seconds=self.settings.break_tolerance_seconds
        )
        created = await detect_and_persist_breaks(
            self.repo,
            result,
            run_id=run.id,
            source=source,
            tolerance_seconds=self.settings.break_tolerance_seconds,
        )
        for brk_info in created:
            await self._emit_detected(brk_info, run.id)
        matched = len(result.matched) + sum(1 for b in result.balances if b.matched)
        unmatched = len(result.unmatched_ledger) + len(result.unmatched_external)
        breaks = len(created)
        completed_run = await self.repo.complete_recon_run(
            run.id, matched=matched, unmatched=unmatched, breaks=breaks
        )
        await self.repo.commit()
        return completed_run if completed_run is not None else run

    async def get_run(self, run_id: uuid.UUID) -> Any | None:
        return await self.repo.get_recon_run(run_id)

    async def list_recon_runs(self, source: str | None = None) -> Sequence[Any]:
        return await self.repo.list_recon_runs(source)

    async def list_breaks(self, **filters: Any) -> Sequence[Any]:
        return await self.repo.list_breaks(**filters)

    async def get_break(self, break_id: uuid.UUID) -> Any | None:
        return await self.repo.get_break(break_id)

    async def resolve_break(
        self, break_id: uuid.UUID, *, actor: str, note: str | None = None
    ) -> Any | None:
        brk = await self.repo.get_break(break_id)
        if brk is None:
            return None
        before = {"status": brk.status}
        await self.repo.update_break_status(break_id, "RESOLVED")
        resolution = await self.repo.add_break_resolution(
            break_id, type="MANUAL", actor=actor, note=note
        )
        await self.repo.commit()
        audit = {
            "break_id": break_id,
            "action": "manually-resolved",
            "actor": actor,
            "before": before,
            "after": {"status": "RESOLVED"},
        }
        await self._emit_audit(audit)
        return resolution

    async def escalate_break(
        self, break_id: uuid.UUID, *, actor: str, note: str | None = None
    ) -> dict[str, Any] | None:
        return await manually_escalate_break(
            self.repo, self.producer, break_id=break_id, actor=actor, note=note
        )

    async def age_and_escalate(self) -> list[dict[str, Any]]:
        """Update ages on open breaks and escalate stale ones."""
        await update_ages(self.repo)
        emitted = await escalate_stale_breaks(self.repo, self.producer, settings=self.settings)
        for ev in emitted:
            await self._emit_audit(ev)
        await self.repo.commit()
        return emitted

    async def consume_once(self, consumer: Consumer) -> int:
        """Consume a single batch of records from ``consumer`` and ingest them.

        Returns the number of records processed. Intended for tests; in
        production the consumer loop runs indefinitely. Poison messages that
        fail to parse or ingest are sent to the ``recon-dlq`` topic.
        """
        count = 0
        # In-memory consumers expose ``drain`` which terminates once the queue
        # is empty; real aiokafka consumers use the infinite ``consume`` loop.
        drain = getattr(consumer, "drain", None)
        iterator = drain() if drain is not None else consumer.consume()
        async for record in iterator:
            source = self._source_for_topic(record.topic)
            if source is None:
                continue
            payload = record.value if isinstance(record.value, dict) else {}
            try:
                await self.ingest(source=source, payload=payload)
            except Exception as e:  # noqa: BLE001 - poison message
                logger.warning("poison message on topic %s: %s", record.topic, e)
                await self.producer.send(
                    "recon-dlq",
                    {
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "key": record.key,
                        "value": payload,
                        "error": str(e),
                    },
                    key=record.key,
                )
            count += 1
            if count >= 100:  # safety cap for test loops
                break
        return count

    async def list_rules(self, source: str | None = None) -> Sequence[Any]:
        return await self.repo.list_rules(source)

    async def upsert_rule(self, **fields: Any) -> Any:
        rule = await self.repo.upsert_rule(**fields)
        await self.repo.commit()
        return rule

    async def _external_entries_for(self, source: str) -> list[ExternalEntry]:
        events = await self.repo.list_external_events(source=source, limit=10_000)
        entries: list[ExternalEntry] = []
        for ev in events:
            payload = ev.payload or {}
            raw_amount = payload.get("amount")
            amount = (
                Decimal(str(raw_amount))
                if isinstance(raw_amount, (int, float, str, Decimal))
                else None
            )
            entries.append(
                ExternalEntry(
                    external_event_id=ev.external_event_id,
                    source=ev.source,
                    asset=payload.get("asset") or "",
                    reference=payload.get("reference"),
                    amount=amount,
                    counterparty=payload.get("counterparty"),
                    timestamp=payload.get("timestamp"),
                )
            )
        return entries

    async def _strategy_for(self, source: str) -> str:
        rules = await self.repo.get_rules(source)
        if rules:
            return rules[0].match_strategy
        return "EXACT"

    def _source_for_topic(self, topic: str) -> str | None:
        for src, t in CONSUMER_TOPICS.items():
            if t == topic:
                return src
        return None

    async def _emit_detected(self, brk_info: dict[str, Any], run_id: uuid.UUID) -> None:
        brk = await self.repo.get_break(brk_info["id"])
        if brk is None:
            return
        alert = BreakAlertEvent(
            break_id=brk.id,
            type=brk.type,
            classification=brk.classification,
            source=brk.source,
            asset=brk.asset,
            reference=brk.reference,
            internal_amount=brk.internal_amount,
            external_amount=brk.external_amount,
            detected_at=brk.detected_at,
            age_seconds=0,
            action="detected",
        )
        audit = BreakAuditEvent(
            break_id=brk.id,
            action="detected",
            actor="system",
            after={"run_id": run_id, "classification": brk.classification},
        )
        await self.producer.send("break-alert", alert.model_dump(mode="json"), key=str(brk.id))
        await self.producer.send(
            AUDIT_TOPIC,
            audit_envelope(audit.model_dump(mode="json"), brk.id),
            key=str(brk.id),
        )

    async def _emit_audit(self, event: dict[str, Any]) -> None:
        audit = BreakAuditEvent(
            break_id=event["break_id"],
            action=event["action"],
            actor=event.get("actor", "system"),
            before=event.get("before", {}),
            after=event.get("after", {}),
        )
        await self.producer.send(
            AUDIT_TOPIC,
            audit_envelope(audit.model_dump(mode="json"), event["break_id"]),
            key=str(event["break_id"])
        )


class _LazyRepo:
    """Placeholder repository that lazily builds a SqlRepository on first use.

    The real engine is created on the first async call so that ``from_settings``
    does not require a running event loop at import time.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._inner: Repository | None = None
        self._engine: Any | None = None
        self._session_factory: Any | None = None

    async def _ensure(self) -> Repository:
        if self._inner is None:
            self._engine = await init_db(async_engine_factory(self._settings.db_url))
            self._session_factory = async_session_factory(self._engine)
            session = self._session_factory()
            self._inner = SqlRepository(session)
        return self._inner

    async def upsert_external_event(
        self, source: str, external_event_id: str, payload: dict[str, Any]
    ) -> tuple[Any, bool]:
        repo = await self._ensure()
        return await repo.upsert_external_event(source, external_event_id, payload)

    async def list_external_events(
        self, source: str | None = None, limit: int = 1000
    ) -> Sequence[Any]:
        repo = await self._ensure()
        return await repo.list_external_events(source, limit)

    async def create_recon_run(self, source: str, scope: str) -> Any:
        repo = await self._ensure()
        return await repo.create_recon_run(source, scope)

    async def get_recon_run(self, run_id: uuid.UUID) -> Any | None:
        repo = await self._ensure()
        return await repo.get_recon_run(run_id)

    async def complete_recon_run(self, run_id: uuid.UUID, **kwargs: Any) -> Any | None:
        repo = await self._ensure()
        return await repo.complete_recon_run(run_id, **kwargs)

    async def list_recon_runs(self, source: str | None = None) -> Sequence[Any]:
        repo = await self._ensure()
        return await repo.list_recon_runs(source)

    async def create_break(self, **fields: Any) -> Any:
        repo = await self._ensure()
        return await repo.create_break(**fields)

    async def get_break(self, break_id: uuid.UUID) -> Any | None:
        repo = await self._ensure()
        return await repo.get_break(break_id)

    async def list_breaks(self, **filters: Any) -> Sequence[Any]:
        repo = await self._ensure()
        return await repo.list_breaks(**filters)

    async def update_break_status(
        self, break_id: uuid.UUID, status: str, age_seconds: int | None = None
    ) -> Any | None:
        repo = await self._ensure()
        return await repo.update_break_status(break_id, status, age_seconds)

    async def add_break_resolution(
        self, break_id: uuid.UUID, type: str, actor: str, note: str | None = None
    ) -> Any | None:
        repo = await self._ensure()
        return await repo.add_break_resolution(break_id, type, actor, note)

    async def open_timing_breaks_for(
        self, source: str, asset: str, reference: str | None
    ) -> Sequence[Any]:
        repo = await self._ensure()
        return await repo.open_timing_breaks_for(source, asset, reference)

    async def get_rules(self, source: str, asset: str | None = None) -> Sequence[Any]:
        repo = await self._ensure()
        return await repo.get_rules(source, asset)

    async def list_rules(self, source: str | None = None) -> Sequence[Any]:
        repo = await self._ensure()
        return await repo.list_rules(source)

    async def upsert_rule(self, **fields: Any) -> Any:
        repo = await self._ensure()
        return await repo.upsert_rule(**fields)

    async def commit(self) -> None:
        repo = await self._ensure()
        await repo.commit()
