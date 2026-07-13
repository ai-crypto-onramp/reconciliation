"""Aging tracker and escalation worker.

The aging worker recomputes ``age_seconds`` for every open break. The
escalation worker promotes breaks older than ``ESCALATION_AGE_MINUTES`` to
``escalated`` status and emits alert/audit events.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ..config import Settings
from ..db.repository import Repository
from ..kafka import Producer
from ..schemas import BreakAlertEvent, BreakAuditEvent
from . import compute_age


async def update_ages(repo: Repository, *, now: datetime | None = None) -> int:
    """Recompute ``age_seconds`` for every open break. Returns count updated."""
    now = now or datetime.now(tz=UTC)
    open_breaks = await repo.list_breaks(status="open")
    count = 0
    for brk in open_breaks:
        new_age = compute_age(brk.detected_at, now)
        if new_age != brk.age_seconds:
            await repo.update_break_status(brk.id, "open", age_seconds=new_age)
            count += 1
    return count


async def escalate_stale_breaks(
    repo: Repository,
    producer: Producer,
    *,
    settings: Settings,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Auto-escalate breaks older than ``ESCALATION_AGE_MINUTES``.

    Emits ``break-alert`` and ``break-event`` for each escalation. Returns the
    list of escalation events for inspection.
    """
    now = now or datetime.now(tz=UTC)
    open_breaks = await repo.list_breaks(status="open")
    threshold_seconds = settings.escalation_age_minutes * 60
    emitted: list[dict[str, Any]] = []
    for brk in open_breaks:
        age = compute_age(brk.detected_at, now)
        if age < threshold_seconds:
            continue
        await repo.update_break_status(brk.id, "escalated", age_seconds=age)
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
            age_seconds=age,
            action="escalated",
            actor="system",
            timestamp=now,
        )
        audit = BreakAuditEvent(
            break_id=brk.id,
            action="escalated",
            actor="system",
            before={"status": "open", "age_seconds": brk.age_seconds},
            after={"status": "escalated", "age_seconds": age},
            timestamp=now,
        )
        await producer.send("break-alert", alert.model_dump(mode="json"), key=str(brk.id))
        await producer.send("break-event", audit.model_dump(mode="json"), key=str(brk.id))
        emitted.append(
            {
                "break_id": brk.id,
                "action": "escalated",
                "actor": "system",
                "before": {"status": "open"},
                "after": {"status": "escalated"},
            }
        )
    return emitted


async def manually_escalate_break(
    repo: Repository,
    producer: Producer,
    *,
    break_id: int,
    actor: str,
    note: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Force-escalate a single break, emitting alert + audit events."""
    now = now or datetime.now(tz=UTC)
    brk = await repo.get_break(break_id)
    if brk is None:
        return None
    before_status = brk.status
    age = compute_age(brk.detected_at, now)
    await repo.update_break_status(break_id, "escalated", age_seconds=age)
    await repo.add_break_resolution(break_id=break_id, type="manual", actor=actor, note=note)
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
        age_seconds=age,
        action="escalated",
        actor=actor,
        timestamp=now,
    )
    audit = BreakAuditEvent(
        break_id=brk.id,
        action="escalated",
        actor=actor,
        before={"status": before_status},
        after={"status": "escalated"},
        timestamp=now,
    )
    await producer.send("break-alert", alert.model_dump(mode="json"), key=str(brk.id))
    await producer.send("break-event", audit.model_dump(mode="json"), key=str(brk.id))
    return {
        "break_id": brk.id,
        "action": "escalated",
        "actor": actor,
        "before": {"status": before_status},
        "after": {"status": "escalated"},
    }
