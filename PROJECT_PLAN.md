# Project Plan — Reconciliation

This plan decomposes the Reconciliation service into ordered implementation stages, from foundational data model and ingestion through match strategies, break detection, auto-resolution, aging/escalation, batch/intraday runs, notifications/audit, reporting/export, and finally test coverage and containerization.

## Stage 1 — Database Schema & Configuration

**Goal:** Establish the durable PostgreSQL schema and configuration layer that all subsequent stages depend on.

**Tasks:**
- [x] Define `external_events` table: idempotent ingest keyed by `source` + `external_event_id`, with payload JSONB, ingested_at, and dedup index.
- [x] Define `recon_runs` table: one row per cycle (intraday/EOD) with source, scope, status, counts, started_at, completed_at.
- [x] Define `breaks` table: type, classification, source, asset, amounts, status, aging, run_id FK, detected_at.
- [x] Define `break_resolutions` table: append-only resolution records keyed to `breaks.id` with type (manual/auto) and actor.
- [x] Define `recon_rules` table: configurable match strategies, tolerances, and escalation thresholds per source/asset.
- [x] Add Alembic migrations (or equivalent) for all tables with indexes on lookup paths.
- [x] Implement settings/config loader from environment variables (PORT, DB_URL, KAFKA_BROKERS, BREAK_TOLERANCE_SECONDS, etc.).
- [x] Set up project scaffolding: `recon/` package, `server.py`, `cli.py`, `config.py`, `db/` module.

**Acceptance criteria:**
- Migrations apply cleanly to an empty PostgreSQL database.
- All five tables exist with correct columns, FKs, and indexes for idempotent lookup and break filtering.
- Settings load from env vars with documented defaults.

## Stage 2 — Kafka Consumers & Event Ingestion

**Goal:** Build at-least-once, idempotent consumers for the four upstream sources.

**Tasks:**
- [x] Implement Kafka consumer framework with per-source concurrency (`CONSUMER_CONCURRENCY`).
- [x] Add consumer for `ledger-accounting` topic (internal postings + balance snapshots).
- [x] Add consumer for `exchange-connectors` topic (fills, deposits/withdrawals, exchange balances).
- [x] Add consumer for `rail-connectors` topic (settlement, chargeback, bank balances).
- [x] Add consumer for `blockchain-gateway` topic (on-chain confirmations, wallet balances).
- [x] Implement idempotent insert into `external_events` keyed by source + external_event_id (upsert on conflict).
- [x] Add dead-letter handling for poison messages.

**Acceptance criteria:**
- Redelivered events do not create duplicate `external_events` rows.
- All four topics are consumed and persisted with original payload preserved.
- Consumer concurrency is configurable per source.

## Stage 3 — Match Strategies

**Goal:** Implement the three match strategies used by the recon engine.

**Tasks:**
- [x] Implement exact match strategy (amount, reference, counterparty align).
- [x] Implement fuzzy match with tolerance window (`BREAK_TOLERANCE_SECONDS`) for timing breaks.
- [x] Implement balance roll-forward strategy (opening + net flow vs. closing balance per asset/source).
- [x] Define match strategy interface/protocol so `recon_rules` can select strategy per source/asset.
- [x] Unit-test each strategy in isolation with fixtures.

**Acceptance criteria:**
- Exact match returns match/no-match deterministically.
- Fuzzy match classifies timing-tolerant pairs correctly within tolerance.
- Balance roll-forward surfaces unexplained deltas as candidate breaks.

## Stage 4 — Break Detection & Classification

**Goal:** Detect breaks of all shapes and classify each as timing vs. real.

**Tasks:**
- [x] Implement break detection for amount mismatches.
- [x] Implement break detection for timing gaps (ledger posting with no external confirmation within tolerance).
- [x] Implement break detection for missing entries.
- [x] Implement break detection for duplicates.
- [x] Classify each detected break as `timing` (expected to self-resolve) or `real` (genuine discrepancy).
- [x] Persist breaks to `breaks` table with classification, source, amounts, status, detected_at, run_id.
- [x] Enforce no-false-negatives: every unmatched pair produces a break.

**Acceptance criteria:**
- All four break shapes are detected from matched/unmatched pairs.
- Classification follows tolerance rules from `recon_rules`.
- Breaks are queryable via `GET /v1/breaks?source=&status=&from=&to=`.

## Stage 5 — Auto-Resolution of Timing Breaks

**Goal:** Automatically close timing breaks once the delayed external confirmation arrives.

**Tasks:**
- [x] On new external event, attempt re-match against open `timing` breaks for the same source/asset/reference.
- [x] If match succeeds, create `break_resolutions` row with type=`auto` and close the break.
- [x] Gate behavior behind `AUTO_RESOLVE_TIMING_BREAKS` flag.
- [x] Emit audit event for each auto-resolution.

**Acceptance criteria:**
- A timing break auto-closes when the matching external confirmation is ingested.
- Manual overrides (flag disabled) leave timing breaks open.
- All auto-resolutions are recorded append-only in `break_resolutions`.

## Stage 6 — Break Aging, Escalation & Alerts

**Goal:** Track break age and escalate stale breaks; emit operator alerts and audit events.

**Tasks:**
- [x] Implement aging tracker computing time-since-detection for each open break.
- [x] Add escalation worker that auto-escalates breaks older than `ESCALATION_AGE_MINUTES`.
- [x] Implement `POST /v1/breaks/:id/escalate` for manual force-escalation.
- [x] Emit `break-alert` to Notification service (email/SMS/webhook) on escalation.
- [x] Emit `break-event` to Audit Event Log for every state transition (append-only).
- [x] Configure `ESCALATION_WEBHOOK` invocation.

**Acceptance criteria:**
- Breaks exceeding the age threshold are auto-escalated without operator action.
- Both auto and manual escalations trigger Notification + Audit emission.
- Audit trail is append-only and reconstructs full break lifecycle.

## Stage 7 — EOD Batch & Intraday Continuous Runs

**Goal:** Run the daily end-of-day recon cycle and the continuous intraday match loop.

**Tasks:**
- [x] Implement intraday continuous match engine (streaming joins over incoming external events).
- [x] Implement EOD batch recon using Pandas/Polars dataframe joins over the day's scope.
- [ ] Orchestrate runs via Celery/Prefect with `EOD_RUN_CRON` schedule.
- [x] Create `recon_runs` rows with status, counts, started_at, completed_at.
- [x] Expose `POST /v1/recon-runs` to trigger ad-hoc runs and `GET /v1/recon-runs/:id` for status.
- [x] Add CLI command `python -m recon.cli run --source ... --scope ...`.

**Acceptance criteria:**
- EOD run executes on cron and produces a `recon_runs` row with counts.
- Intraday loop continuously matches events with < 5 min detection latency.
- Ad-hoc runs are triggerable via REST and CLI.

## Stage 8 — Notifications & Audit Emission

**Goal:** Wire up outbound emission of break alerts and audit events.

**Tasks:**
- [x] Implement Kafka producer for `break-alert` topic consumed by Notification service.
- [x] Implement Kafka producer for `break-event` topic consumed by Audit Event Log.
- [x] Define event schemas (break id, type, source, classification, amounts, timestamp, actor).
- [x] Emit on every break state transition: detected, classified, auto-resolved, escalated, manually resolved.
- [x] Ensure at-least-once delivery with idempotent consumers downstream in mind.

**Acceptance criteria:**
- Every break state transition produces both a `break-alert` and `break-event`.
- Event payloads conform to the documented schema.
- Notification + Audit services can subscribe and process without loss.

## Stage 9 — Reports & Export

**Goal:** Produce EOD recon reports and break exports for operators and compliance.

**Tasks:**
- [x] Generate EOD recon report per source/asset summarizing run, breaks, resolutions.
- [x] Export breaks list (CSV/JSON) filtered by source/status/time range.
- [x] Archive reports to object storage (`REPORTS_BUCKET`).
- [x] Expose report download endpoint or signed URL flow.
- [x] Add multi-currency / multi-asset grouping in reports.

**Acceptance criteria:**
- EOD report is generated, archived to object storage, and retrievable.
- Break export reflects applied filters and includes full history.
- Reports correctly partition by asset and currency.

## Stage 10 — Tests, Coverage, Docker & CI

**Goal:** Hardening: comprehensive tests, lint/type-check, container image, and CI pipeline.

**Tasks:**
- [x] Add unit tests for match strategies, break detection, classification, auto-resolution, aging, escalation.
- [ ] Add integration tests with Kafka + PostgreSQL via testcontainers.
- [x] Add API tests for all REST endpoints.
- [x] Configure `ruff` lint and `mypy` type-check with `make lint` / `make typecheck`.
- [x] Configure pytest with coverage target and Codecov upload.
- [x] Write `Dockerfile` and `docker-compose.yml` (service + Postgres + Kafka).
- [x] Add GitHub Actions CI workflow running lint, typecheck, tests, coverage.

**Acceptance criteria:**
- `make test`, `make lint`, `make typecheck` all pass locally and in CI.
- Coverage meets project threshold and is reported to Codecov.
- Service runs end-to-end via `docker-compose up`.