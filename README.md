# Reconciliation

![CI](https://github.com/ai-crypto-onramp/reconciliation/actions/workflows/ci.yml/badge.svg)

Continuously matches the internal ledger against bank, exchange, on-chain, and custody state; detects, classifies, and escalates breaks (a top-4 failure mode for crypto on-ramps).

## Overview / Responsibilities

The Reconciliation service is the financial control plane of the on-ramp. It ingests async snapshots and events from four upstream sources — the Ledger, Exchange Connectors, Rail Connectors, and the Blockchain Gateway — and continuously verifies that the platform's internal view of funds agrees with the external world. Where it does not, the service raises a **break**, classifies it (timing vs. real), ages it, and drives it to resolution or escalation.

Core responsibilities:

- Continuously match internal ledger state against external state across **four sources**: fiat rails, exchanges, on-chain, and custody.
- Detect breaks of multiple shapes: **amount mismatches, timing delays, missing entries, and duplicates**.
- Classify breaks as **timing** (expected to self-resolve within a tolerance window) or **real** (genuine discrepancies requiring investigation).
- Track **break aging** and **escalate** stale breaks via webhook and notification.
- **Auto-resolve** timing breaks once the delayed external confirmation arrives.
- Produce **break reporting + alerts** to operators and the audit event log.
- Run **intraday continuous** recon and a **daily EOD** recon cycle.
- Operate across **multi-currency / multi-asset** books.

## Language & Tech Stack

- **Python** — primary implementation language.
- **Pandas / Polars** — batch reconciliation and dataframe joins for EOD runs.
- **Streaming joins** — incremental match engine for intraday continuous recon.
- **Celery / Prefect** — scheduled and on-demand recon job orchestration.
- **PostgreSQL** — durable store for external events, recon runs, breaks, and rules.
- **Kafka** — async event ingestion from upstream services.

## System Requirements

| Requirement | Description |
|---|---|
| Continuous matching | Match internal ledger vs. external state across 4 sources: bank/rails, exchanges, on-chain, custody. |
| Break detection | Identify amount mismatches, timing gaps, missing entries, and duplicates. |
| Break classification | Classify each break as *timing* (expected to self-resolve) vs. *real* (genuine discrepancy). |
| Break aging & escalation | Track time-since-detection; escalate breaks that exceed configurable aging thresholds. |
| Auto-resolution | Automatically close timing breaks when the delayed external confirmation arrives. |
| Break reporting + alerts | Surface breaks via REST API and push alerts to Notification + Audit Event Log. |
| EOD + intraday | Run a daily end-of-day recon cycle and a continuous intraday recon loop. |
| Multi-currency / multi-asset | Reconcile across fiat currencies and crypto assets on a per-asset basis. |

## Non-Functional Requirements

- **Break detection latency:** < 5 minutes from the originating external event.
- **Idempotent ingestion:** Re-delivered events from the bus must not create duplicate breaks or double-count balances.
- **Scalability:** Must scale to high transaction volume via horizontal workers and partitioned event consumption.
- **No false negatives:** Every real discrepancy must surface as a break; missed breaks are treated as severity-1 incidents.
- **At-least-once + idempotent:** Event consumers are at-least-once; the match engine is idempotent on event ID.
- **Auditable:** All break state transitions are append-only logged for compliance forensics.

## Technical Specifications

### API Surface

The service exposes a **REST API** for operators and an **async event consumer** for upstream ingestion.

- **REST:** synchronous query and mutation endpoints (see below).
- **Async consumer:** subscribes to Kafka topics published by ledger-accounting, exchange-connectors, rail-connectors, and blockchain-gateway.

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/breaks?source=&status=&from=&to=` | List breaks filtered by source, status, and time range. |
| `GET` | `/v1/breaks/:id` | Fetch a single break by ID, including full history. |
| `POST` | `/v1/breaks/:id/resolve` | Mark a break as resolved with a resolution record. |
| `POST` | `/v1/breaks/:id/escalate` | Force-escalate a break to the escalation webhook + notification. |
| `GET` | `/v1/recon-runs/:id` | Fetch the status and summary of a reconciliation run. |
| `POST` | `/v1/recon-runs` | Trigger a recon run. Body: `{ "source": "...", "scope": "..." }`. |

### Data Model

| Table | Purpose |
|---|---|
| `external_events` | Idempotent ingest of events/snapshots from rails, exchanges, on-chain, custody. Keyed by source + external event ID. |
| `recon_runs` | One row per reconciliation cycle (intraday or EOD) with source, scope, status, counts, and timestamps. |
| `breaks` | Detected discrepancies: type, classification, source, amounts, status, aging, and links to the run that produced them. |
| `break_resolutions` | Append-only resolution records (manual or auto) keyed to `breaks.id`. |
| `recon_rules` | Configurable match strategies, tolerances, and escalation thresholds per source/asset. |

### Recon Sources

| Source | External state matched |
|---|---|
| **Rails** | Settlement confirmations, chargebacks, refunds, bank balance snapshots. |
| **Exchanges** | Order fills, deposit/withdrawal confirmations, exchange-held balances. |
| **On-chain** | Transaction confirmations, gas paid, and per-address wallet balances. |
| **Custody** | Custodian-held wallet balances (Fireblocks/Dfns/Turnkey) per asset. |

### Match Strategies

- **Exact match** — amount, reference, and counterparty align precisely (e.g., ledger posting vs. bank settlement).
- **Fuzzy match with tolerance window** — used for timing breaks: amount matches but the external confirmation arrives within a configurable tolerance window (`BREAK_TOLERANCE_SECONDS`).
- **Balance roll-forward** — reconciles opening balance + net flow against closing balance per asset/source over the recon window; surfaces unexplained deltas as breaks.

### Integrations

**Consumes (async, Kafka):**

- `ledger-accounting` — internal ledger postings and balance snapshots.
- `exchange-connectors` — fills, deposits/withdrawals, and exchange balance snapshots.
- `rail-connectors` — settlement, chargeback, and bank balance events.
- `blockchain-gateway` — on-chain confirmations and wallet balance snapshots.

**Emits (async):**

- `break-alert` → **Notification** service (email/SMS/webhook to operators).
- `break-event` → **Audit Event Log** (append-only compliance trail).

## Dependencies

| Dependency | Purpose |
|---|---|
| **PostgreSQL** | Durable store for external events, recon runs, breaks, resolutions, and rules. |
| **Kafka** (event bus) | Async ingestion of upstream events and emission of break alerts. |
| **Object storage** (S3/GCS) | Archive of EOD recon reports and break exports. |
| **Notification** service | Delivery of break alerts to operators and partner webhooks. |
| **Audit Event Log** service | Append-only compliance trail of all break state transitions. |

## Configuration

Environment variables:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP port for the REST API. |
| `DB_URL` | — | PostgreSQL connection string. |
| `KAFKA_BROKERS` | — | Comma-separated Kafka bootstrap brokers. |
| `REPORTS_BUCKET` | — | Object storage bucket for EOD recon report archives. |
| `BREAK_TOLERANCE_SECONDS` | `300` | Tolerance window for classifying a break as *timing* vs. *real*. |
| `AUTO_RESOLVE_TIMING_BREAKS` | `true` | Whether timing breaks auto-resolve when the delayed confirmation arrives. |
| `ESCALATION_WEBHOOK` | — | Webhook URL invoked when a break is escalated or ages out. |
| `ESCALATION_AGE_MINUTES` | `60` | Age after which an unresolved break is auto-escalated. |
| `EOD_RUN_CRON` | `0 23 * * *` | Cron schedule for the daily end-of-day recon run. |
| `CONSUMER_CONCURRENCY` | `4` | Number of concurrent Kafka consumer workers per source. |
| `LOG_LEVEL` | `info` | Application log level. |

## Local Development

```bash
# Install dependencies
make install          # or: pip install -r requirements.txt

# Run the service locally
make run             # or: python -m recon.server

# Run a one-off recon run
python -m recon.cli run --source rails --scope daily

# Run the test suite
make test            # or: pytest

# Lint and type-check
make lint            # or: ruff check .
make typecheck       # or: mypy recon/
```