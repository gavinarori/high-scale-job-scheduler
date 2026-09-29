# Job Scheduler — Phase 8

Distributed job scheduler for high-scale systems. **Phase 8**: a seed
script for demo/manual-testing data, and an end-to-end load driver that
exercises the whole running pipeline over HTTP (distinct from `test/load`'s
Go benchmarks, which measure individual layers in isolation). See
`job-scheduler-design.md` for the full architecture and phase plan.

## What's implemented

- **API service** (`cmd/api`): create, get, list (with `limit`) jobs. Cron
  jobs compute and validate their first occurrence at creation time.
- **Scheduler** (`cmd/scheduler`): multi-instance safe via etcd leader
  election. Handles one-off and recurring (cron) jobs.
- **Executor** (`cmd/executor`): Kafka consumer group per job type, scales
  on consumer lag. Four handlers registered: `noop`, `log-message`,
  `flaky` (configurable failure rate — exists specifically so retry/DLQ
  gets exercised by real runs, not just unit tests), and `slow`
  (configurable sleep, for load-testing realistic handler latency).
- **Reaper** (`cmd/reaper`): sweeps stuck `queued` and `claimed`/`running`
  jobs.
- **DLQ**: exhausted-retry instances and unparseable cron templates both
  dead-letter, with a Kafka message for the former.
- **Observability** (`internal/observability`): every binary exposes
  `/metrics` (Prometheus format) and `/healthz` on port 9100 (override with
  `METRICS_PORT`), served on a listener separate from job/API traffic.
- **Seed script** (`cmd/seed`) and **load driver** (`cmd/loadtest`) — see
  their own sections below.

## Metrics exposed

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `scheduler_tick_duration_seconds` | histogram | — | Is the claim+dispatch loop keeping up with the tick interval |
| `scheduler_jobs_claimed_total` | counter | — | Throughput of jobs entering dispatch |
| `scheduler_dispatch_errors_total` | counter | — | Kafka publish failures (each one relies on the reaper's queued-sweep to recover) |
| `scheduler_cron_instances_spawned_total` | counter | — | Recurring job firing rate |
| `scheduler_cron_invalid_total` | counter | — | Recurring templates that broke and got dead-lettered |
| `executor_jobs_processed_total` | counter | `job_type`, `result` | Success/failure rate per job type |
| `executor_job_duration_seconds` | histogram | `job_type` | Handler execution time per job type — the input KEDA's lag-based scaling should eventually be tuned against |
| `reaper_recovered_jobs_total` | counter | `sweep` (`queued`\|`execution`) | How often jobs are getting stuck — a rising rate here means something upstream (Kafka, an executor) is unhealthy |

Note: Kafka **consumer lag** (what the k8s `ScaledObject` in
`executor-deployment.yaml` actually scales on) comes from KEDA's own Kafka
scaler querying the broker directly — it's not one of the metrics above,
since the executor process itself doesn't need to export lag it can't see
outside its own consumer group's viewpoint.

## Run locally

Two ways to run this, depending on whether you want a fully local stack or
to point at a real MongoDB (e.g. Atlas):

**Option A — everything local (Docker):**

```bash
docker compose -f deployments/docker/docker-compose.yml up --build
```

Starts etcd, Kafka, Mongo, the API, two scheduler instances, one executor
per job type, the reaper, **Prometheus** (`localhost:9090`), and
**Grafana** (`localhost:3000`, anonymous admin access for local dev). Add
Prometheus as a Grafana data source (`http://prometheus:9090`) to start
building dashboards — none are pre-built yet, see "What's next."

**Option B — Mongo hosted (Atlas), everything else local:**

```bash
cp .env.example .env   # fill in your real MONGO_URI
go run ./cmd/api
```

`config.Load()` auto-loads `.env` from the working directory if present
(via `godotenv`) — real deployments (docker-compose, k8s) set env vars
directly and never touch `.env` at all. `.env` is gitignored; never commit
real credentials to `.env.example` or anywhere else in the repo. Note:
drop any `?replicaSet=...` query param when pointing at Atlas — the
`mongodb+srv://` scheme already resolves to the cluster's own replica set.
You'd still need Kafka and etcd running somewhere (locally via
`docker compose up kafka etcd`, or point `KAFKA_BROKERS`/`ETCD_ENDPOINTS`
at hosted equivalents) to run `cmd/scheduler` and `cmd/executor` this way —
`cmd/api` alone only needs Mongo.

## Recurring jobs

Create one by passing `cron` instead of `scheduledAt`:

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "nightly-report",
    "tenantId": "tenant-a",
    "jobType": "log-message",
    "payload": {"message": "nightly report run"},
    "cron": "0 2 * * *",
    "maxAttempts": 3
  }'
```

Standard 5-field cron (minute hour dom month dow), no seconds field. Each
firing creates a new job document with idempotency key
`<template-key>-run-<timestamp>`; the template itself never dispatches —
only its spawned instances do.

## Try it

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "test-job-1",
    "tenantId": "tenant-a",
    "jobType": "log-message",
    "payload": {"message": "hello from the scheduler"},
    "priority": 5,
    "scheduledAt": "2026-09-21T00:00:00Z",
    "maxAttempts": 3
  }'
```

Within one scheduler tick (default 500ms) the job will be claimed, published
to `jobs.dispatch.log-message`, picked up by `executor-log-message`, run
through `logMessageHandler`, and marked `completed`. Check status:

```bash
curl "http://localhost:8080/jobs?tenantId=tenant-a"
```

## Seeding demo data

`cmd/seed` populates the database directly (bypassing the API/Kafka — this
is a setup step, not a load test) with a realistic mix: one-off jobs
(some immediate, some scheduled minutes/hours out), a configurable
fraction using the `flaky` handler (so retries and the DLQ actually have
entries to look at), some `slow` jobs, and a few recurring cron templates
across multiple tenants.

```bash
go run ./cmd/seed                                    # defaults: 200 jobs, 3 tenants
go run ./cmd/seed -count 1000 -tenants 5 -flaky-rate 0.3
# or, against the docker-compose stack:
docker compose -f deployments/docker/docker-compose.yml run --rm seed -count 500
```

Run this right after bringing the stack up and watch Grafana/the executor
logs — it's the fastest way to see every code path (retry, backoff, DLQ,
cron firing) exercised without hand-writing curl commands for each one.

## Load testing the running system

`cmd/loadtest` is different from `test/load`'s Go benchmarks — it drives
load against a **running stack** over HTTP and measures real end-to-end
latency through the whole pipeline (API → Mongo → scheduler → Kafka →
executor), not just one layer in isolation:

```bash
go run ./cmd/loadtest -rate 20 -duration 30s
go run ./cmd/loadtest -rate 100 -duration 1m -jobtype slow -maxwait 5m
```

It sends jobs at a target rate for a fixed duration (reporting HTTP ack
latency), then polls each created job by ID until it reaches `completed`
or `dlq`, reporting end-to-end latency percentiles (p50/p95/p99) and how
many jobs didn't finish within `-maxwait`. Needs the full stack running,
not just Mongo — `docker compose up` first.

## Adding a new job type

1. Write a handler in `internal/executor/handlers/handlers.go`:
   ```go
   func myJobHandler(ctx context.Context, payload map[string]any) error {
       // ...
   }
   ```
2. Register it in `RegisterAll`: `r.Register("my-job-type", myJobHandler)`.

## Testing

`test/integration/` runs against a **real** Mongo replica set spun up
automatically via `testcontainers-go` — not mocks. Requires Docker
(testcontainers manages the container lifecycle; nothing to start
manually):

```bash
go test ./test/integration/... -v
```

Covered:
- **Idempotency**: duplicate keys rejected, not silently double-created.
- **Claiming**: no double-claim under concurrency (10 goroutines racing a
  50-job pool), future jobs ignored, priority ordering respected.
- **Retry/DLQ**: attempts increment with backoff until `maxAttempts`, then
  routes to `dlq`; DLQ'd jobs are never reclaimed.
- **Reaper**: stuck `running`/`claimed`/`queued` jobs past timeout are
  recovered — and, just as importantly, jobs claimed *moments* ago are
  left alone.
- **Cron**: next-occurrence parsing (valid and invalid expressions),
  spawned instances carry no `cron` field and get their own idempotency
  key, template rescheduling moves it to `pending` at the right time, and
  a queued template can't be double-claimed within one tick (which is what
  prevents a sub-minute tick interval from firing the same minute twice).

## Load testing

`test/load/` benchmarks claim throughput (with a correctness assertion,
not just a number) and API job-creation throughput at increasing
concurrency. See `test/load/README.md` for how to run and interpret them.

```bash
go test ./test/load/... -bench . -benchtime=1x -run ^$ -v
```

## What's next (Phase 9+)

See `job-scheduler-design.md` section 7. Remaining: pre-built Grafana
dashboards (currently just the raw Prometheus data source — no dashboard
JSON committed yet), an etcd failover test with the same rigor as the
claim-concurrency benchmark, and sharding/multi-tenant isolation once
volume demands it. End-to-end dispatch throughput is now covered by
`cmd/loadtest`.
