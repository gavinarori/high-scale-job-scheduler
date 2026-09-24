# Job Scheduler — Phase 5

Distributed job scheduler for high-scale systems. **Phase 5**: Prometheus
metrics across all four binaries, plus a local Prometheus + Grafana stack
for actually looking at them. See `job-scheduler-design.md` for the full
architecture and phase plan.

## What's implemented

- **API service** (`cmd/api`): create, get, list jobs. Cron jobs compute
  and validate their first occurrence at creation time.
- **Scheduler** (`cmd/scheduler`): multi-instance safe via etcd leader
  election. Handles one-off and recurring (cron) jobs — see the Phase 4
  README history in git for details on the template/instance split.
- **Executor** (`cmd/executor`): Kafka consumer group per job type, scales
  on consumer lag.
- **Reaper** (`cmd/reaper`): sweeps stuck `queued` and `claimed`/`running`
  jobs.
- **DLQ**: exhausted-retry instances and unparseable cron templates both
  dead-letter, with a Kafka message for the former.
- **Observability** (`internal/observability`): every binary exposes
  `/metrics` (Prometheus format) and `/healthz` on port 9100 (override with
  `METRICS_PORT`), served on a listener separate from job/API traffic so
  scrape load and outages can't cross-affect each other.

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

```bash
docker compose -f deployments/docker/docker-compose.yml up --build
```

Starts etcd, Kafka, Mongo, the API, two scheduler instances, one executor
per job type, the reaper, **Prometheus** (`localhost:9090`), and
**Grafana** (`localhost:3000`, anonymous admin access for local dev). Add
Prometheus as a Grafana data source (`http://prometheus:9090`) to start
building dashboards — none are pre-built yet, see "What's next."

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

## What's next (Phase 6+)

See `job-scheduler-design.md` section 7. Remaining: pre-built Grafana
dashboards (currently just the raw Prometheus data source — no dashboard
JSON committed yet), an etcd failover test with the same rigor as the
claim-concurrency test, a load test for actual dispatch throughput, and
sharding/multi-tenant isolation once volume demands it.
