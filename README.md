# Job Scheduler — Phase 4

Distributed job scheduler for high-scale systems. **Phase 4**: recurring
(cron) jobs are now actually implemented — Phase 1-3's `cron` field existed
on the model but nothing computed occurrences or fired them. See
`job-scheduler-design.md` for the full architecture and phase plan.

## What's implemented

- **API service** (`cmd/api`): create, get, list jobs. Cron jobs compute
  and validate their first occurrence at creation time — a bad cron
  expression is rejected immediately with a 400, not discovered later by
  the scheduler.
- **Scheduler** (`cmd/scheduler`): multi-instance safe via etcd leader
  election. For one-off jobs, claims and dispatches as before. For
  recurring jobs, a claimed template spawns a concrete run-instance
  (separate document, own idempotency key, no `cron` field — it's a normal
  job from there on) and reschedules the template to its next occurrence.
  The template itself is never dispatched or executed — only its spawned
  instances are, keeping retry/DLQ history scoped per-run rather than
  polluting the recurring definition's own status.
- **Executor** (`cmd/executor`): Kafka consumer group per job type, scales
  on consumer lag.
- **Reaper** (`cmd/reaper`): sweeps stuck `queued` and `claimed`/`running`
  jobs — also covers a cron template that got claimed but never
  successfully rescheduled (e.g. a Mongo write failure mid-reschedule).
- **DLQ**: exhausted-retry job instances get Mongo status `dlq` + a Kafka
  message. A cron template with an unparseable expression is also
  dead-lettered on discovery, so a broken recurring job fails loudly
  instead of silently stalling forever.
- **Handler registry** (`internal/executor`): register a `func(ctx, payload)
  error` per `jobType`.

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

Standard 5-field cron (minute hour dom month dow), no seconds field — for
sub-minute recurrence, use repeated one-off jobs instead. Each firing
creates a new job document with idempotency key
`<template-key>-run-<timestamp>`; query `?tenantId=tenant-a` to see the
run history accumulate alongside the (perpetually `pending`) template.

## Run locally

```bash
docker compose -f deployments/docker/docker-compose.yml up --build
```

Starts etcd, Kafka, a single-node Mongo replica set, the API, two scheduler
instances (leader election — kill the leader to watch failover), one
executor per registered job type, and the reaper.

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

## What's next (Phase 5+)

See `job-scheduler-design.md` section 7. Remaining: Prometheus/Grafana
observability (scheduler tick latency, dispatch lag, executor success
rate, consumer lag — the last of which the k8s executor autoscaler already
assumes exists but nothing exports yet), an etcd failover test with the
same rigor as the claim-concurrency test, a load test for actual dispatch
throughput, and sharding/multi-tenant isolation once volume demands it.
