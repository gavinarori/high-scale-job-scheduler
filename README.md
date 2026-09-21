# Job Scheduler — Phase 2

Distributed job scheduler for high-scale systems. **Phase 2**: the
scheduler now dispatches via Kafka instead of running jobs in-process, and
execution lives in its own horizontally scalable `executor` binary — one
consumer group per job type. No etcd leader election yet (Phase 3), so
only run a single scheduler instance for now. See `job-scheduler-design.md`
for the full architecture and phase plan.

## What's implemented

- **API service** (`cmd/api`): create, get, list jobs. Enforces idempotency
  keys at the database level (unique index).
- **Scheduler** (`cmd/scheduler`): polls Mongo on a fixed tick, atomically
  claims due jobs via `findOneAndUpdate`, and publishes a small dispatch
  message (job ID + type + priority) to that job type's Kafka topic.
- **Executor** (`cmd/executor`): a Kafka consumer group member for one job
  type (set via `JOB_TYPE` env var). Fetches the full job payload from
  Mongo by ID, runs it against the handler registry, and commits its Kafka
  offset only after the job's result is written back to Mongo. Run one
  deployment per job type — each scales independently on its own topic's
  consumer lag (see `deployments/k8s/executor-deployment.yaml`, KEDA-driven).
- **Reaper** (`cmd/reaper`): two sweeps — one for jobs stuck in
  `claimed`/`running` past an execution timeout (crashed/hung executor),
  one for jobs stuck in `queued` past a much shorter timeout (scheduler
  claimed the job but failed to publish to Kafka). Both reset to `pending`.
- **DLQ**: jobs that exhaust `maxAttempts` get Mongo status `dlq` *and* a
  message on the `jobs.dlq` Kafka topic for external inspection/alerting.
- **Handler registry** (`internal/executor`): register a `func(ctx, payload)
  error` per `jobType`. Two examples in `internal/executor/handlers/`.

## Run locally

```bash
docker compose -f deployments/docker/docker-compose.yml up --build
```

This starts Kafka (KRaft mode, no Zookeeper), a single-node Mongo replica
set (required even solo, since transactions in later phases need it), the
API, scheduler, one executor per registered job type, and the reaper.

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

## What's next (Phase 3+)

See `job-scheduler-design.md` section 7. Next up: etcd leader election so
multiple scheduler instances can run safely (only one dispatches at a
time), which removes the current single-scheduler-instance constraint.

## Testing

`test/integration/` runs the claim/retry/reaper logic against a **real**
Mongo replica set spun up automatically via `testcontainers-go` — not
mocks. This matters specifically for `ClaimDueJobs`: a mock can't
meaningfully prove `findOneAndUpdate`'s atomicity guarantee, so
`claim_test.go` fires 10 concurrent goroutines at a 50-job pool and asserts
every job was claimed exactly once.

Requires Docker (testcontainers manages the Mongo container lifecycle
itself — nothing to start manually):

```bash
go test ./test/integration/... -v
```

Covered:
- **Idempotency**: duplicate keys rejected, not silently double-created.
- **Claiming**: no double-claim under concurrency, future jobs ignored,
  priority ordering respected.
- **Retry/DLQ**: attempts increment with backoff until `maxAttempts`, then
  routes to `dlq` status; DLQ'd jobs are never reclaimed by the scheduler.
- **Reaper**: stuck `running`/`claimed` jobs past timeout are recovered —
  and, just as importantly, jobs claimed *moments* ago are left alone (the
  negative case that actually matters — a reaper that's too eager will
  duplicate work on slow-but-healthy executors).
