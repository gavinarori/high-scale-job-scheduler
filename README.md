# Job Scheduler — Phase 3

Distributed job scheduler for high-scale systems. **Phase 3**: etcd leader
election removes the single-scheduler-instance constraint — multiple
scheduler replicas now run safely, with exactly one dispatching at a time
and automatic failover on crash. See `job-scheduler-design.md` for the
full architecture and phase plan.

## What's implemented

- **API service** (`cmd/api`): create, get, list jobs. Enforces idempotency
  keys at the database level (unique index).
- **Scheduler** (`cmd/scheduler`): now runs as multiple replicas safely.
  Each instance campaigns for leadership via etcd (`internal/scheduler/election.go`);
  only the elected leader actually ticks the dispatch loop and claims/publishes
  jobs. Standbys sit warm, doing nothing, until the leader's etcd session
  dies — crash, network partition, or clean shutdown — at which point one
  of them takes over automatically. No split-brain window: leadership is
  watched via the standby's own session, so a leader that loses contact
  with etcd stops dispatching immediately rather than continuing on stale
  belief.
- **Executor** (`cmd/executor`): a Kafka consumer group member for one job
  type (set via `JOB_TYPE` env var). Scales independently per job type on
  consumer lag (KEDA, see `deployments/k8s/executor-deployment.yaml`).
- **Reaper** (`cmd/reaper`): sweeps stuck `queued` jobs (short timeout —
  scheduler claimed but failed to publish) and stuck `claimed`/`running`
  jobs (longer timeout — executor crashed or hung).
- **DLQ**: exhausted-retry jobs get Mongo status `dlq` plus a message on
  the `jobs.dlq` Kafka topic.
- **Handler registry** (`internal/executor`): register a `func(ctx, payload)
  error` per `jobType`.

## Run locally

```bash
docker compose -f deployments/docker/docker-compose.yml up --build
```

Starts etcd, Kafka, a single-node Mongo replica set, the API, **two**
scheduler instances (`scheduler-a`, `scheduler-b` — watch the logs to see
one become leader and the other sit in `campaigning...`), one executor per
registered job type, and the reaper.

To see failover in action: find which scheduler logged `elected leader`,
then `docker compose kill scheduler-a` (or whichever won) and watch the
other pick up leadership within a few seconds — the `leaseTTL` passed to
`NewLeaderElector` (10s) bounds the worst-case failover time.

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

## What's next (Phase 4+)

See `job-scheduler-design.md` section 7. Retry/backoff and DLQ are already
implemented (Phase 2's `MarkFailed` + DLQ topic) and covered by the
integration tests — remaining phases are observability (Prometheus/Grafana
dashboards for scheduler tick latency, dispatch lag, executor success rate)
and sharding/multi-tenant isolation if volume demands it.

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
