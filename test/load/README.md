# Load Tests

Go benchmarks (not `go test`) measuring throughput at the two layers most
likely to become the actual bottleneck at scale: the atomic claim query,
and the API's job-creation path. Kept in a separate package from
`test/integration/` deliberately — these are slow and their numbers are
environment-dependent, so they shouldn't run incidentally as part of a
normal `go test ./...`.

Requires Docker (testcontainers spins up Mongo automatically, same as the
integration suite).

## Run everything

```bash
go test ./test/load/... -bench . -benchtime=1x -run ^$ -v
```

`-run ^$` skips any accidental `Test*` functions in this package (there
are none currently, but it's cheap insurance); `-benchtime=1x` runs each
benchmark once rather than Go's default of repeating until statistically
stable — these benchmarks already do their own internal iteration
(claiming/creating thousands of jobs), so the outer `-benchtime` looping
would just repeat the whole scenario redundantly.

## What each one measures

**`BenchmarkClaimDueJobs_SingleClaimer`** — raw claim throughput with zero
contention: one goroutine draining a 5,000-job pool. This is the ceiling
every concurrent scenario is bounded by; if this number itself is low,
the problem is index shape or Mongo instance sizing, not concurrency.

**`BenchmarkClaimDueJobs_Concurrent`** — the one that actually matters for
"how many scheduler instances can I run." Runs the same drain at
concurrency 1, 5, 20, and 50, and — this is the important part — asserts
zero double-claims at every level, not just reports a number. A throughput
figure next to a silent atomicity violation would be worse than no
benchmark at all, so this fails loudly (`b.Fatalf`) if `findOneAndUpdate`'s
guarantee ever breaks down under real contention.

Note this doesn't currently motivate running more than one active
scheduler — Phase 3's etcd election means only one instance dispatches at
a time regardless. This benchmark is here for two reasons: (1) it proves
the claim query itself scales if a future design change allows sharded or
per-tenant multi-leader dispatch, and (2) elevated concurrency here also
approximates what happens organically when a scheduler's batch size is
increased rather than instance count.

**`BenchmarkCreateJob_Concurrent`** — end-to-end HTTP throughput for
`POST /jobs` through the real handler + repo + unique-index path, at
concurrency 1, 10, and 50 (100 requests per worker). The likely ceiling
here is write contention on the `idempotencyKey` unique index, not CPU —
if throughput flattens or errors appear at higher concurrency, that index
(or sharding the `jobs` collection) is where to look first, per the
scaling table in `job-scheduler-design.md` section 6.

## Reading the output

Each benchmark logs a human-readable summary line (jobs/sec or req/sec,
plus context) via `b.Logf` — run with `-v` to see them. `b.ReportMetric`
also surfaces the throughput number in Go's standard benchmark output
format, so these numbers are diffable across runs/commits if you want to
track regressions over time (`benchstat` works against saved output).

## What these don't cover yet

- **Dispatch-to-execution throughput** (scheduler → Kafka → executor →
  Mongo write-back, end to end) isn't benchmarked — that needs a running
  Kafka broker and executor process, not just Mongo, so it's a heavier
  lift than a Go benchmark can spin up on its own. A `docker compose`-based
  load driver (hit the API at N req/sec, watch Grafana) is the more
  practical way to measure that, now that Phase 5 has metrics wired up.
- **Sustained load / soak testing** — these benchmarks run once and stop;
  they don't reveal memory growth, connection pool exhaustion, or Mongo
  index degradation under hours of continuous load.
