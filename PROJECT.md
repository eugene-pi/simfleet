# simfleet — project handoff

A distributed platform that expands parameter sweeps into thousands of
simulation jobs, distributes them over Kafka, runs them on workers, and scores
the results.

This document captures the idea, the design decisions and their rationale, what
currently works, and what is left. It is written to be self-contained: someone
(or some session) starting from here should not need the conversation that
produced it.

---

## 1. Why this exists

Built as a portfolio project for a Simulation Backend role.
The target gaps it is meant to close, in priority order:

1. **Kafka** — no prior production exposure.
2. **Terraform** — prior IaC experience is Ansible, which is a different
   category (configuring existing machines vs. declaring infrastructure).
3. **Kubernetes / EKS** — Docker experience, but no orchestration.
4. **Batch vs. real-time mindset** — prior work is latency-oriented (voice,
   jitter buffers, sub-second delivery). Simulation infrastructure is
   throughput-oriented: fan out thousands of jobs, retry failures, keep cost
   down, reproduce any run.

The platform is deliberately workload-agnostic: the simulation itself is a few
hundred lines, and the infrastructure around it is the point.

---

## 2. Architecture

```
  simfleet (CLI)          controller                 worker
       |                      |                        |
   submit spec          publishLoop ──> Kafka ──> readLoop
       |                 reconcileLoop             processLoop
       v                      |                        |
   PostgreSQL <───────────────┴────────────────────────┘
   (jobs, results, outbox)                    S3/LocalFS (artifacts)
```

### Components

| Component | Responsibility |
|---|---|
| `cmd/simfleet` | Submits experiments, queries status. Short-lived. |
| `cmd/controller` | Two loops: publishes outbox → Kafka (fast tick), and reconciles state (slow tick). Long-lived, no state in memory. |
| `cmd/worker` | Consumes job ids from Kafka, runs them through the executor. Long-lived, one runner type per process. |
| `cmd/migrate` | Applies schema migrations. Separate on purpose (see D-11). |
| `cmd/runlocal` | Dev-only orchestrator: spawns the real binaries, waits for completion, sends SIGTERM. Models what Kubernetes will do. |

### Storage split

| Store | Holds | If lost |
|---|---|---|
| PostgreSQL | Experiments, jobs, state machine, results, metrics, outbox. **Source of truth.** | Unacceptable |
| S3 / LocalFS | Simulation artifacts (traces) | Unacceptable |
| Kafka | Jobs *in flight*. Transport, not storage. | Controller republishes from Postgres |
| Redis *(not yet built)* | Progress counters | System degrades, stays correct |

**Governing principle:** if everything except Postgres and S3 is wiped, the
system must degrade (progress rebuilds, jobs re-leased late) but never become
incorrect — no lost results, no double-counted results.

---

## 3. Design decisions

These are the decisions worth defending in an interview, with the reasoning
that produced them. Candidates for `docs/adr/`.

### D-1. Metrics are truth, scores are derived

Runners emit **metrics** (`min_distance_m`) and **flags** (`collision`). The
controller derives **score** (0–100) and **zone** (green/yellow/red) from them
using the experiment's scoring spec.

Why: thresholds change constantly, and re-running 10k jobs to apply a new
threshold is unacceptable. Rescoring reads stored metrics and takes seconds.

Mechanism: `experiments.scoring_hash` vs `job_results.scoring_hash`, compared
with `IS DISTINCT FROM` (not `!=` — NULL comparison would silently exclude
never-scored rows). Changing the spec bumps the hash; the next reconcile pass
rescores everything affected.

### D-2. Score combination is non-compensating by default

`combine: min` — the result is no better than its weakest component. A
comfortable stop 20 cm from a pedestrian must not be rescued by a good braking
score. `weighted_mean` exists but is opt-in.

Hard gates (`collision → 0`) are evaluated *before* components and short-circuit
the whole calculation. `Evaluation.GatedBy` records which gate fired, so a zero
score can be distinguished from "metrics were genuinely bad".

### D-3. Fencing tokens, not leases, provide correctness

`jobs.attempt` is a monotonically increasing fencing token. Every state
transition is a **single conditional UPDATE**:

```sql
UPDATE jobs SET state='completed' WHERE id=$1 AND attempt=$2 AND state='running'
```

A worker that stalled past its lease, had its job reassigned, then woke up and
tried to write its result matches zero rows → `ErrFenced`.

Leases and heartbeats are an **efficiency** mechanism (stop doing work nobody
wants, detect dead workers). They are *not* a correctness mechanism: clocks
drift and GC pauses happen, so time-based schemes never guarantee anything in a
distributed system. The guard at the write point does.

Lease renewal runs on a ticker at `leaseDuration/3` and extends the lease by a
full `leaseDuration`, so one missed renewal (a network hiccup) does not lose
the lease. Only a definite loss (`ErrLeaseLost`, the conditional UPDATE matched
nothing) cancels the run; any other renewal error is logged and retried on the
next tick.

**Failure policy.** A failed attempt goes through `job.OnFailure`: transient
errors return the job to the queue while `attempt < MaxAttempts`, then fail it.
A runner marks errors that retrying cannot fix by wrapping
`runner.ErrPermanent` (e.g. bad params); those — and a runner mismatch — fail
the job on the first attempt instead of burning the rest.

**Shutdown.** Once a job is claimed, every exit path leaves it in a definite
state, even after SIGTERM: state writes after the run use a context detached
from shutdown (`context.WithoutCancel`) and bounded by `writeTimeout` (10 s,
must stay inside `terminationGracePeriodSeconds`). A run interrupted by
shutdown is handed back with `ReleaseForRetry("worker shutdown")` — not run
through the failure policy, and republished through the outbox — so another
worker takes it immediately instead of after lease expiry. A run that finished
just as shutdown began still saves its result. The attempt counter is not
rolled back: it is the fencing token and must only grow, so a shutdown costs
one attempt. Draining (finish the current job before exiting) is still to do;
today the runner is cancelled at once.

### D-4. Transactional outbox for the Postgres → Kafka hand-off

`submit` writes the experiment, the jobs, **and** the outbox rows in one
transaction. The controller reads unpublished rows under
`FOR UPDATE SKIP LOCKED`, sends them to Kafka, marks them published, and only
then commits.

Why: writing to Postgres and Kafka directly is a dual write with no shared
transaction. Either order leaves a window where a crash produces jobs nobody
will run, or messages pointing at jobs that do not exist.

**Order matters:** Kafka first, stamp second. A crash in between leaves the rows
unpublished → they are sent again → duplicate in Kafka, which `Claim` absorbs.
The reverse order could silently lose a job. General rule: when atomicity is
impossible, order the operations so failure causes a *repeat*, not a *loss*.

The transaction stays open across the Kafka send because the `FOR UPDATE` lock
only exists inside it — closing early would let a second controller instance
publish the same rows.

**Every transition back to `queued` writes a new outbox row**, in the same
transaction: `CreateExperiment`, `ReclaimExpired` and `ReleaseForRetry` all go
through `store.outboxValues` + `store.enqueue`. With Kafka as the only delivery
path, a requeued job whose original message was already consumed would
otherwise never run again and its experiment would never finish. A fenced
requeue matches zero rows and enqueues nothing; any duplicate message that
does slip through is absorbed by `Claim`.

### D-5. Controller is a reconciliation loop, not an event handler

The controller periodically compares desired state with actual state and fixes
the difference. It does not depend on events arriving.

Reconcile pass, **in this order** (order prevents races, no locking needed):

1. Reclaim expired leases (`state='running' AND lease_until < now() AND attempt < max`),
   republishing each through the outbox (see D-4)
2. Fail exhausted jobs (`attempt >= max`) — without this they sit in `running` forever
3. Score new results
4. Finalize experiments with no `queued`/`running` jobs left

Every operation is idempotent, so the pass can run every 5s for years and a
second pass immediately after the first changes nothing. A controller that was
down for an hour catches up on restart with no replay.

Publishing was moved **out** of reconcile into its own goroutine with a 500 ms
tick: it reacts to new rows rather than correcting drift, and every delay there
is idle worker time.

### D-6. Multiple controller instances must be safe

Not because several are needed — one is enough — but because Kubernetes runs
two pods briefly during a rolling update. Every controller query uses either
`SKIP LOCKED` or a conditional UPDATE. This constraint must not be broken.

### D-7. Determinism is a product feature

Seeds are derived (`FNV-1a` over experiment id + combination index), never
random. A job recreated after a crash reproduces the same run.

`Cartesian` sorts parameter names before expanding, because Go map iteration is
deliberately randomised. Without the sort, `idx` would mean a different
parameter combination on every run, and stored results would become
unreproducible. There is a golden test pinning `DeriveSeed` output: if it ever
fails, reproducibility of all prior experiments is broken.

Range expansion computes `from + i*step` rather than accumulating, to keep
floating-point error bounded to a single rounding.

Two distinct guarantees, worth naming separately:
- **Replay reproducibility** — rerun gives the same result (local model, fixed seed).
- **Record reproducibility** — cannot rerun exactly, but everything that
  influenced the result is recorded (cloud STT engine, model version, date).

### D-8. Kafka was chosen for education, not for the task

Honest assessment: **RabbitMQ fits job distribution better.** Dynamic
dispatch instead of pinned partitions; no "consumers ≤ partitions" ceiling; no
head-of-line blocking from one slow job; no rebalance breaking long jobs; retry
and priority built in.

Kafka's defensible justification: in a system that already runs Kafka as an
event bus, not adding a
second broker is reasonable.

Interview answer: *"For pure job distribution I would use RabbitMQ or a Postgres
queue; Kafka is justified when it is already there as an event bus."*

KEDA scales on RabbitMQ queue length and on Postgres query results too, so the
alternatives lose nothing in autoscaling.

### D-9. Offsets are committed after processing, tracked by the consumer

`DisableAutoCommit`. The worker calls `consumer.Done(rec)` after a job is
finished; `KafkaConsumer` tracks `partition → last processed offset` and commits
on a 1 s ticker, plus on partition revoke, plus once more on close.

Why not `CommitUncommittedOffsets`: it commits everything *fetched*, which would
mark unprocessed records as done — silent work loss. Tracking what was actually
processed is the only correct version.

Kafka stores the offset of the **next** record to read, hence `offset + 1`.
Getting this wrong is invisible (fencing absorbs the redelivery) which makes it
worse.

Offsets never leave `internal/bus`. The worker only says "this record is done".

Details that keep this correct:
- Commits are **synchronous** (`CommitOffsetsSync`); a failed commit puts the
  offsets back so the next tick retries. franz-go explicitly asks for a
  blocking commit in `OnPartitionsRevoked` when autocommit is disabled.
- The consumer tracks which partitions it **owns** (assigned / revoked / lost).
  A job that finishes after its partition moved away is ignored by `Done`:
  committing it could overwrite the new owner's offset. The redelivery this
  causes is absorbed by `Claim`. `OnPartitionsLost` drops pending offsets
  without committing.
- A record interrupted by **shutdown** is not marked done. Its job may still
  be `queued` (e.g. `Claim` never ran) with no other message pointing at it;
  leaving the offset uncommitted gets it redelivered.
- Known gap: because the commit point is the *highest* finished offset, a
  record whose handling failed transiently (e.g. `Claim` hit a DB error) is
  committed past by the next record. Its job stays `queued` without a message
  until the reconcile loop learns to republish stale queued jobs (see §7,
  retention).

### D-10. One runner per worker process

`SIMFLEET_RUNNER=crossing` at startup. The worker builds exactly one implementation.

Rationale: different workloads have incompatible resource profiles (simulation:
64 MB, milliseconds; STT: hundreds of MB, minutes) and must scale
independently. In Kubernetes this is one Deployment and one ScaledObject per
workload, same image, different env var.

`task.Runner` from the DB is still checked against the worker's runner — a
mismatch means a routing or configuration error, and failing the job beats
running it with the wrong workload.

### D-11. Migrations are a separate command

Services do not migrate on startup. With N replicas they would all race; and a
production service should hold data-plane permissions, not DDL permissions.

`cmd/migrate` + goose + `//go:embed`. In Kubernetes this becomes a Job run as a
Helm pre-install/pre-upgrade hook.

### D-12. Small decisions worth remembering

- **`enum` over `text`+CHECK** for job state: self-documenting schema, typos
  rejected by Postgres instead of silently matching zero rows. Cost: removing
  a value requires recreating the type. Transition *rules* live in Go; the DB
  only checks domain membership.
- **UUIDv7** for primary keys: time-ordered, so inserts append to the right edge
  of the B-tree index instead of scattering.
- **`job_results` separate from `jobs`**: results are written once, job state
  changes on every transition. Postgres MVCC rewrites the whole row on UPDATE,
  so keeping heavy metrics in the frequently-updated table would be wasteful.
- **`worker_id` lives in both tables, meaning different things**: in `jobs` it
  is the current lease holder (cleared on completion); in `job_results` it is
  attribution (immutable).
- **Partial indexes** for the hot queues: `WHERE state='running'` and
  `WHERE published_at IS NULL` stay small no matter how big the tables get.
- **No progress counter column in `experiments`**: 10k jobs updating one row is
  a hot spot (lock contention, dead tuples). Counters belong in Redis,
  rebuildable from a `GROUP BY`.
- **Artifacts are written before the DB row**: an orphaned S3 object is
  harmless, a DB row pointing at a missing object is a user-visible error.
- **LocalFS blob writes via temp file + rename**: reproduces S3's atomic
  visibility, so a crash never leaves a truncated artifact.

---

## 4. Current state

### Working end to end

Submit a spec → jobs expand → Kafka delivers → workers run → results stored →
controller scores and closes the experiment.

```bash
docker compose up -d
go run ./cmd/migrate
./bin/runlocal spawn 5          # controller + 5 workers
./bin/simfleet submit examples/crossing-smoke.yaml
```

### Packages

```
internal/
├── core/          Spec, Sweep, ParamValue (typed, not any), Task, Result,
│                  Evaluation, ExperimentID/JobID. No external deps.
├── job/           State machine: states, events, transition table, OnFailure
├── runner/        Runner interface + Descriptor registry (IN FLIGHT, see §5)
│   └── crossing/  Pedestrian-crossing simulation, self-registers via init
├── expander/      Cartesian, DeriveSeed, ValidateSweep, Create
├── store/         Postgres: jobs, results, outbox, scoring queries
├── scoring/       Evaluate, normalize, combine, zones, Validate
├── execution/     Executor: claim → lease → run → save, with fencing
├── controller/    publishLoop + reconcileLoop
├── worker/        readLoop + processLoop over a Consumer interface
├── bus/           Kafka producer and consumer group adapter
├── blob/          LocalFS (S3 not yet)
├── config/        Env-based config per command
├── app/           Shared command scaffold: logging, signals, exit code
└── dbtest/        Integration test helpers (tag: integration)
```

### Tests

| Test | Kind | What it proves |
|---|---|---|
| `scoring` unit tests | fast | Direction of scale, clamping, gates override components, min is non-compensating, zone boundaries (`>= Min`), missing metric errors |
| `TestFencingRejectsStaleResult` | integration | **The key test.** Worker A claims → lease expires → reclaimed → B claims → A's stale write is rejected, exactly one result row, and it is B's |
| `TestDuplicateResultIsIdempotent` | integration | Redelivery does not create a second result row |
| `TestReclaimSkipsExhausted` | integration | Jobs at max attempts are not requeued; `FailExhausted` retires them. Tests the boundary, not just the outcome |
| `TestScoringPipeline` | integration | Scores computed, second pass idempotent, threshold change triggers rescore |
| `TestPublishFailureKeepsRowsPending` | fast (fake bus) | Failed publish rolls back; rows are retried on the next pass |

Run: `make test` (fast) / `make test-integration` (`-tags=integration`, uses
testcontainers).

### The crossing model

Kinematic: car approaches a pedestrian at origin, detects within sensor range,
reacts after a delay, brakes at a fixed rate. Metrics: `min_distance_m`,
`final_speed_mps`, `sensor_bias_m`. Flag: `collision`.

Known simplifications, documented deliberately:
- Controller brakes at full force rather than choosing braking effort, so
  there is no meaningful "achieved deceleration" metric. `max_decel_mps2` is a
  sweep axis (road conditions), not a metric — scoring an *input* would penalise
  a scenario for the road it was given.
- Sensor noise is sampled **once per run** as a bias, not per step. Per-step
  noise made detection a min-over-many-draws, which depends on `dt` — a number
  with no physical meaning. One sample per run keeps the statistical meaning and
  removes the `dt` dependence and the detection flicker.
- Collision is "crossed x=0 while moving", not "distance < threshold". The
  0.3 m threshold is a user safety threshold and belongs in scoring.

---

## 5. In-flight refactor (pick this up first)

A `Registry` of **descriptors** is replacing the registry of runner instances.
The goal: everything that depends on a runner implementation lives in one place,
and the platform stays ignorant of what runners do.

```go
type Descriptor struct {
    Name    string
    Profile core.Profile              // Timeout, Deterministic
    Params  map[string]core.ParamKind
}

type Runner interface {                // one method now
    Run(ctx context.Context, t core.Task) (core.Result, error)
}

func Register(d Descriptor, f Factory)     // from init
func Get(name string) (Descriptor, bool)
func Names() []string
func Build(name string, env Environment) (Runner, error)   // worker only
```

**Resolved while deciding this — do not re-litigate:**

- `Partitions` does **not** belong in `Descriptor`. It is a property of the
  topic (how many workers are planned, what the broker can take), not of the
  workload. → `KAFKA_PARTITIONS` in controller config.
- `MemoryMB` does **not** belong in `Descriptor` either. It is a deployment
  property and will be written in the Kubernetes manifest anyway; keeping it in
  both places guarantees they diverge.
- **The controller needs no runner registry at all.** The runner name is
  written into the outbox row at submit time. The controller reads rows and
  hands them to the bus. For `EnsureTopic` at startup it takes `KAFKA_TOPIC`
  from config.
- **Broker vocabulary stays inside `bus`.** Topics, groups and partitions are
  Kafka internals: `runner`, `expander` and `store` never mention them, and
  `NewExperiment` / `OutboxRow` carry only the `Runner` name. `bus` maps runner
  → destination (`Kafka.topicFor`, currently always `KAFKA_TOPIC`). Swapping
  Kafka for RabbitMQ means a new `bus` adapter, nothing else. The DB column is
  `outbox.runner` (renamed from `topic` in migration 004).
- Only submit (currently `runlocal submit`; `cmd/simfleet` does not exist yet)
  and `cmd/worker` (execute) import runner packages.
- **One topic, one group for now** (`sim.jobs`, `simfleet-workers`). Per-runner
  topics matter only when two workloads run *simultaneously* — not when one is
  swapped for the other.

**Remaining mechanical work:**

- [x] Move `Name`/`Profile`/`ParamSchema` out of `Crossing` methods into the
      `Register` call
- [x] `execution.Executor` holds one `runner.Runner` + descriptor, built
      once at startup (not per job). `Runner.Run` is documented as safe for
      concurrent use — one instance is shared across jobs.
- [x] `expander.Create` takes a `runner.Descriptor` (resolved by the caller via
      `runner.Get`) instead of a `Registry` interface — easier to test than a
      global lookup inside the package
- [x] `cmd/worker`: `runner.Build(cfg.Runner, ...)`, fail fast on unknown name
      with the available list in the message
- [x] Remove `topicsFor(reg)` and any `runner.Names()` call from
      `cmd/controller` — this is what was dragging the imports in
- [x] `OutboxRow.Topic` renamed to `Runner`; `bus.Kafka.topicFor` maps it to
      the Kafka topic, so the store knows nothing about the broker
- [x] Validate `Profile.Timeout > 0` at executor construction (a zero timeout
      makes `context.WithTimeout` return an already-expired context, and every
      job cancels instantly). `NewExecutor` returns an error; the worker builds
      the executor before joining the consumer group, so it fails fast.
- [x] Runner mismatch (`task.Runner != desc.Name`) marks the job failed at once
      (`runner.ErrPermanent`) instead of leaving it in `running` until the lease
      expires and burning all attempts

### Other loose ends

- `runlocal` does not run migrations; `controller`/`worker` do not check schema
  version. Plan: `runlocal` migrates (it is a dev harness, single instance);
  services check the version and refuse to start with a clear message.
- `store.NextQueued` / `NextQueuedForExperiment` are obsolete now that Kafka
  delivers job ids, but still used by `runlocal`'s legacy DB-polling modes
  (`work`, `runSingle`, `spawnOld`). Delete those modes and the queries
  together.
- `HasActiveExperiments` moved from worker to `runlocal`, which decides when
  everything is done and sends SIGTERM. Workers never decide this — in
  Kubernetes they must outlive any single experiment.

### Review findings (not yet fixed)

Found in an architecture review on 2026-10-26. The core design (fencing,
outbox, idempotent reconcile, metrics vs scores, deterministic seeds) holds
up; these are the places where code, docs and tooling disagree.

**Docs vs code**

| This document says | Reality |
|---|---|
| `cmd/simfleet` submits experiments (§2, §4) | Does not exist. Submit is `runlocal submit`. The root `main.go` prints "Hello from WSL", and a 2.3 MB Linux binary `simfleet` is committed at the repo root. |
| `./bin/runlocal spawn 5` starts controller + 5 workers (§4) | `spawn` takes a spec path, not a count; 5 is hardcoded (`cmd/runlocal/main.go:79`), and it starts **workers only**, no controller. |
| `make test-integration` with `-tags=integration` (§4) | No such target, and no test file has a `//go:build integration` tag, so `make test` starts testcontainers. `make run-api` points at a nonexistent `cmd/api`. |
| `TestScoringPipeline` proves scoring, idempotency and rescoring (§4) | Broken. Step 1 calls `PublishOutbox` instead of `Reconcile` (`controller_test.go:47`), so the result map is empty and the zone loop passes vacuously; the "second pass" check then fails because that is the first scoring pass. Step 3 (threshold change → rescore) has no assertion. Fails on clean HEAD too. |
| `job_results.worker_id` is attribution (D-12) | Column exists (migration 002) but `SaveResult` never writes it; `ResultRecord` has no worker id. |
| Publish loop ticks every 500 ms (D-5) | `controller.go:78` uses 50 ms (comment still says 500ms). |

**Bugs**

- `cmd/runlocal/main.go:40`: `slog.Any("error", err)` builds an attribute and
  discards it — every `runlocal` error is silent and the exit code is 0.
- `internal/bus/kafka.go`: the error from `ensureTopic` is ignored (`:33`), and
  `if cl == nil` after `kgo.NewClient` (`:26`) should be `if err != nil`.
- `internal/core/param.go` `UnmarshalJSON`: Go `case`s do not fall through, so
  the empty `case int:` / `int64` / `uint` branches leave the value as
  `KindInvalid`. Harmless today (JSON decodes numbers as `float64`), misleading.
- A record whose handling fails transiently is committed past by the next
  record — see the known gap in D-9. Fix together with the §7 retention item:
  reconcile republishes jobs `queued` for too long.

**Cleanup**

- `internal/execution/executor.go:81`: the crash-test code (`WorkerId == "w1"`
  → `os.Exit(137)`) lives in production code. Move it behind a build tag or an
  env var.
- `cmd/worker/main.go:24-25` calls `envfile.LoadEnv()` and `core.SetupLogger()`
  again; `core.RunService` already does both.
- `Execute` returns `claimed=true` for a failed run, so the worker logs it as
  "job done". Return the outcome explicitly (done / retried / failed / skipped).
- Shutdown cancels the runner immediately; draining (finish the current job
  within `drainTimeout`) is still to do — see D-3 and the Kubernetes roadmap.

---

## 6. Roadmap

Each part is wanted in **minimal but real** form — the project exists to prove
distributed-systems fundamentals, so every piece must actually run.

### Next: observability (1–2 evenings)

Prometheus metrics in each service, one Grafana dashboard,
`kube-prometheus-stack` via Helm later. Metrics worth having: job duration
histogram, jobs by state, consumer group lag, outbox depth, fenced-write
counter (a spike means leases are shorter than real job duration).

Do this first — without it, debugging inside a cluster is blind.

Not needed: tracing, alerting, custom storage.

### Then: Kubernetes locally (2–3 evenings)

Dockerfile per service (static Go, scratch base), one Helm chart with three
Deployments, k3d (k3s in Docker — lighter than kind: control plane ~0.5 GB vs
~1 GB), KEDA with one ScaledObject on consumer lag, `minReplicaCount: 0`.

Stateful dependencies stay **outside** the cluster, in docker-compose — which
mirrors AWS, where Postgres is RDS and S3 is S3. Run one environment at a time
(16 GB laptop).

This is where graceful shutdown finally matters: SIGTERM → stop taking new jobs
→ finish the current one → exit, with `drainTimeout + margin ≤
terminationGracePeriodSeconds`.

Note: `GOMEMLIMIT` must be set to ~90% of the container memory limit. Go 1.25+
honours cgroup CPU limits automatically but **not** memory limits — the GC will
happily grow the heap into an OOM kill.

### Then: Redis (1–2 evenings)

Live progress counters per zone, made idempotent with a Lua script (`SADD`
returns 1 only for a new member → `HINCRBY`). Keys need a common hash tag
(`{exp:123}:done`, `{exp:123}:counts`) so a script can touch both in Redis
Cluster.

Benchmark worth including in the README: 300 concurrent writers against a hot
counter row in Postgres vs. `HINCRBY`, measuring throughput and table bloat.
Turns "we used Redis because it's fast" into a measured argument.

Leases stay in Postgres — moving them to Redis is a separate decision with its
own trade-offs.

### Then: AWS (2–3 evenings)

Terraform modules: `vpc`, `eks`, `data` (RDS, S3, ECR). State in S3 with
`use_lockfile` (Terraform ≥1.10 does locking natively; no DynamoDB table
needed). Bootstrap config with local state creates the state bucket.

VPC gateway endpoints for S3 — free, and they avoid NAT data charges.
Watch out: NAT Gateway (~$33/month just for existing), unattached Elastic IPs,
orphaned EBS volumes.

Account is on the post-2025-07-15 credit model: $100–200 credits, free plan
expires after 6 months. Access is via IAM Identity Center with short-lived
credentials (`aws sso login`), no long-lived access keys.

Run a 10k-scenario experiment, record throughput and cost per experiment, take
Grafana screenshots, then `terraform destroy`.

### Finally: write-up (1–2 evenings)

README with architecture, the measured numbers, and an honest "known
limitations" section. Design doc. ADRs from §3.

### Optional, if time remains

- **`slow-crossing` runner**: `crossing` wrapped in a configurable delay.
  Zero new dependencies, but exercises what is currently untested — rebalance
  mid-job, head-of-line blocking, lease renewal across many cycles. The delay
  must be interruptible (`select` on `ctx.Done()`), or lease loss will not stop
  the job.
- **`synctest` lease test**: `keepLease` under Go 1.27's virtual clock — a 60 s
  lease verified instantly and deterministically, with a fake store. Good
  interview material.
- **STT runner** via go-whisper as a **separate HTTP service**, not cgo. Decided:
  cgo breaks static linking and scratch images, complicates arm64
  cross-compilation, and a panic in C++ kills the whole worker process.
- **Kafka vs. `SKIP LOCKED` comparison** with throughput and fairness
  benchmarks. The strongest possible design-doc content, but needs a second full
  implementation.

---

## 7. Known limitations (for the README)

State these openly — being able to name them is worth as much as the code.

- **The whole sweep is materialised at submit time.** For sweeps beyond ~100k
  jobs this needs chunked expansion with admission control: the controller keeps
  a bounded window of jobs in flight per experiment and releases more as earlier
  ones finish. Three reasons it matters: fairness (a 100k sweep must not starve
  a small urgent one — Kafka delivers partition messages in order), cancellation
  (messages already in Kafka cannot be recalled), and backpressure (queue depth
  stops reflecting real load). `Cartesian`'s positional-number-system expansion
  already supports computing combination *N* without materialising the rest.
- **Pinned partitions distribute unevenly.** A slow worker's partition drains
  slower while others idle. Visible as uneven per-partition lag at the end of a
  run. This is the Kafka weakness RabbitMQ does not have.
- **Rebalance mid-job duplicates work.** `OnPartitionsRevoked` commits processed
  offsets, but a job already running is not cancelled. Fencing prevents
  incorrectness; the work is simply done twice.
- **Retention vs. stalled consumers.** If all workers are down longer than the
  topic retention, messages vanish and jobs sit in `queued` forever. The fix is
  republication from the reconciliation loop (jobs `queued` for > N minutes with
  no recent outbox row), not a longer retention.
- **Progress is computed by query.** At thousands of concurrent experiments this
  needs the Redis counters.
- **No multi-tenancy.** No accounts, no quotas, no per-team fair share — only
  the hooks for it.
- **Scoring hash is `sha256(json.Marshal(scoring))`.** Stable across restarts,
  but changes if struct field order changes. A canonical form would be needed
  for strict guarantees.

---