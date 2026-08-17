# TSS — project rules

Read `TSS-Architecture.md` before changing anything in `tss/core/`. It is the spec, not a suggestion.

## Model

- An **AGENT** is a machine. It heartbeats and holds the presence lease. It has no `busy` state —
  capacity is counted, not flagged.
- A **RESOURCE** is a device cabled to that machine. It is the unit of allocation.
  States: `free | busy | unhealthy | retired`. `unhealthy` is present-but-broken (someone fixes
  it); `retired` is no longer on the bench (it vanished from a re-registered inventory). Never
  conflate them, and never hard-delete a resource that has `job_resources` history.
- A **JOB** claims a *set* of resources, all on one agent (co-location), all-or-nothing.

## Non-negotiable invariants

Any change that could violate one of these needs a test proving it doesn't.

- **I1** At any instant at most one agent is AUTHORIZED to run a job — holds its *current* epoch —
  and across the run at most one completion report is ever accepted for it. Not "believes it owns":
  a partitioned zombie sincerely believes it owns the job it is still executing. Execution is
  at-least-once by design; authorization and the *result* are exactly-once.
- **I2** No resource is held by two jobs. Structural: `resources.current_job_id` is a single column,
  guarded by `WHERE state='free'` on claim.
- **I3** Every submitted job reaches a terminal state within the run deadline.
  Terminal = `passed | failed | infra_error | cancelled | dead_letter`. *(liveness, end-of-run)*
- **I4** A job never runs on resources lacking its required capabilities. Verified against the agent's
  ground truth, not TSS's copy of what the agent claimed.
- **I5** No resource of an `offline` agent is `busy` or holds a `current_job_id`. (Stated negatively
  on purpose: "all free" was only true before `retired` existed, and would break on the next state.)
- **I6** A hung job is terminated by the job-timeout sweep, not by presence expiry.
- **I7** A terminal job's outcome is never overwritten.
- **I8** A job in `assigned`/`running` holds **exactly** `resource_count` resources — never fewer,
  never more — AND conversely every `current_job_id` points at a job still `assigned`/`running`.
  Both directions: counted from the job's side alone, a device orphaned by a finished job is never
  looked at. No partial allocation, ever.
- **I9** At most one job reserves at a time, all its reservations are on one *feasible* agent, and no
  reserved resource is claimed by another job. *(Checked against scheduler state, not the DB —
  reservations deliberately leave no DB trace.)*

## Rules that follow

**Allocation**

- The claim is ONE transaction (`BEGIN IMMEDIATE`) covering every resource plus the job row. Each
  statement has a `WHERE` guard and a rowcount check; ANY zero rolls back ALL of it.
- Never a loop of individual claims with cleanup on failure. A crash mid-loop strands devices with no
  owner and no lease to expire them. The database's rollback is the point.
- Resource `UPDATE`s inside the claim are issued **sorted by `resource_id`** (lock ordering). SQLite
  hides lock-order deadlocks; Postgres will not.
- `sqlite3.OperationalError` (SQLITE_BUSY) on the claim path is a lost race, not an error.
- **Reserving is not claiming.** A reservation leaves the resource `free` with no owner and lives in
  scheduler memory — never a DB write. It covers exactly one feasible target agent. If no agent is
  feasible, do not reserve: set `blocked_reason='no_capable_agent'` and emit an event.
- Feasibility assessment is PER STARVING JOB — every one gets flagged and starts its dead-letter
  clock. Reservation is exclusive to the oldest FEASIBLE starving job: an unsatisfiable job at the
  head must not suppress reservation for the jobs behind it.
- Co-location is ONE predicate in the matcher (`all candidate resources share an agent_id`), not an
  assumption spread through the claim, reaper, and reservation logic. It exists for physical and
  reliability reasons, **not** to prevent deadlock — all-or-nothing does that.

**Ownership and liveness**

- Every assignment carries an epoch, self-incremented in SQL (`epoch = epoch + 1`), never computed in
  Python. Every completion is validated against it. Any transition that ends or reassigns ownership
  bumps the epoch — including cancel.
- Presence leases belong to AGENTS, always, whatever their load. Resources have no lease.
- Heartbeat renewal is guarded by `state != 'offline'`. A reaped agent gets `410 presence_expired` and
  re-registers; it must never renew itself back to life.
- Reap **releases claims and nothing else**: free only `state='busy'` resources, leaving `unhealthy`
  and `retired` untouched. The agent is the authority on device health; TSS never infers it. A bench
  crashing tells you nothing about whether a J-Link came back.
- Reaper fan-out requeues per `SELECT DISTINCT current_job_id`, never per resource, and the requeue
  is additionally guarded on `state IN ('assigned','running')`. A job on three devices is requeued
  once, with one epoch bump.
- `tried_agents` is appended at CLAIM time only, never on requeue — the bench is recorded when it
  takes the job; appending again on the way out counts one bench twice. Poison detection counts
  DISTINCT entries (`len(set(...))`), not list length.
- Agent health and device health are separate. One bad device does not take the machine offline.
- Presence timeout and job timeout are different things and must never be collapsed.

**General**

- ALL writes live in `core/store.py`. The scheduler decides; the store commits. API handlers and the
  reaper never write SQL directly.
- Persisted timestamps are wall-clock unix floats (UTC). `time.monotonic()` is for measuring durations
  inside one process only, never for a column — it resets on restart and would break recovery.
- Background loops (reaper, health) wrap their body in `try/except`, log, and continue. A background
  task must never die silently.
- Retry and poison decisions key off `tried_agents` (distinct benches), not `attempt`.
- `FAILED` is terminal and never auto-retried. Only `INFRA_ERROR` retries. A hung job is
  `infra_error:timeout`, never `failed`.
- `state` says what happened; `outcome` says whose problem it is. A dead-lettered job is
  `state='dead_letter'`, `outcome='infra_error'` — never `outcome='dead_letter'`. Repeating the state
  in the outcome discards the FAILED-vs-INFRA_ERROR distinction on exactly the jobs that failed worst.
- The scheduler SKIPS jobs it cannot satisfy and walks on; it never blocks the queue at the head.
  Safe at N=1; at N>1 it starves multi-device jobs, which is what the reservation guard is for.
- No new dependency without asking.

## Testing

- `just test` must pass before any commit.
- New allocation, scheduling, or lease logic requires a concurrency test using **OS threads on
  separate connections with overlapping resource sets** — not asyncio tasks, which have no preemption
  point between the check and the act and would pass against a naive implementation.
- `just chaos` — 15 agents × 2–4 devices, 100 jobs at 30% multi-device, 5 seeds — must report zero
  invariant violations. **This is the merge gate.**
- Integration tests go over real HTTP. Never mock the transport; mocked tests pass while the real
  thing deadlocks.
- Every chaos run logs its seed.
- Before trusting a new check, sabotage the code and watch it fail. A checker nobody has seen fail is
  not evidence. Record which harness caught it: concurrency bugs need threads, failure bugs need
  chaos, latency bugs need timing assertions — each is blind to the other two.

## Working style

- Build in the order in `TSS-Architecture.md` §10. Stop after each step and show what runs.
- When asked for a test that should catch a bug, write it so it **fails against the naive
  implementation first**. A test that passes before the fix proves nothing.
- Flag it explicitly rather than guessing if the spec is ambiguous.
