# TSS — project rules

Read `TSS-Architecture.md` before changing anything in `tss/core/`. It is the spec, not a suggestion.

## Model

- An **AGENT** is a machine. It heartbeats and holds the presence lease. It has no `busy` state —
  capacity is counted, not flagged.
- A **RESOURCE** is a device cabled to that machine. It is the unit of allocation.
- A **JOB** claims a *set* of resources, all on one agent (co-location), all-or-nothing.

## Non-negotiable invariants

Any change that could violate one of these needs a test proving it doesn't.

- **I1** At most one agent believes it owns a job at any moment, and exactly one result is ever
  accepted per job. (Execution is at-least-once by design; the *result* is exactly-once.)
- **I2** No resource is held by two jobs. Structural: `resources.current_job_id` is a single column,
  guarded by `WHERE state='free'` on claim.
- **I3** Every submitted job reaches a terminal state within the run deadline.
  Terminal = `passed | failed | infra_error | cancelled | dead_letter`. *(liveness, end-of-run)*
- **I4** A job never runs on resources lacking its required capabilities. Verified against the agent's
  ground truth, not TSS's copy of what the agent claimed.
- **I5** An `offline` agent's resources are all free and hold no job.
- **I6** A hung job is terminated by the job-timeout sweep, not by presence expiry.
- **I7** A terminal job's outcome is never overwritten.
- **I8** A job in `assigned`/`running` holds **exactly** `resource_count` resources — never fewer,
  never more. No partial allocation, ever.
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
- Reaper fan-out requeues per `SELECT DISTINCT current_job_id`, never per resource. A job on three
  devices is requeued once, with one epoch bump and one `tried_agents` append.
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

## Working style

- Build in the order in `TSS-Architecture.md` §10. Stop after each step and show what runs.
- When asked for a test that should catch a bug, write it so it **fails against the naive
  implementation first**. A test that passes before the fix proves nothing.
- Flag it explicitly rather than guessing if the spec is ambiguous.
