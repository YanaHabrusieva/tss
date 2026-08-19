# TSS — build brief for Claude Code

*A point-in-time artifact: the original build handoff, written before a line of TSS existed and
deliberately not rewritten since. The per-step prompts below are what actually drove the early
commits — `git log --reverse` shows them arriving in this order — so where this brief and the
finished tree disagree, the tree is right and the disagreement is the record. `AILOG.md` is the
account of what changed along the way.*

Paste section 1 as your opening message in Claude Code, with `TSS-Architecture.md` in the repo root.
Sections 4 and 5 become `CLAUDE.md` and the repo tooling — copy them in on day one so every future
session inherits the invariants.

---

## 1. Opening prompt

> Read `TSS-Architecture.md` in full before writing any code. It is the spec. Pay particular attention
> to §1.2 — the unit of allocation is the **device**, not the bench. One agent machine owns several
> resources (DUTs); jobs claim a set of resources; two jobs run on one bench concurrently as long as
> their resource sets are disjoint.
>
> We're building a Test Scheduling Service: a job broker that routes hardware-in-the-loop test jobs to
> devices across a fleet of testbed machines, survives machines dying mid-job, and shows fleet state to
> a human. Python 3.12, FastAPI, SQLite (WAL), httpx, Rich, pytest. Single process.
>
> Build in the order given in §10. Stop after each step and show me what runs — do not build ahead.
> Start with **step 1 only**: `core/models.py`, `core/store.py`, `core/config.py`, the schema from §5
> (agents, resources, jobs, job_resources, events), a `justfile` with `serve`/`test`/`chaos`/`lint`,
> and `tests/test_concurrency.py` + `tests/test_allocation.py`.
>
> The most important thing in step 1 is the **N-way all-or-nothing claim** in §3.3. Write the tests
> **first**:
>
> - `test_concurrency.py` — a bench with 3 devices; 50 **OS threads, each on its own sqlite3
>   connection**, each trying to claim a random *overlapping pair* of those devices. Assert: no device
>   is ever held by two jobs, and every job that succeeded holds exactly 2.
> - `test_allocation.py` — force a claim to fail on the last resource of the set; assert every earlier
>   resource in that transaction is back to `free`. No partial holds, ever.
>
> Threads and separate connections specifically — with asyncio tasks over a synchronous driver there's
> no preemption point between the check and the act, so a naive check-then-act version would pass and
> prove nothing.
>
> I want to see both tests fail against a naive implementation before you make them pass.
>
> Notes on §3.3: every `UPDATE` needs a rowcount check and any single failure rolls back the **whole**
> transaction; the epoch is self-incremented in SQL (`epoch = epoch + 1`, never passed in); and the
> losing thread may surface as `sqlite3.OperationalError` (SQLITE_BUSY) rather than `rowcount == 0` —
> handle both as "lost the race."

**Why phrase it this way.** Asking for the failing test first is the difference between "the model
wrote a lock" and "you know the lock works." It also hands you a concrete artifact for the
AI-reflection section: the naive version, the tests that killed it, the fix.

---

## 2. Step-by-step prompts

One per step; do not batch. Run each yourself before moving on.

| Step | Prompt seed | Done when |
|---|---|---|
| 1 · Store | Schema + N-way claim + the two tests above | 50-thread overlapping-pair race: no double-book, no partial hold |
| 2 · Presence | `register` with inventory, guarded heartbeat renewal, reaper sweep 1 with **fan-out**, `GET /v1/fleet`, `tss fleet` | An **idle** bench you `kill -9` flips OFFLINE within ~14s and all its devices go free |
| 3 · Happy path (N=1) | `POST /v1/jobs`, matcher, long-poll heartbeat, `/start`, `/complete` | One single-device job runs end to end; a second job runs concurrently on the same bench's other device |
| 4 · Fencing | Epoch bump on requeue/cancel, `409 stale_epoch`, `410 presence_expired` | Zombie test passes (below) |
| 5 · Multi-device | Turn on `--multi-pct 30`; reservations + feasibility filter; chaos; invariant checker | 15 agents × 2–4 devices, 100 jobs, I1–I9 hold on 5 seeds |
| 6 · README + TUI | `README.md`, `tss watch` with nested resources, `tss why` | Someone who's never seen it can run it from the README alone |
| 7 · Hardening | Per-device health, quarantine attribution, drain, operator verbs | Optional — only if time remains |

**Do step 2 before the happy path.** Presence-lease reaping is the mechanism everything else rests on,
and getting an idle bench to go OFFLINE is the cheapest possible test of it. If you build scheduling
first you'll retrofit leases around it.

**Prompt for step 2's fan-out, verbatim:**

> Implement reaper sweep 1 per §3.5. The dangerous part is the fan-out: a dead bench may have been
> running several jobs across its devices, and a *single* job may occupy several of those devices.
>
> Collect `SELECT DISTINCT current_job_id FROM resources WHERE agent_id = ? AND current_job_id IS NOT
> NULL`, then requeue **per job, not per resource**. A job on three devices must be requeued once, with
> its epoch bumped once and the agent appended to `tried_agents` once.
>
> Write `tests/test_fanout.py`: a bench with 4 devices runs job-X on 3 of them and job-Y on 1. Kill it.
> Assert exactly 2 requeues, each epoch incremented by exactly 1, each `tried_agents` of length 1.

**Prompt for step 4, verbatim — this is the one that matters:**

> Implement epoch fencing per §3.5. Four things I will check specifically:
>
> 1. The reaper bumps `jobs.epoch` in the **same transaction** as the requeue and the resource release.
> 2. `POST /v1/jobs/{id}/complete` rejects any report whose epoch ≠ the job's current epoch, with
>    `409 {"error":"stale_epoch","action":"abandon_job"}` — and does **not** free that job's resources,
>    since they now belong to someone else.
> 3. Heartbeat renewal is guarded by `state != 'offline'`, and a reaped agent gets `410 presence_expired`
>    telling it to re-register. It must not renew itself back to life.
> 4. Cancelling a running job also bumps the epoch, so a late completion can't overwrite CANCELLED.
>
> Then write `tests/test_lease.py` for the zombie scenario in §3.5: agent-7 assigned at epoch 4 holding
> vg-01 + ag-01, goes silent, reaper frees both and requeues to agent-3 at epoch 5, agent-7 later
> reports success at epoch 4. Assert the stale report is rejected, agent-3's devices stay claimed, and
> the recorded outcome comes from agent-3.

---

## 3. What to watch the model get wrong

Check these by reading the diff, not by asking the model whether it did them.

| Likely mistake | What to look for |
|---|---|
| **Bench-level allocation** | The default instinct: `agents.state='busy'`, one job per machine. Wastes ~⅔ of a 3-device fleet. §1.2. |
| **Acquire-in-a-loop with cleanup** | `for r in resources: claim(r)` + try/except release. Looks equivalent to a transaction and isn't — a crash mid-loop locks devices with no owner and no lease to expire them. Must be one transaction. §7.5. |
| Rowcount checked on only some UPDATEs | Every resource UPDATE **and** the job UPDATE. Any zero → roll back all of them. |
| **Unsorted resource UPDATEs** | Issue them sorted by `resource_id`. Two transactions locking the same rows in different orders deadlock at the DB level. SQLite hides it (`BEGIN IMMEDIATE` serializes writers) — Postgres at Stage 2 does not. Costs one `sorted()`; invisible to test until it isn't. |
| Fan-out double-requeue | Iterating resources instead of `SELECT DISTINCT current_job_id`. Silent: no error, the job just dead-letters early with a burned retry budget. §7.3 item 5. |
| Epoch computed in Python | `epoch = current + 1` read-then-written is itself a race — in the mechanism built to prevent races. Must be `epoch = epoch + 1` in SQL. |
| Presence lease on resources | Liveness belongs to the machine. Per-resource leases mean N heartbeats per bench and a device that can outlive its host. |
| Presence only for loaded agents | A bench powered off while idle must still be reaped, or it keeps getting handed jobs. §3.5. |
| Unguarded heartbeat renewal | A renewal landing just after the reaper resurrects a dead lease. Needs `WHERE state != 'offline'`. |
| Device health = machine health | One dead J-Link should cost one device, not the bench. Separate `resources.state='unhealthy'` from `agents.state`. §4.2. |
| One timeout for everything | Presence TTL used as job timeout. A 20-minute test must not look like a dead bench. §7.1. |
| Job timeout with no owner | `max_duration_s` in the schema and nothing scanning it. It belongs in reaper sweep 2. |
| Hung job recorded as `failed` | It's `infra_error:timeout`. Recording a dead rig as a firmware failure breaks the §4.3 contract. |
| Retry gated on a single counter | Retry/poison decisions key off `tried_agents` (distinct benches), not `attempt`. |
| Multi-reserver starvation guard | Two starving jobs each reserving partial sets is a deadlock you built yourself. Exactly one reserver — the oldest. §3.4.1. |
| **Reserving = claiming** | A reservation must leave the resource `free` in the DB with no owner. If the model marks it `busy` or sets `current_job_id`, it has re-invented the partial hold — deadlock is back, and I8 won't catch it because the job isn't `assigned` yet. |
| **Reserving across benches** | Co-location means a job's devices come from ONE bench. Reserving one device on bench-01 and another on bench-02 idles two devices toward a set that can never be assembled. |
| **Reserving on an infeasible bench** | Step 1 is a feasibility filter on *total healthy inventory*, ignoring free/busy. A bench with 2 healthy VGs can never run a 3-VG job; reserving there idles a device forever. |
| **Impossible job reserves forever** | If no bench is feasible, don't reserve — flag `no_capable_agent`, emit an event, keep queued, dead-letter after `UNSATISFIABLE_TIMEOUT`. Silently waiting is indistinguishable from a broken scheduler. |
| Unscoped starvation guard | Holding the *whole* queue behind the oldest job collapses throughput. Reserve only the resources that job matches. |
| `time.monotonic()` in `presence_expires_at` | Monotonic resets on restart, breaking the zero-code restart recovery. Persisted times are wall-clock unix floats. §3.3. |
| Swallowed exceptions in background loops | One raised exception kills the reaper silently and the entire resilience story stops working. Wrap, log, continue. |
| Invariants checked against TSS only | I1 and I4 need the mock agents' ground truth. The `liar` profile is invisible if you check TSS against its own DB. |
| Chaos without a seed, or without `--multi-pct` | Unreproducible failures; and a single-device-only load never exercises the N-way claim, reservations, or I8. |
| Tests that mock the transport | Integration tests must go over real HTTP. Mocked ones pass while the real thing deadlocks. |

**The top two are the ones to look for first.** Bench-level allocation is what a model produces when
you say "assign jobs to agents," and acquire-in-a-loop is what it produces when you say "claim several
devices." Both look completely reasonable in review.

---

## 4. `CLAUDE.md` — invariants (copy into the repo)

```markdown
# TSS — project rules

## Model
An AGENT is a machine. It heartbeats and holds the presence lease.
A RESOURCE is a device cabled to that machine. It is the unit of allocation.
A JOB claims a SET of resources, all on ONE agent (co-location), all-or-nothing.

## Non-negotiable invariants
Any change that could violate one of these needs a test proving it doesn't.

- I1  At most one agent believes it owns a job at any moment, and exactly one result is ever
      accepted per job. (Execution is at-least-once by design; the RESULT is exactly-once.)
- I2  No resource is held by two jobs. Structural: `resources.current_job_id` is a single column,
      guarded by `WHERE state='free'` on claim.
- I3  Every submitted job reaches a terminal state within the run deadline.
      Terminal = passed | failed | infra_error | cancelled | dead_letter.  (liveness, end-of-run)
- I4  A job never runs on resources lacking its required capabilities. Verified against the
      agent's ground truth, not TSS's copy of its claim.
- I5  An OFFLINE agent's resources are all free and hold no job.
- I6  A hung job is terminated by the job-timeout sweep, not by presence expiry.
- I7  A terminal job's outcome is never overwritten.
- I8  A job in assigned/running holds EXACTLY `resource_count` resources — never fewer, never more.
      No partial allocation, ever.
- I9  At most one job reserves at a time, all its reservations are on ONE feasible agent, and no
      reserved resource is claimed by another job. (Checked against scheduler state, not the DB —
      reservations deliberately leave no DB trace.)

## Rules that follow
- The claim is ONE transaction (`BEGIN IMMEDIATE`) covering every resource plus the job row.
  Each statement has a `WHERE` guard and a rowcount check; ANY zero rolls back ALL of it.
  Never a loop of individual claims with cleanup on failure — a crash mid-loop strands devices.
- Resource UPDATEs inside the claim are issued in sorted order by resource_id (lock ordering).
- Co-location (all of a job's resources on one agent) is ONE predicate in the matcher, not an
  assumption spread through the claim/reaper/reservation. It exists for physical and reliability
  reasons, NOT to prevent deadlock — all-or-nothing does that.
- `sqlite3.OperationalError` (SQLITE_BUSY) on the claim path is handled as a lost race, not an error.
- ALL writes live in `core/store.py`. The scheduler decides; the store commits.
- Every assignment carries an epoch, self-incremented in SQL. Every completion is validated against
  it. Any transition that ends or reassigns ownership bumps the epoch.
- Presence leases belong to AGENTS, always, whatever their load. Resources have no lease.
- Reaper fan-out requeues per DISTINCT job, never per resource.
- Agent health and device health are separate. One bad device does not take the machine offline.
- Presence timeout and job timeout are different things and must never be collapsed.
- At most ONE starving job reserves at a time. A reservation leaves the resource `free` with no
  owner — it is scheduler memory, never a DB write. It covers exactly ONE feasible target agent.
  If no agent is feasible, do not reserve: flag `no_capable_agent` and alert.
- Persisted timestamps are wall-clock unix floats (UTC). `time.monotonic()` is for measuring
  durations inside one process only, never for a column.
- Background loops wrap their body in try/except, log, and continue. Never die silently.
- No new dependency without asking.

## Testing
- `just test` must pass before any commit.
- New allocation, scheduling, or lease logic requires a concurrency test using OS threads on
  separate connections with OVERLAPPING resource sets — not just a unit test.
- `just chaos` — 15 agents × 2-4 devices, 100 jobs at 30% multi-device, 5 seeds — must report zero
  invariant violations. This is the merge gate.
```

## 5. Repo quality tooling — build this on day one

Direct answer to *"AI tools you'd implement permanently in your repo to maintain a quality bar."* Have
it working, not just described.

```bash
# justfile
#   serve  → uvicorn tss.api:app --port 8090
#   test   → pytest
#   chaos  → python -m tss.chaos --agents 15 --jobs 100 --multi-pct 30 --profile mixed --seeds 5
#   lint   → ruff check --fix && ruff format

# .pre-commit-config.yaml
#   - ruff check --fix + ruff format
#   - pytest tests/test_concurrency.py tests/test_allocation.py tests/test_lease.py   (~3s)

# .github/workflows/ci.yml
#   - lint → unit → integration → chaos (5 seeds, invariants asserted)
#   - chaos failure is a merge blocker, not a warning
```

Plus `CLAUDE.md` above. Three files, about an hour, and it turns "I used AI" into "I built a harness
that keeps AI honest."

**Keep the numbers identical everywhere** — seed count, crash probability, multi-device percentage,
detection time. In `CLAUDE.md`, in CI, in the README, and in what you say on the slide. Claiming 50
seeds while CI runs 5 is the kind of small inconsistency an interviewer notices and then pulls on.

---

## 6. Capturing the AI log as you go

The AI Log is optional in the brief, which means almost nobody brings one. Bring one.

Cheapest method that works: **commit after each prompt**, with the prompt's intent in the message.

```
git commit -m "store: all-or-nothing N-way claim in one transaction

prompt: implement claim_all() so a job takes every resource it needs or none.
write the failing tests first — 50 threads on overlapping pairs, plus a
forced failure on the last resource of a set.

AI produced a loop of single-resource claims with a try/except that released
what it had taken. Looks equivalent to a transaction and isn't: kill the
process between resource 2 and resource 3 and those two devices are locked
with no owner and no lease to expire them. Moved the whole thing inside
BEGIN IMMEDIATE so the database does the rollback — including on crash.

Also switched the test from asyncio tasks to OS threads on separate
connections; the async version couldn't have caught the original bug because
there's no preemption point between the check and the act."
```

`git log` is then your AI log, and each message is a ready-made talking point. Three or four commits
like this beat a transcript dump — they show judgement, not just usage.

---

## 7. Demo script (rehearse this — 6 minutes)

1. **`tss watch`** — 8 benches, 20 devices, mixed capabilities, nested in the fleet view. *"This is the
   fleet. It registered itself — every device you see was reported by the machine it's plugged into,
   not read from a config file."*
2. **Submit 12 single-device jobs.** Watch them spread. *"Note bench-04 takes two of them at once —
   it has two VGs. A bench isn't one thing, and treating it as one would idle most of this hardware."*
3. **Submit a 2-device job.** Watch it wait for a bench with two matching free devices, then take both
   at once. *"All or nothing — it never sits holding one VG waiting for a second."*
4. **`kill -9` a bench running three jobs.** Count out loud: heartbeats stop… ~14 seconds… the bench
   goes OFFLINE, **all** its devices go free, and **three** jobs reappear in the queue and redistribute.
   **Do not skip the wait.** The pause is the proof.
5. **`kill -9` an idle bench too.** It also goes OFFLINE. *"Most schedulers miss this one — a bench
   someone powered off keeps looking available and eats jobs."* Ten seconds, and it shows you thought
   past the obvious case.
6. **`tss why <job_id>`** on a queued multi-device job. *"It's not stuck — it's reserving. There's a free
   VG on bench-01 that nothing else is allowed to take, because otherwise this job would never assemble
   a full set. This is the question every firmware engineer asks in Slack."*
7. **Chaos run** — 15 benches, `crasher` at p=0.3, 100 jobs, 30% multi-device, invariant checker
   streaming. End on: *zero violations across every seed.*
8. **The zombie** — replay §3.5. *"This is the failure that would have shipped a result from an
   abandoned run. One integer stops it."*

**Rehearse step 4 with a stopwatch.** Fourteen seconds of silence feels like a minute when people are
watching. Narrate the wait — say what the reaper is doing and why the TTL is four heartbeats — and it
becomes the best moment in the demo instead of the most awkward one.

**Step 6 is the one that reads as senior.** Anyone can demo "it recovered." Explaining *why a job is
correctly waiting next to a free device* shows you understood the failure mode you were preventing.
