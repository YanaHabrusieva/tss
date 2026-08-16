# Test Scheduling Service (TSS) — Architecture

**Author:** Yana
**Target:** Samsara Automation Team — Building with AI exercise
**Stack:** Python 3.12+ / FastAPI / SQLite (WAL) / httpx / Rich
**Scope:** Single-process POC, correctness-first, with a documented evolution path to 1,000 agents across global offices.
**Companion files:** `tss-system-diagram.html` (the section 5 diagram deliverable) · `CLAUDE-CODE-BRIEF.md` (build handoff)

---

## 0. The one-paragraph version

TSS is a job broker for physical hardware. A **testbed agent** is a machine; the **resources** it owns
are the firmware devices physically cabled to it. Agents register themselves and their inventory.
Firmware engineers submit jobs that declare which resources they need. TSS matches jobs to free,
compatible resources — **all of them or none** — and hands the owning agent a time-limited lease that
it renews by heartbeating. If an agent stops heartbeating, its lease expires, TSS marks it offline,
takes back every job holding any of its resources, and re-queues them. Everything else in the system
is either a view onto that loop or a way to attack it.

**The design thesis:** every failure mode here reduces to *"is this machine still alive, and who owns
these resources?"* Answer both with **one** mechanism — a presence lease on the agent plus a fencing
epoch on the job — instead of five special cases, and the chaos demo becomes easy to build and easy
to explain.

---

## 1. Requirements

### 1.1 Functional

The brief lists its core requirements as "four pillars" but renders three bullets. The fourth is
clearly **Resilience** — §1 of the brief describes testbeds going offline and connections flickering,
and the presentation section asks for a live demo of "Agents dropping and jobs being re-assigned."
Treat it as a stated requirement; say so if asked, rather than quoting the brief for something it
doesn't literally print.

| # | Pillar | What it means concretely |
|---|--------|--------------------------|
| F1 | Registration & Capability | An agent announces itself **and its device inventory**, declaring what each attached unit is (`vehicle_gateway`, `asset_gateway`). TSS learns the fleet from the agents, not from a config file. |
| F2 | Intelligent Routing | Accept jobs; assign each to **free, compatible resources**. Never double-book a device. Never start a job holding only part of what it needs. |
| F3 | Resilience *(inferred fourth pillar)* | Agents drop mid-job. Every job they were running must land somewhere else and finish. No job is lost; no result from an abandoned run is ever accepted. |
| F4 | Visibility | A human can see at a glance which devices are busy, which benches are offline — **including ones that died while idle** — and how deep the queue is. |

### 1.2 The unit of allocation — read this before anything else

**A bench is not one thing.** A single testbed machine typically has several DUTs cabled to it: two
Vehicle Gateways on different harnesses, an Asset Gateway, a spare at a different hardware revision.
Your own `device_fixtures_config.json` has exactly this shape — `mac-mini-17` holds two Gemini
fixtures.

So the model is two-level:

```
agent  bench-sf-04              ← the machine. Heartbeats. Holds the presence lease.
  ├─ resource  vg-01   {product: vehicle_gateway, hw_rev: B, harness: j1939}
  ├─ resource  vg-02   {product: vehicle_gateway, hw_rev: C, harness: obd2}
  └─ resource  ag-01   {product: asset_gateway,   hw_rev: A}
```

**Liveness is a property of the machine; allocation is a property of the device.** An agent doesn't
go "busy" — it has capacity, some of which is in use. Two jobs run side by side on one bench as long
as they need disjoint resources. Getting this wrong is expensive in an obvious way: if the unit of
allocation were the whole bench, a fleet averaging three DUTs per machine would idle roughly
two-thirds of its hardware, and the queue would stall with devices sitting free.

Two rules fall out of this, and both are load-bearing:

- **All-or-nothing.** A job that needs three devices gets all three in one transaction or none of
  them. Never a partial hold. (§3.3, and the reasoning in §7.5.)
- **Co-location.** Every resource for one job comes from a **single agent**.

**Be precise about which rule does what — this gets probed.** All-or-nothing is the deadlock
prevention. **Co-location is not.** Deadlock arises from partial holds, not from bench boundaries:
every resource is a row in one table in one database, so a claim spanning five benches is the same
single local transaction as a claim on one, and is equally deadlock-free. **Cross-bench allocation
would be safe.** There is no distributed commit to avoid — do not claim there is.

Co-location is justified on four other grounds, and the first is the one that actually decides it:

1. **Physics.** In a multi-device HIL test the devices are usually cabled *to each other* — a Vehicle
   Gateway talking to an Asset Gateway over a harness, a lock wired to its J-Link on the same USB bus.
   You cannot run that test with one device in San Francisco and one in Amsterdam. This is a domain
   constraint, and it makes the remaining three largely academic.
2. **Failure surface compounds.** A job spanning *k* benches dies if *any* of the *k* dies. At a 30%
   per-bench crash rate a two-bench job is ~51% likely to be killed versus 30% — manufacturing infra
   failures for no benefit.
3. **Agent-side execution is the hard part.** Two daemons co-executing one test need a sync protocol:
   who runs the test logic, how they barrier, what happens when one finishes first. That is a whole
   subsystem, and it lives in the *agent*, not the scheduler.
4. **Scheduling combinatorics.** Matching N devices within one bench is a scan. Matching N devices
   across M benches is a set-cover search, and reservation (§3.4.1) goes from "pick one feasible
   target" to "pick one feasible *combination*."

**Treat it as policy, not an assumption baked into the plumbing.** Implement co-location as a single
predicate in the matcher (`all candidate resources share an agent_id`) rather than something threaded
through the claim, the reaper, and the reservation logic. If Samsara turns out to need a gateway on one
rig talking to a gateway on another, relaxing it is then a flag plus two changes — requeue if *any* of
the job's presence leases expires, and extend feasibility to bench combinations — instead of a redesign.

### 1.3 Non-functional

| Property | POC target | Why this number |
|---|---|---|
| Fleet size | 10–20 agents, ~40 resources | The demo runs ~15 mock agents with 2–4 devices each |
| Dispatch latency | < 1s from "resource freed" to "agent has job" | Long-poll, not interval polling. Below human perception in the demo. |
| Failure detection | ≤ 14s from agent death to requeue | `PRESENCE_TTL` (12s) + `REAPER_INTERVAL` (2s). §7.1 — this number is quoted in the demo, so it must match. |
| Utilization | A bench with a free compatible device is never idle while a matching job waits | The whole point of §1.2 |
| Durability | Survives TSS restart with queue and allocations intact | SQLite WAL |
| Correctness | 9 invariants, never violated under chaos | §3.8 |

### 1.4 Constraints and non-goals

- **Constraint:** a few days of build time. Every component must be demoable, not just designed.
- **Constraint:** agents live behind office NAT. TSS cannot reliably open a connection *into* a testbed.
- **Non-goal:** actually running firmware tests. Agents execute a simulated workload.
- **Non-goal:** jobs spanning multiple benches. Excluded by the co-location rule (§1.2), deliberately.
- **Non-goal:** authn/authz, multi-tenancy, artifact storage. Named as future work, not built.
- **Non-goal:** exactly-once *execution*. Impossible over an unreliable network. §7.4 states what we
  guarantee instead (exactly-once **result**) and why it's the right trade for HIL.

---

## 2. Component map

The rendered version — the actual section 5 deliverable — is `tss-system-diagram.html`, which also
shows the event/WebSocket return path and the chaos sequence.

```
                        FIRMWARE ENGINEER
                               │
                  ┌────────────┴────────────┐
             tss CLI / TUI            CI (GitHub Actions)
                  └────────────┬────────────┘
                               │  HTTP  ↓        WebSocket  ↑
        ╔══════════════════════▼══════════════════════════════╗
        ║                    TSS CORE                          ║
        ║   ┌──────────┐   ┌───────────┐   ┌───────────────┐  ║
        ║   │   API    │──▶│ SCHEDULER │──▶│  STATE STORE  │  ║
        ║   │ (FastAPI)│   │ N-way     │   │ (SQLite + WAL)│  ║
        ║   │          │   │ matcher   │   │ all-or-nothing│  ║
        ║   └────▲─────┘   └───────────┘   └───────▲───────┘  ║
        ║        │                                  │          ║
        ║   ┌────┴─────┐   ┌───────────────────┐   │          ║
        ║   │ EVENT BUS│◀──│      REAPER       │───┘          ║
        ║   │ (pub/sub)│   │ presence + timeout│              ║
        ║   └──────────┘   └───────────────────┘              ║
        ╚══════════════════════▲══════════════════════════════╝
                               │  agents PULL (long-poll heartbeat)
        ┌──────────────┬───────┴────────┬──────────────┐
   ┌────▼──────┐  ┌────▼──────┐  ┌──────▼─────┐  ┌─────▼──────┐
   │bench-sf-01│  │bench-sf-02│  │bench-ams-03│  │  mock-11   │
   │ vg-01 BUSY│  │ ag-01 FREE│  │ vg-01 BUSY │  │ vg-01 BUSY │
   │ vg-02 FREE│  │ ag-02 BUSY│  │ vg-02 BUSY │  │ vg-02 FREE │
   │ ag-01 FREE│  │           │  │ ag-01 FREE │  │ chaos:     │
   │           │  │           │  │            │  │ crasher    │
   └───────────┘  └───────────┘  └────────────┘  └────────────┘
       real testbeds — one agent, several DUTs      CHAOS FLEET
```

---

## 3. What each part is, and why it exists

### 3.1 Agent (testbed daemon)

**What it is.** A small Python process on each bench machine, next to the hardware. It is the only
thing that knows which devices are physically cabled to this box, and on which harness.

**What it does.**

1. **Registers on boot, with its inventory** — its own ID, plus one entry per attached device with
   that device's capability tags. Inventory is *pushed*, never read from a central file.
2. **Heartbeats on a fixed interval, always** — regardless of how many of its devices are in use.
   This is both "I'm alive" and, when it has spare capacity, "give me work."
3. **Runs several jobs concurrently**, one per resource set, up to its capacity.
4. **Re-registers** when TSS tells it to (`404 unknown_agent` or `410 presence_expired`).

**Why pull, not push.** The obvious design is for TSS to push jobs to an agent's HTTP endpoint. Do not
do this. Testbeds sit inside office networks behind NAT and firewalls; a service in one region cannot
reliably dial into a bench in another. Pull inverts that — the agent always opens the connection
outbound, which works through NAT with no network engineering at all.

Pull has a second, subtler benefit: an agent only asks for work when it is *genuinely* ready. A bench
whose J-Link is unplugged simply reports that resource as unhealthy and stops being offered work for
it. Readiness becomes self-reported rather than inferred, and one whole class of "TSS thinks the
bench is fine but it isn't" bugs disappears.

> **Contrast with MeteorShower:** there the orchestrator *discovers* devices by polling
> `adb`/`idevicelist` on its own host, and reads fixture inventory from a central
> `device_fixtures_config.json` keyed by `runner_name`. That works because orchestrator and devices
> share a machine. It cannot work across offices, and nobody hand-maintains a JSON file listing 1,000
> benches. **Inverting to agent-pushed inventory is the single most important change between the two
> systems** — and it is the same data, just sourced from the machine that actually knows it.

**The long-poll heartbeat.** Plain 3-second heartbeats mean up to 3 seconds of dead air between "a
device frees up" and "the agent gets the next job." Instead: an agent **with spare capacity** blocks
its heartbeat server-side for up to `LONGPOLL_TIMEOUT`, returning the instant the scheduler assigns
it something. An agent at full capacity gets an immediate reply — it just needs to renew presence.
One endpoint, one connection per agent, sub-second dispatch. Same trick as MeteorShower's
`wait_for_allocation`, moved to the agent side.

### 3.2 API layer (FastAPI)

**What it is.** The HTTP/WebSocket surface, with two audiences on separate routers: agents
(`api/agent.py`) and humans-and-CI (`api/client.py`).

**Why they're split.** Agent endpoints are high-frequency and machine-to-machine, and will need their
own auth and rate limits. Client endpoints are low-frequency and human-facing. Splitting now costs
nothing and turns the eventual split into two services into a config change rather than a refactor.

**Why it holds no logic.** Handlers validate input, call the scheduler or store, and serialize the
result. Every scheduling decision lives in `Scheduler`; **every write lives in `Store`** — the
scheduler decides *which resources* to claim and calls `store.claim_all(...)` to do it. That boundary
is what makes the scheduler unit-testable without an HTTP client or a database.

### 3.3 State store (SQLite + WAL)

**What it is.** The single source of truth: agents, resources, jobs, allocations, event history. Every
write goes through here.

**Why SQLite and not a dict.** Durability, first — restart TSS and the fleet is still there. But the
real reason: **a transaction is how you claim N devices atomically**, which is both the race the brief
warns about and the deadlock in §7.5. An in-memory dict guarded by `asyncio.Lock` also works in one
process, but it is a dead end: the moment you run two TSS replicas the lock protects nothing. Putting
the claim in the database from day one makes scaling out a swap rather than a rewrite.

**Pragmas, set once at startup.**

```sql
PRAGMA journal_mode = WAL;      -- readers don't block on the scheduler's writes
PRAGMA busy_timeout = 5000;     -- wait for a contended write lock instead of failing instantly
PRAGMA foreign_keys = ON;       -- otherwise the FKs in §5 are decorative
PRAGMA synchronous = NORMAL;    -- safe under WAL; full fsync per commit isn't worth it here
```

**The clock — decide this once and write it down.** `presence_expires_at` and all timestamps are
**absolute unix floats in UTC wall-clock time**. They must be, because §7.2's zero-code restart
recovery depends on an expiry that survives process death, and `time.monotonic()` resets on restart
so it cannot back a persisted column. `time.monotonic()` is used only for *measuring durations within
a process*. Put this in `CLAUDE.md` and in the store's docstring: mixing the two is a bug an AI will
introduce cheerfully, and it won't show up until an NTP step or a restart.

**The critical operation — the N-way all-or-nothing claim.**

```sql
BEGIN IMMEDIATE;                       -- take the write lock up front; no upgrade, no deadlock

-- Once per resource the matcher selected, ISSUED IN SORTED ORDER BY resource_id.
-- All of them are on the SAME agent (§1.2). See the lock-ordering note below.
UPDATE resources
   SET state            = 'busy',
       current_job_id   = :job_id,
       last_assigned_at = :now
 WHERE id = :resource_id
   AND state = 'free'                  -- ← the guard
   AND agent_id = :agent_id;
-- rowcount MUST be 1. If ANY resource returns 0, ROLLBACK the entire transaction —
-- including the resources already updated in this same transaction. All or nothing.

UPDATE jobs
   SET state        = 'assigned',
       agent_id     = :agent_id,
       epoch        = epoch + 1,       -- self-incrementing; never passed in from outside
       attempt      = attempt + 1,
       tried_agents = json_insert(tried_agents, '$[#]', :agent_id),
       assigned_at  = :now
 WHERE id = :job_id
   AND state = 'queued';               -- ← the second guard, equally necessary
-- rowcount == 0 → the job was cancelled or claimed since we read it. ROLLBACK.

INSERT INTO job_resources (job_id, resource_id, epoch, claimed_at)
     VALUES (:job_id, :resource_id, :new_epoch, :now);   -- once per resource; the durable record

COMMIT;
-- return the new jobs.epoch; it goes into the assignment the agent receives.
```

**Every rowcount check is load-bearing.** Guarding only some of the resources leaves a job holding a
device another job already owns — the exact double-booking the whole design exists to prevent. And
rolling back *all* of them on any single failure is what makes hold-and-wait deadlock structurally
impossible (§7.5). A transaction is the natural home for this: SQLite gives you the rollback for free,
whereas hand-rolling "undo the three I already took" is where this goes wrong.

**Sort the resource IDs before issuing the UPDATEs.** Two concurrent transactions that lock the same
rows in *different* orders can deadlock at the database level — the classic lock-ordering cycle. SQLite
hides this completely: `BEGIN IMMEDIATE` serializes writers, so one transaction simply waits and you
will never see it in the POC. **Postgres at Stage 2 does not hide it** — it detects the cycle and
aborts one transaction with a deadlock error. Issuing the updates in a deterministic order (sorted by
`resource_id`) makes a cycle impossible, costs one `sorted()` call, and is invisible to test until the
day it isn't. This is the single most likely way the claim breaks when you scale out, and it is
unrelated to which benches the devices are on.

**The epoch is self-incrementing.** `epoch = epoch + 1` inside the statement, never a value computed
in Python and passed in. A read-then-write epoch is itself a race — which would be a bitterly funny
bug to ship in the mechanism designed to prevent races.

**One honest caveat about SQLite.** With `BEGIN IMMEDIATE`, a *second connection* contending for the
write lock does not get `rowcount = 0` — it blocks for `busy_timeout` and then raises `SQLITE_BUSY`.
The store must catch `sqlite3.OperationalError` on the claim path and treat it exactly like a lost
race. SQLite serializes writers by construction; the rowcount guard defends against two *logical*
passes racing, and `SQLITE_BUSY` handling defends against two *connections*. You need both, and saying
this out loud is much stronger than claiming the transaction handles everything.

> This is MeteorShower's `atomic_allocate_all` — same problem, same shape. Worth saying plainly in the
> presentation: *"I've shipped the partial-allocation bug before, so I designed for it before writing
> a line of scheduler code."*

### 3.4 Scheduler (the matcher)

**What it is.** The decision layer: given queued jobs and free resources, decide which resource set
goes to which job, then ask the store to commit it. It performs no writes itself.

**How it decides.**

1. Wake on either event — a job was submitted, or a resource became free — plus a 1s backstop tick.
2. Compute **reservations** first (see below).
3. Walk the queue oldest-first. For each job, for each **online, non-draining, non-quarantined**
   agent: try to satisfy every one of the job's requirement specs from that agent's free,
   non-reserved, healthy resources — each spec consuming a distinct resource.
4. Among agents that can satisfy the whole job, prefer the one whose matched resources have the
   oldest `last_assigned_at` (**LRU**).
5. Attempt the N-way claim. On any rowcount-0 or `SQLITE_BUSY`, roll back and try the next agent.
6. Stop when the queue empties or no agent can satisfy the head of the queue.

**Capability matching is a subset test, per resource.** A requirement spec matches a resource if the
resource satisfies every key in the spec:

```json
// job requirements — a LIST, one entry per device needed
[{"product": "vehicle_gateway", "harness": "j1939"},
 {"product": "asset_gateway"}]
```

`{"product": "vehicle_gateway"}` matches a `j1939` VG; adding `{"harness": "obd2"}` does not. Modelling
requirements as a list of tag-subsets rather than a product string is what lets the system express
*"needs a heavy-duty VG **and** an AG on the same bench"* — which is what a real gateway-to-gateway
test actually needs. MeteorShower already needs platform **and** fixture product **and** bezel colour;
a flat string was never going to hold.

**Why LRU and not first-fit.** First-fit sends every job to the same device while its siblings idle.
That unit's hardware wears out first, and — worse — if it is subtly broken, every job fails and you
conclude the *firmware* is broken. LRU spreads load and makes a single bad device show up as "one
resource failing" rather than "everything failing." It needs one column,
`resources.last_assigned_at`, written by the claim in §3.3.

### 3.4.1 Starvation and reservation — where multi-device gets dangerous

A job needing three free VGs on one bench can wait forever while a stream of single-VG jobs nibbles
capacity the instant it frees. Without a guard, big jobs never run on a busy fleet.

**Reserve is not claim. This distinction carries the entire safety argument.**

| | Claim (§3.3) | Reserve |
|---|---|---|
| Resource state in the DB | `busy` | **stays `free`** |
| Owner | the job | **nobody** |
| Job state | `assigned` / `running` | still `QUEUED` |
| Where it lives | one SQL transaction, durable | scheduler memory, recomputed every pass |
| Survives a TSS restart | yes | no — and doesn't need to |
| Can deadlock | — | **no** |

A job that needs three devices and can see one free **does not take it**. It takes nothing. The
scheduler merely declines to offer that free device to anyone else while the starving job waits.

**Why this is not the deadlock of §7.5.** Deadlock requires *hold*-and-wait. Reservation is
wait-**without**-hold: no job owns anything, so no cycle can form. And because reservations are
recomputed from scratch each pass, a TSS crash mid-wait leaves nothing to clean up — compare a partial
claim, which strands devices with no owner and no lease to expire them.

**The algorithm — three steps, and step 1 is the one that's easy to miss.**

> Once the oldest queued job has waited past `STARVATION_THRESHOLD` and cannot be satisfied, it becomes
> **the reserving job**. Then, each pass:
>
> **1. Feasibility filter.** Consider only agents whose *total healthy inventory* could satisfy the job
> **ignoring current free/busy state**. A bench with two healthy VGs can never run a 3-VG job; reserving
> there idles a device forever, for nothing.
>
> **2. Pick exactly one target agent** among the feasible ones — the one closest to satisfying the job
> (most matching resources already free; tie-break on fewest busy). Reservation is single-bench because
> **allocation is single-bench** (co-location, §1.2): reserving one device on bench-01 and another on
> bench-02 idles two devices to assemble a set that can never be assembled.
>
> **3. Reserve only on the target.** Free matching resources on that agent are withheld from other jobs.
> Everything on every other bench keeps flowing normally.
>
> **At most one job reserves at a time** — always the oldest starving one. The target is recomputed each
> pass, so it follows the fleet: if another bench frees a full set first, the job takes that instead.
> Reservation never prevents a job from claiming a set it can actually get.

**The single-reserver rule is the other half of the safety argument.** Two jobs each reserving partial
sets is a deadlock you built yourself — in bookkeeping rather than in hardware, but with the same
outcome: neither ever proceeds. One reserver can always eventually be satisfied, because nothing else
is permitted to take what it is waiting for.

**When no agent is feasible — the case that looks like a bug and isn't.** If step 1 eliminates every
agent, the job **cannot run on this fleet as it currently stands**: nobody has three j1939 VGs, or the
only bench that did is quarantined. Do not reserve — there is nothing to reserve *toward*. Instead:

- annotate the job `blocked_reason = 'no_capable_agent'` and emit `job.unsatisfiable` once,
- surface it at the top of `tss queue` and in `tss why` (§3.9),
- keep it **queued**, because fleets change — a bench gets repaired, un-quarantined, or added,
- dead-letter it only after `UNSATISFIABLE_TIMEOUT` (30 min) with `result_detail='no_capable_agent'`.

Silently reserving forever for an impossible job is the failure mode here, and from the outside it is
indistinguishable from a broken scheduler. Detecting it explicitly costs one inventory query and turns
"the scheduler is stuck" into "you asked for hardware we don't have."

**The honest cost of reservation: idle devices.** A reserved free device does nothing while the job
waits for its siblings — bounded by the running jobs' `max_duration_s`, but real. The alternative is a
job that never runs at all, which is worse. The standard refinement, and a strong *"what's next"*
answer, is **backfill**: let a short job use a reserved device if it will finish before the reserving
job could possibly start. Every HPC scheduler does this (Slurm calls it exactly that). It needs
historical duration data the POC doesn't have, which is precisely why §13.6 lists duration history as
the prerequisite — worth naming as the next step rather than hand-waving it.

> **This is MeteorShower's `MULTI_DEVICE_PRIORITY_TIMEOUT`, generalized and made safe.** That code
> blocks *all* single-device allocation while a multi-device test starves — correct, but coarse: it
> idles benches the starving test could never have used, which is exactly what the feasibility filter
> above prevents. Being able to say "I ran the coarse version in production, here's what it cost, here's
> the refinement" is a much better answer than presenting either version cold.

### 3.5 Presence leases and the reaper

**This is the heart of the resilience story. Read this section twice.**

**One lease, per agent, always.** Every registered agent — idle, partially loaded, or full — holds a
**presence lease**: `presence_expires_at`, pushed forward by every heartbeat. Resources do not have
their own leases; they are reachable exactly as long as the machine holding them is. Job ownership is
fenced separately by `jobs.epoch`.

> **Why presence lives on the agent and covers idle machines.** An earlier draft gave leases only to
> *busy* agents. That leaves a hole big enough to fail Pillar 4: a bench unplugged **while idle** stays
> eligible forever, keeps getting handed jobs, and burns a full TTL on each. Making presence universal
> and machine-level closes it with *fewer* moving parts, and keeps the "one mechanism" claim true.

**What the reaper is.** A background coroutine that wakes every `REAPER_INTERVAL` and runs two sweeps.

*Sweep 1 — dead agents (with fan-out):*

```sql
SELECT id FROM agents WHERE presence_expires_at < :now AND state != 'offline';
-- then, per dead agent:
SELECT DISTINCT current_job_id FROM resources
 WHERE agent_id = :agent AND current_job_id IS NOT NULL;
```

Mark the agent `offline`; free every one of its resources; and requeue **each distinct job** it was
running — `state='queued'`, `agent_id=NULL`, `epoch = epoch + 1` — or dead-letter jobs that have now
failed on `MAX_DISTINCT_AGENTS` distinct benches. Emit `agent.offline` and one `job.requeued` per job.
Poke the scheduler.

> **`DISTINCT` is not decoration.** A bench running one job across three of its devices appears three
> times if you iterate resources. Requeue it per-resource and you bump the epoch three times, append
> the agent to `tried_agents` three times, and burn the job's entire retry budget on a single bench
> failure. This is the most likely bug in the whole fan-out and it is completely silent — the job just
> mysteriously dead-letters early. Dedupe by job, then requeue once.

*Sweep 2 — hung jobs:*

```sql
SELECT id, agent_id FROM jobs
 WHERE state = 'running' AND :now > started_at + max_duration_s;
```

The agent is alive and heartbeating; the job is not finishing. Queue a `cancel_job` directive, bump
the epoch (so its eventual report is fenced out), free its resources, and record
`outcome='infra_error', result_detail='timeout'`. Retry once, then dead-letter.

**Both sweeps live in the reaper.** An earlier draft named `max_duration_s` as the "detector" of hung
jobs — but a column detects nothing. If no loop scans it, the `hung` chaos profile has nothing to
prove and devices stay locked forever.

**Why leases instead of an "agent is alive" flag.** A flag requires someone to *decide* liveness, and
every such decision is a special case: what if the agent is slow? what if the network blipped? what if
TSS restarted? A lease has no opinion. Time passes, it expires. On TSS restart the reaper's first tick
sweeps every stale lease automatically — **no dedicated recovery code path at all.** That is the
payoff, and it is why the expiry column must be wall-clock.

**Heartbeat renewal must be guarded.** Renewal looks trivial and is not:

```sql
UPDATE agents
   SET presence_expires_at = :now + :ttl, last_heartbeat_at = :now
 WHERE id = :agent_id
   AND state != 'offline';          -- ← a reaped agent must NOT renew itself back to life
```

Without that guard, a renewal arriving microseconds after the reaper ran resurrects a dead lease: the
agent believes it still owns resources that have been freed and reassigned. Instead TSS returns
`410 presence_expired`, the agent re-registers, and comes back clean as a fresh bench with all
resources free. **It does not get its jobs back.**

**Epochs, and the zombie bug.** Here is the failure AI will not design for and an interviewer will
absolutely probe:

```
t=0    Agent-7 gets job J on resources vg-01 + ag-01, epoch 4. Starts running it.
t=3    Agent-7's network drops. It keeps running — it doesn't know it's isolated.
t=12   Presence lease expires. Reaper frees both resources, requeues J, epoch → 5.
t=14   Agent-3 gets J at epoch 5 on its own devices. Starts running it.
t=40   Agent-7's network returns. It reports: "job J complete, PASSED."
```

Without epochs, TSS records J as passed — and a firmware engineer ships on a result from a run that
was abandoned. **Every completion report carries the epoch the agent was issued**, and TSS rejects any
report whose epoch is stale:

```python
if report.epoch != job.epoch:
    return 409, {"error": "stale_epoch", "action": "abandon_job"}
```

Agent-7 gets a `409`, abandons the job, releases its hardware locally, and goes back to having spare
capacity. This is a **fencing token** — the same mechanism distributed locks use — and it is the most
sophisticated thing in the design. Build a `zombie` chaos profile specifically to prove it.

**The epoch also protects terminal states.** Cancelling a running job bumps the epoch too (§6), which
is what stops the agent's late "PASSED" from overwriting `CANCELLED`. The rule: **any transition that
ends or reassigns ownership bumps the epoch.** Easier to remember than a list of cases.

**Two different timeouts — do not conflate them.**

| | Presence timeout (12s) | Job timeout (per-job, default 600s) |
|---|---|---|
| Detects | The **machine** died | The **job** hung |
| Scope | Every job on that bench | One job |
| Symptom | Heartbeats stopped | Heartbeats fine, job never completes |
| Response | Free all resources, requeue each distinct job | Cancel on the agent; `infra_error:timeout` |
| Retry? | Yes, until `MAX_DISTINCT_AGENTS` benches tried | Once, then dead-letter |

Collapsing these is the most common design mistake here. A test that legitimately takes 20 minutes
must not look like a dead bench, and a bench whose power supply died must not get 20 minutes of grace.

### 3.6 Event bus

**What it is.** An in-process pub/sub. Reaper, scheduler, and store publish; the WebSocket endpoint
and TUI subscribe.

**Why it exists.** Without it the fleet view polls `GET /v1/fleet` every second — laggy, and at 1,000
agents genuinely expensive. With it, the dashboard is push-driven and updates the instant something
happens. The demo is dramatically better when killing a bench produces an *immediate* visible change —
three resources going free and two jobs re-queueing at once — rather than one that appears on the next
poll.

**Durable log and live stream are written together.** The append-only `events` insert happens **inside
the same transaction as the state change it records**; publication to subscribers happens after
commit. Otherwise a crash between the two leaves the audit log and the live stream telling different
stories — and the audit log is what you use to answer "why did my job move?" after the fact.

**Why it's an abstraction over `asyncio.Queue`.** At Stage 2 (§9) it becomes Redis pub/sub or NATS.
Keep the interface to `publish(event)` / `subscribe() -> AsyncIterator[Event]` and the call sites never
change.

### 3.7 Chaos simulator

**What it is.** A launcher that spins up N mock agents — each with an inventory and a **failure
profile** — plus a job generator that submits a mix of single- and multi-device jobs.

```bash
just chaos      # 15 agents × 2-4 devices, 100 jobs (30% multi-device), 5 seeds — the CI gate
python -m tss.chaos --agents 15 --jobs 100 --multi-pct 30 --profile mixed --seed 42
```

**The profiles — each targets a specific design decision.**

| Profile | Behavior | What it proves |
|---|---|---|
| `clean` | Always works | Baseline; the happy path |
| `crasher` | Whole machine dies mid-job, p = 0.3 | Presence expiry → **fan-out** requeue of every job on the bench |
| `flaky_network` | Drops 30% of heartbeats | Presence TTL tolerates transient loss (§7.1 has the arithmetic) |
| `zombie` | Goes silent past expiry, returns claiming completion | **Epoch fencing** rejects the stale report |
| `slowpoke` | Jobs take 3–10× expected | Slow ≠ dead; renewal keeps it alive |
| `hung` | Heartbeats forever, never completes | **Job-timeout sweep** fires independently of presence |
| `idle_death` | Registers, heartbeats, vanishes **without ever taking a job** | Idle agents are reaped too — the gap most designs have |
| `resource_flap` | One device goes unhealthy while the machine stays alive | **Resource-level vs agent-level** health are different things |
| `liar` | Declares capabilities its devices don't have | Graceful failure, not a crash; `infra_error`, then quarantine |
| `flapper` | Registers/deregisters every few seconds | Registration idempotent; no duplicate resources; no orphaned jobs |

**Plus a load shape, not a profile: `--multi-pct`.** A fleet running only single-device jobs never
exercises the N-way claim, the reservation logic, or the fan-out. Thirty percent multi-device jobs is
what makes contention real, and it is the setting under which I8 and the starvation guard actually get
tested.

**Why `--seed`.** Reproducible chaos. When a run finds an invariant violation you need to replay that
exact run to debug it. A chaos suite you can't replay is a chaos suite you can't fix. Log the seed on
every run, including in CI.

**Why this is the highest-leverage thing to build.** It is the demo. It is also the only way to find
the concurrency bugs, because they are timing-dependent and will not show up in hand-testing. Build it
early — not as a final flourish.

### 3.8 Invariant checker

**What it is.** A watchdog that runs alongside a chaos run and asserts safety properties continuously,
plus a liveness check at the end. This is what turns "watch it recover" into "watch it *provably* not
break."

**It reads two sources.** TSS's database, *and* the mock agents' ground-truth execution state (exposed
by the chaos harness). Several properties are unverifiable from the database alone — TSS's record of a
resource's capabilities is exactly what the `liar` claimed, and TSS's record of ownership can never
show two owners because the reaper clears the dead one. Checking TSS against itself proves nothing.

| # | Invariant | Kind | Checked against |
|---|---|---|---|
| I1 | At most one agent **believes it owns** a job at any moment, and exactly one result is ever accepted per job | safety | agent ground truth + `jobs.epoch` |
| I2 | No **resource** is held by two jobs | safety | DB — structural, see below |
| I3 | Every submitted job reaches a terminal state within the run deadline | **liveness** | DB, end of run |
| I4 | A job never runs on resources lacking its required capabilities | safety | **agent ground truth**, not TSS's copy |
| I5 | An `offline` agent's resources are all free, and hold no job | safety | DB |
| I6 | A hung job is terminated by the job-timeout sweep, not by presence expiry | safety | DB + timing |
| I7 | A terminal job's outcome is never overwritten | safety | DB, event log |
| **I8** | **A job in `assigned`/`running` holds exactly as many resources as it required — never fewer, never more** | safety | DB |
| **I9** | **At most one job reserves at a time, all its reservations are on a single feasible agent, and no reserved resource is claimed by another job** | safety | scheduler state |

**I8 is the one this whole revision exists for.** It is the machine-checkable form of "never a partial
allocation." Any bug in the N-way claim — a missing rollback, a rowcount not checked, a resource freed
early — shows up here immediately, and nowhere else.

**On I1 and honest wording.** An earlier draft said "no job is *running* on two live agents at once,"
which §7.4 openly contradicts and which the zombie scenario deliberately violates (agent-7 and agent-3
both execute J between t=14 and t=40). Fencing buys single-**result**, not single-**execution**. State
it that way. An invariant your own demo breaks is worse than no invariant.

**On I2 and structural enforcement.** `resources.current_job_id` is a single nullable column, so a
resource physically cannot reference two jobs — I2 is structural by construction, and the
`WHERE state='free'` guard is what keeps it honest. The checker verifies the column and guard exist
rather than hoping. *"I made that one structural"* is a much better answer than *"I have a test for it."*

**On I9 and self-inflicted deadlock.** Every clause is a bug someone would otherwise ship. *One
reserver* — two reservers holding partial sets deadlock in bookkeeping. *Single feasible agent* —
reservations spread across benches idle devices toward a set that can never be assembled. *Not claimed
by another job* — the reservation is only real if the claim path actually respects it. Unlike the
others this one is checked against **scheduler state**, not the database, because reservations
deliberately leave no database trace (§3.4.1).

**On I3 and liveness.** It cannot be asserted continuously — an in-flight job legitimately has no
terminal state yet. It is a completion check with a deadline, run at the end, with `DEAD_LETTER` and
`CANCELLED` counting as terminal.

**Why this matters more than it looks.** "I ran chaos and it seemed fine" is an anecdote. "Here are
nine properties, 100 jobs, 30% of them multi-device, a 30% crash rate, zero violations across every
seed CI runs" is engineering. It also directly answers *"how did you know when it was good enough?"* —
the answer is a threshold, not a feeling.

### 3.9 Visibility surfaces

Three surfaces, one data source. Note the fleet view is now two-level:

```
$ tss fleet
BENCH          STATE     BEAT   RESOURCES
bench-sf-01    online     1s    vg-01 BUSY job-8f21 · vg-02 free · ag-01 free      1/3
bench-sf-02    online     2s    ag-01 free · ag-02 BUSY job-8f30                   1/2
bench-ams-03   OFFLINE   15s    (2 jobs requeued)                                  —
bench-ams-04   online     0s    vg-01 BUSY job-8f44 · vg-02 BUSY job-8f44          2/2
                                                     ↑ one job, two devices
```

- **`tss fleet` / `tss queue`** — one-shot CLI; the thing you paste into Slack.
- **`tss watch`** — live TUI (Rich): benches with their resources nested, queue with wait times,
  scrolling event log. This is what's on screen during the demo.
- **`GET /v1/fleet` + `WS /v1/events`** — the API, for CI and any future dashboard.
- **Operator verbs** the state machines require: `tss drain <agent>`, `tss unquarantine <agent|resource>`,
  `tss cancel <job>`.

**The customer feature: `tss why <job_id>`.** Nobody builds this and every firmware engineer wants it.
With multi-device jobs it becomes far more valuable, because "why am I waiting" now has a genuinely
non-obvious answer:

```
job-8f3a  QUEUED 4m12s  — RESERVING on bench-sf-01 (starved 3m12s)
  needs: 3 × {product=vehicle_gateway, harness=j1939}  on ONE bench
  feasible benches (enough healthy j1939 VGs to ever satisfy this):
    bench-sf-01   vg-01 BUSY (job-8f21, 3m elapsed / 10m budget)
                  vg-02 free — RESERVED for you
                  vg-03 free — RESERVED for you          ← target: 2 of 3 held for you
    bench-ams-03  vg-01 BUSY · vg-02 BUSY · vg-03 BUSY (job-8f44, 1m / 10m)
  not feasible:
    bench-sf-04   only 2 healthy j1939 VGs — can never satisfy a 3-device job
    bench-sf-09   QUARANTINED since 14:02 (3 consecutive failures)
  waiting on: vg-01 to free on bench-sf-01  (~7m of budget left)
  nothing else can take vg-02 or vg-03 while you wait
```

And the case that would otherwise look like a stuck scheduler:

```
job-9c11  QUEUED 6m40s  — UNSATISFIABLE (no capable bench)
  needs: 3 × {product=vehicle_gateway, harness=j1939}  on ONE bench
  no bench in the fleet has 3 healthy j1939 VGs.
  closest:  bench-sf-01  has 3, but vg-03 is UNHEALTHY since 13:51
            bench-sf-09  has 3, but the bench is QUARANTINED since 14:02
  not reserving — there is nothing to reserve toward.
  will dead-letter at 15:12 unless the fleet changes.
```

**Show only what you actually know.** Elapsed-versus-budget is honest; a confident "est. start ~3m" is
a lie unless you're tracking historical durations, which the POC isn't. Quoting a number you can't
support is exactly the kind of thing that gets probed — and "I show elapsed and budget because I don't
have the history to estimate properly yet" is a better answer than a fabricated ETA.

**Why it's worth the hour it takes.** "Why is my test stuck?" is the number-one support question for
any shared-hardware system, and the default answer is a Slack message to whoever owns the fleet.
Answering it in the tool removes an entire category of interruption. On a multi-device fleet it also
removes a category of *false bug report* — "the scheduler is broken, there are free VGs and my job
isn't running" is, nine times out of ten, the reservation logic working correctly, and this is what
shows that.

---

## 4. State machines

Splitting agent from resource gives two small machines instead of one overloaded one.

### 4.1 Agent — liveness and administration

```
                 register
      (none) ──────────────► ONLINE ◄──────────────────────┐
                              │  ▲                          │
       presence expires       │  │  re-register             │
       (idle or loaded)       │  └──────────────────────────┤
                              ▼                             │
                           OFFLINE ─────────────────────────┘
                              ▲
                              │ last job done
      failures spanning    DRAINING ◄── drain requested ── ONLINE
      multiple resources      │
   ONLINE ──────► QUARANTINED ┘──► ONLINE  (tss unquarantine, or re-register with a new version)
```

- **ONLINE** — heartbeating. Has capacity if any of its resources are free. There is deliberately no
  `busy` state: capacity is counted, not flagged.
- **OFFLINE** — presence lease expired. All resources freed; every job it held requeued.
- **QUARANTINED** — the *machine* is suspect: failures spanning **multiple different resources**.
  Nothing on it is scheduled.
- **DRAINING** — finish current jobs, accept no more, then go offline. Needed for deploys; without it,
  upgrading an agent means killing running tests.

### 4.2 Resource — allocation and device health

```
              register                claim (N-way, all-or-nothing)
   (none) ──────────────► FREE ◄──────────────────────► BUSY
                            │  ▲                          │
                            │  │  release / job done      │
                            │  └──────────────────────────┘
        agent reports       │
        probe failure       ▼
                        UNHEALTHY ──(probe passes / unquarantine)──► FREE

   (agent goes OFFLINE → every one of its resources returns to FREE, whatever its state)
```

- **FREE** — healthy, unclaimed, offerable.
- **BUSY** — claimed by exactly one job.
- **UNHEALTHY** — this specific device is bad (J-Link dropped, DUT unresponsive) while the machine is
  fine. Skipped by the matcher; the bench keeps working on its other devices.

**Why device-level health matters.** One dead J-Link on a three-device bench should cost you one
device, not the bench. Conflating them is how you lose a third of your fleet to a single unplugged
cable. It also changes the diagnosis: failures clustered on **one resource** mean a bad device;
failures spanning **several resources on one machine** mean a bad machine. That attribution rule is
what decides whether you quarantine the resource or the agent — and it's a genuinely useful thing to
be able to say you thought about.

**Every non-automatic transition needs a control surface.** Quarantine clears via
`POST /v1/agents/{id}/unquarantine` (or the resource equivalent), or when the agent re-registers
reporting a **new version** — a restarted-but-unfixed bench should stay quarantined, or you just
re-break the fleet. A state with no way out is a slow fleet-drain dressed up as a health feature.

### 4.3 Job

```
                    submit
          (none) ──────────► QUEUED ◄──────────────────────────┐
                              │                                │
      N-way atomic claim      │                    presence expired, or
      (all resources or none) │                    INFRA_ERROR reported
                              ▼                    (fewer than 3 distinct
                          ASSIGNED                  agents tried)
                              │ agent starts                   │
                              ▼                                │
                           RUNNING ─────────────────────────────
                              │
        ┌─────────────────────┼─────────────────────┬───────────────┐
        ▼                     ▼                     ▼               ▼
     PASSED                FAILED             INFRA_ERROR       CANCELLED
   (terminal)            (terminal —                │            (terminal)
                        a real result,              │  3 distinct agents tried
                     never auto-retried)            ▼
                                              DEAD_LETTER  (poison job)
```

**`FAILED` vs `INFRA_ERROR` is the most important distinction in the whole data model.**

- `FAILED` — the test ran to completion and the firmware did not do what it should. **The engineer's
  problem.**
- `INFRA_ERROR` — the bench died, a J-Link dropped, the agent crashed, the lease expired, the job hung.
  **Our problem, and it must never be reported as a firmware failure.**

Every HIL system that conflates these trains its engineers to distrust it — "the rig is flaky, just
re-run it" — and then real regressions get re-run away. Separating them is a one-line schema decision
that determines whether people believe the system. In the presentation this is your strongest
customer-impact point, and it costs nothing to build.

**Note which outcome retries.** `INFRA_ERROR` retries; `FAILED` does not. Retrying a failing test
automatically is how a real regression gets quietly re-run away until nobody is looking. If a team
wants flake detection, make it an explicit per-job opt-in (`retry_on_failure: 2`) recorded separately,
so "passed on attempt 3" never reads the same as "passed."

**Two counters, not one.** `attempt` counts every dispatch (history). **Retry and dead-letter
decisions are driven by `tried_agents`** — the list of *distinct benches* the job has failed on. A
single counter cannot tell "this job is poison" from "these three benches are broken," and gating on
it means three infra failures dead-letter a healthy job while §3.5's table promises presence-expiry
requeue "always."

**`DEAD_LETTER` / poison jobs.** A job that crashes every bench it touches will walk the fleet
quarantining machines one at a time until nothing is left. After `MAX_DISTINCT_AGENTS` (3), stop: mark
it `DEAD_LETTER`, and **do not count those failures against the agents**. Distinguishing "this job is
poison" from "these benches are broken" is the difference between a self-healing fleet and a
self-destructing one.

---

## 5. Data model

```sql
CREATE TABLE agents (
    id                  TEXT PRIMARY KEY,      -- "bench-sf-04"
    hostname            TEXT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN
                          ('online','offline','quarantined','draining')),
    presence_expires_at REAL NOT NULL,         -- unix float, UTC. ALWAYS set, whatever the load.
    last_heartbeat_at   REAL NOT NULL,
    consecutive_fails   INTEGER NOT NULL DEFAULT 0,   -- failures spanning ≥2 distinct resources
    quarantined_at      REAL,
    registered_at       REAL NOT NULL,
    agent_version       TEXT
);
CREATE INDEX idx_agents_presence ON agents(presence_expires_at) WHERE state != 'offline';

CREATE TABLE resources (
    id                TEXT PRIMARY KEY,        -- "bench-sf-04:vg-01"
    agent_id          TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    capabilities      TEXT NOT NULL,           -- JSON: {"product":"vehicle_gateway","harness":"j1939"}
    state             TEXT NOT NULL CHECK (state IN ('free','busy','unhealthy')),
    current_job_id    TEXT REFERENCES jobs(id),  -- single column ⇒ I2 is structural
    last_assigned_at  REAL,                    -- drives LRU (§3.4)
    consecutive_fails INTEGER NOT NULL DEFAULT 0,
    quarantined_at    REAL
);
CREATE INDEX idx_res_dispatch ON resources(agent_id, state, last_assigned_at);
CREATE INDEX idx_res_job      ON resources(current_job_id) WHERE current_job_id IS NOT NULL;

CREATE TABLE jobs (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    requirements   TEXT NOT NULL,              -- JSON LIST of tag-subsets, one per device needed
    resource_count INTEGER NOT NULL,           -- len(requirements); denormalized so I8 is one query
    payload        TEXT NOT NULL,
    state          TEXT NOT NULL CHECK (state IN
                     ('queued','assigned','running','passed','failed',
                      'infra_error','cancelled','dead_letter')),
    agent_id       TEXT REFERENCES agents(id), -- co-location: ALL resources are on this one agent
    epoch          INTEGER NOT NULL DEFAULT 0, -- fencing token; only ever self-incremented
    attempt        INTEGER NOT NULL DEFAULT 0, -- total dispatches (history only)
    tried_agents   TEXT NOT NULL DEFAULT '[]', -- JSON list — drives retry AND poison detection
    priority       INTEGER NOT NULL DEFAULT 100,
    max_duration_s INTEGER NOT NULL DEFAULT 600,
    submitted_at   REAL NOT NULL,
    assigned_at    REAL,
    started_at     REAL,
    finished_at    REAL,
    blocked_reason TEXT,                       -- 'no_capable_agent' — surfaced by `tss why`
    outcome        TEXT CHECK (outcome IS NULL OR outcome IN
                     ('passed','failed','infra_error','cancelled','dead_letter')),
    result_detail  TEXT                        -- 'timeout', 'agent_lost', 'no_capable_agent'
);
CREATE INDEX idx_jobs_queue   ON jobs(state, priority, submitted_at);
CREATE INDEX idx_jobs_timeout ON jobs(state, started_at) WHERE state = 'running';

CREATE TABLE job_resources (                   -- durable allocation record; survives release
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    epoch       INTEGER NOT NULL,
    claimed_at  REAL NOT NULL,
    released_at REAL,
    PRIMARY KEY (job_id, resource_id, epoch)
);

CREATE TABLE events (                          -- append-only audit log
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL NOT NULL,
    kind      TEXT NOT NULL,                   -- job.requeued, agent.offline, resource.unhealthy...
    agent_id  TEXT, resource_id TEXT, job_id TEXT,
    detail    TEXT
);
```

**Why `resource_count` is denormalized.** I8 — "holds exactly what it required" — becomes one cheap
query per running job instead of parsing a JSON array on every check. The invariant checker runs
continuously under chaos; make its hot path trivial.

**Why `job_resources` exists alongside `resources.current_job_id`.** The column is live state, cleared
on release. The table is history: it survives, carries the epoch, and is what answers "which devices
did attempt 2 actually run on?" after the fact. Including `epoch` in the primary key means a requeued
job's second allocation doesn't collide with its first.

**Why `tried_agents` is a JSON list and not a count.** A count says a job failed three times. The list
says it failed on three *different* benches — which is what separates "poison job" from "one broken
bench." It's written by the claim in §3.3 via `json_insert`, not by hopeful convention.

**Why `events` is persisted and append-only.** During the demo you'll want to scroll back and narrate.
More practically, when an engineer asks "why did my job get moved?", the answer must be reconstructable
after the fact. A log that only exists in the TUI's memory is not evidence.

---

## 6. API contracts

### Agent-facing (`/v1/agents`)

```http
POST /v1/agents/register
  → { "agent_id": "bench-sf-04", "hostname": "...", "agent_version": "0.1.0",
      "resources": [
        {"id": "vg-01", "capabilities": {"product":"vehicle_gateway","hw_rev":"B","harness":"j1939"}},
        {"id": "vg-02", "capabilities": {"product":"vehicle_gateway","hw_rev":"C","harness":"obd2"}},
        {"id": "ag-01", "capabilities": {"product":"asset_gateway","hw_rev":"A"}}
      ] }
  ← 200 { "heartbeat_interval_s": 3, "presence_ttl_s": 12, "longpoll_timeout_s": 8 }

  Idempotent — re-registering an existing ID replaces its inventory in place. Never duplicates.
  If the agent held any running jobs, ALL of them are requeued and their epochs bumped in one
  transaction, and every resource is reset to FREE. A restarted agent has lost its hardware
  state; pretending it still owns those jobs orphans them forever with no lease to expire.
  Quarantine clears only if agent_version changed.

POST /v1/agents/{id}/heartbeat        ← the workhorse; long-polls while it has spare capacity
  → { "running_jobs": [{"job_id": ..., "epoch": 5}], "resource_health": {"vg-02": "unhealthy"} }
  ← 200 { "assignment": { "job_id": ..., "epoch": 6, "resource_ids": ["vg-01","ag-01"],
                          "payload": {...}, "max_duration_s": 600 } | null,
          "directives": [] | ["drain"] | [{"cancel_job": "job-8f21"}] }
  ← 404 { "error": "unknown_agent",    "action": "register" }
  ← 409 { "error": "lease_lost",       "action": "abandon_job", "job_id": ... }
  ← 410 { "error": "presence_expired", "action": "register" }

POST /v1/jobs/{job_id}/start      → { "agent_id", "epoch" }   marks RUNNING, sets started_at
POST /v1/jobs/{job_id}/complete
  → { "agent_id", "epoch": 6, "outcome": "passed"|"failed"|"infra_error",
      "detail": "...", "duration_s": 42.1 }
  ← 200 { "accepted": true }        ← frees ALL of the job's resources in one transaction
  ← 409 { "error": "stale_epoch", "action": "abandon_job" }   ← fencing
```

**The heartbeat carries a list, not a single job.** A bench with four devices may be running two jobs
at once; the request reports every job it believes it owns, and TSS fences each independently. The
`409` names which job to abandon — the agent's other jobs are unaffected.

**`resource_health` is how device-level health gets reported.** The agent probes its own hardware (the
J-Link check, in MeteorShower terms) and tells TSS which devices are bad. TSS never probes hardware
itself — it can't reach it.

**The `410` is what makes recovery reachable.** A reaped agent's row still exists, so its heartbeat
would otherwise get a cheerful `200` and it would sit in OFFLINE forever, silently gone from the fleet —
precisely what a `flaky_network` bench does after a few dropped beats.

### Client-facing (`/v1/`)

```http
POST   /v1/jobs                     { "name", "requirements": [ {...}, {...} ], "payload" }
                                    → { "job_id", "queue_position" }
GET    /v1/jobs/{id}                status, including why-blocked and reservation state
DELETE /v1/jobs/{id}                cancel. queued → CANCELLED. running → bump epoch + cancel
                                    directive + free resources. The epoch bump is what stops a
                                    late "PASSED" overwriting CANCELLED.
GET    /v1/fleet                    agents, each with its resources and their states
GET    /v1/queue                    queued and running jobs with wait times
GET    /v1/stats                    utilization, throughput, requeue rate, quarantined devices
POST   /v1/agents/{id}/drain
POST   /v1/agents/{id}/unquarantine
POST   /v1/resources/{id}/unquarantine
WS     /v1/events                   live event stream
```

**Why `/v1` from the start.** Agents are deployed software on machines you may not control. The day you
need a breaking change you'll have to run both versions while benches roll forward. Adding the prefix
on day one is free; retrofitting it is not.

---

## 7. Failure semantics — the decisions worth defending

### 7.1 Timing parameters

| Constant | Value | Reasoning |
|---|---|---|
| `HEARTBEAT_INTERVAL` | 3s | One per **agent**, not per resource — device count doesn't change the load |
| `PRESENCE_TTL` | 12s | **4× heartbeat** — tolerates 3 consecutive losses with slack for RTT |
| `REAPER_INTERVAL` | 2s | Bounds detection to TTL + 2s = **14s worst case**. This is the demo number. |
| `LONGPOLL_TIMEOUT` | 8s | Constraint: `LONGPOLL_TIMEOUT + HEARTBEAT_INTERVAL < PRESENCE_TTL` (8+3 < 12), so an agent's own long-poll can never let its presence lapse |
| `MAX_DISTINCT_AGENTS` | 3 | Distinct **benches** tried before a job is called poison |
| `QUARANTINE_THRESHOLD` | 3 | Consecutive failures — on one resource → quarantine the device; spanning ≥2 resources → quarantine the machine |
| `STARVATION_THRESHOLD` | 60s | After this, the oldest unsatisfiable job becomes the sole reserver (§3.4.1) |
| `UNSATISFIABLE_TIMEOUT` | 30 min | A job no bench *could ever* satisfy stays queued this long (fleets get repaired), then dead-letters rather than clogging the queue forever |

**Show the arithmetic on `flaky_network`.** At a 30% heartbeat drop rate and a TTL of 4 beats, a false
reap needs four consecutive losses: 0.3⁴ ≈ **0.8% per 12s window** — about one spurious reap per agent
per 25 minutes, and it self-heals (the agent re-registers on `410`). At 3× TTL it'd be 2.7%; at 2×,
9% — visibly broken. **That is why the multiplier is 4 and not 3.** Being able to show this calculation
is worth more than the parameter itself: it's the difference between a number you picked and a number
you derived.

**Heartbeat load is per-machine.** Worth saying explicitly when the scale question comes: 1,000
*benches* is ~333 req/s whether each holds one device or six. Resource count multiplies your capacity,
not your control-plane load.

### 7.2 The failure matrix

| Failure | Detected by | Response | Job outcome |
|---|---|---|---|
| Bench dies running 1 job | Presence expiry (12s) | Offline; free its resources; requeue | Retried elsewhere |
| **Bench dies running 3 jobs** | Presence expiry | Offline; free **all** resources; requeue **each distinct job once** | All three retried |
| Bench dies while idle | Presence expiry | Offline | — (nothing held) |
| One device fails, machine fine | Agent's `resource_health` | Resource → unhealthy; bench keeps working on the others | That job `infra_error`, retried |
| Network partition, agent alive | Presence expiry | Jobs requeued; returning agent fenced per job by epoch | Retried; zombie reports rejected |
| Agent alive, job hung | Reaper sweep 2 | Cancel directive, epoch bump, free that job's resources only | `infra_error:timeout`, one retry |
| 3 failures on one device | `resources.consecutive_fails` | Quarantine the **device** | Routed to other devices |
| 3 failures across devices | `agents.consecutive_fails` | Quarantine the **machine** | Routed to other benches |
| Job fails on 3 distinct benches | `tried_agents` | Dead-letter, benches unblamed | `DEAD_LETTER` |
| Agent restarts while loaded | Re-register | All its jobs requeued + epochs bumped; resources reset FREE | Retried elsewhere |
| Job cancelled while running | Client `DELETE` | Epoch bump, cancel directive, resources freed | `CANCELLED`, late report fenced |
| Partial claim fails mid-way | Rowcount check | **`ROLLBACK`** — every resource in that transaction returns to free | Stays queued, retried next pass |
| Big job starved by small ones | `STARVATION_THRESHOLD` | Sole reserver on one feasible bench; free matching devices there withheld | Runs once its siblings free |
| No bench could ever satisfy it | Feasibility filter (§3.4.1) | **Don't reserve** — flag `no_capable_agent`, alert, keep queued | Dead-letters after 30 min |
| TSS restarts | Reaper's first tick | Every stale presence lease expires normally | Requeued automatically |
| Agent completes after requeue | Epoch mismatch | `409 stale_epoch` | Ignored; agent releases locally |

**Note the TSS-restart row.** It needs no dedicated recovery code — presence leases are wall-clock, so
after a restart they're simply expired and the ordinary reaper path handles them. That's the payoff for
choosing leases over liveness flags.

### 7.3 The races the brief warns about

> *"AI often misses edge cases like race conditions in thread-locking or socket timeouts."*

Five that actually bite here:

1. **Double-booking a device** — two scheduler passes claim the same free resource.
   → Conditional `UPDATE ... WHERE state='free'` + rowcount check + `SQLITE_BUSY` handling (§3.3). **I2.**
2. **Partial allocation** — a job takes 2 of 3 devices and waits, while another does the mirror image.
   → All N claims in one transaction; any failure rolls back every one (§3.3, §7.5). **I8.**
3. **Requeue/complete race** — the reaper requeues at the same instant the agent reports success.
   → Both paths move `epoch` inside one transaction; the completion compares. **I1.**
4. **Renewal resurrecting a dead lease** — a heartbeat lands microseconds after the reaper reaped.
   → Renewal guarded by `state != 'offline'`; the agent gets `410` and re-registers (§3.5).
   *Most likely to be generated incorrectly, because renewal looks trivial.*
5. **Fan-out double-requeue** — a dead bench running one job on three devices requeues that job three
   times, bumping the epoch and burning the retry budget in one go.
   → `SELECT DISTINCT current_job_id` and requeue per **job**, not per resource (§3.5). *This one is
   silent: nothing errors, the job just dead-letters early.*

Plus the classic: **lost wakeup** — a resource frees up mid-pass, the notify is dropped, and the queue
stalls with free devices sitting there. Clear the notify event *before* reading state, not after, and
keep a 1s backstop tick. MeteorShower's `wait_for_allocation` carries exactly this comment — a real bug
that was really hit.

**These belong in the AI-reflection section.** A model will cheerfully generate a scheduler with all of
them. Naming them, showing the test that catches each, and explaining why the generated version was
wrong is the strongest possible answer to *"where did AI give you a sub-optimal solution?"*

### 7.4 At-least-once execution, exactly-once result

Requeue-on-expiry means a job can occasionally **execute** twice: the agent was alive but partitioned,
finished the work, and by then someone else had started it. Exactly-once execution is not achievable
over an unreliable network — anyone claiming otherwise has hidden the problem, not solved it.

**What we guarantee instead:** at-least-once execution, **exactly-once result**. The job may run twice;
only one result is ever accepted, always from the agent that currently owns it. That's what I1 asserts,
and it's what fencing buys.

For HIL testing this is the right trade — a firmware test re-flashing a device is wasteful but
harmless, whereas losing a result means an engineer waits an hour for nothing.

**Say this out loud in the presentation.** Naming a limitation and justifying the trade-off reads as
senior; pretending it doesn't exist reads as not having thought about it.

### 7.5 Why partial allocation is never allowed

The question sounds like a scheduling nicety and is actually a deadlock.

```
job-A needs 2 VGs.   Takes bench-01:vg-01.   Waits for a second VG.
job-B needs 2 VGs.   Takes bench-01:vg-02.   Waits for a second VG.
                     Neither can ever proceed. Both hold hardware forever.
```

This is textbook **hold-and-wait**, one of the four Coffman conditions for deadlock. The standard
prevention is to break exactly that condition: acquire everything atomically or acquire nothing. Which
is what §3.3 does — and it's why the whole claim lives in one transaction rather than a loop of
individual claims with cleanup on failure.

**Three things follow, and all are worth saying.**

First, the failure mode of all-or-nothing is *starvation*, not deadlock — a big job may keep losing
races to small ones. That's strictly better: starvation is recoverable and observable, deadlock is
neither. And §3.4.1's single-reserver rule bounds it.

Second, this is why the reservation logic and the atomic claim are the same design decision viewed from
two angles. The transaction stops you from *holding* a partial set; the reservation stops you from
*never assembling* a full one.

Third — and this is the correction to make if someone asks whether co-location is what saves you —
**bench boundaries have nothing to do with it.** The Coffman condition being broken is hold-and-wait,
and that is broken by the transaction, not by where the devices live. A claim spanning five benches is
the same single local transaction and is equally safe. Co-location is justified by physics and by
failure surface (§1.2), not by deadlock. Saying "we avoid deadlock by keeping jobs on one bench" is
wrong, and it is wrong in a way that suggests you don't know which mechanism is doing the work.

There *is* one deadlock that bench boundaries don't touch and ordering does: two transactions locking
the same rows in different orders. Sort the resource IDs (§3.3). Invisible in SQLite, real in Postgres.

> **Do not accept an AI-generated "acquire in a loop, release on failure" version.** It looks
> equivalent and is not: if the process dies between acquiring resource 2 and failing on resource 3,
> those two devices are locked with no owner and no lease to expire them. The transaction gets rollback
> from the database for free — including on crash.

---

## 8. Testing strategy

| Layer | What it covers | Notes |
|---|---|---|
| **Unit** | Capability subset match, N-way resource matching, LRU pick, **feasibility filter + target selection**, epoch comparison | Pure functions, no I/O |
| **Concurrency** | 50 **OS threads on separate SQLite connections** racing for overlapping resource sets → no double-book, no partial hold | See below |
| **Integration** | Real mock agents with real inventories over real HTTP | `httpx.AsyncClient`; never mock the transport |
| **Chaos** | 15 agents × 2–4 devices, 100 jobs (30% multi-device), mixed profiles, invariants throughout | The demo *is* the test. Runs in CI. |
| **Property** | Every seed CI runs (5) → zero invariant violations | Catches the timing bug one hand-picked seed misses |

**The concurrency test must use threads, not asyncio tasks.** With async tasks over a synchronous
`sqlite3` connection there's no preemption point between the check and the act, so a *naive*
check-then-act implementation passes — and the point of writing this test first is to watch the naive
version fail. Real OS threads on separate connections is what makes it real.

**Test the reservation separately, and test the impossible case.** `test_starvation.py` needs three
scenarios, not one: (a) a 3-device job under a stream of 1-device jobs eventually runs; (b) while it
reserves, jobs that match *other* benches keep flowing — this is what catches an unscoped guard; (c) a
job no bench could ever satisfy is flagged `no_capable_agent` and **reserves nothing**. Case (c) is the
one an implementation will get wrong, and it fails silently: the queue just stops moving.

**Make it a 3-device bench and 2-device jobs.** Fifty threads all claiming one resource proves less
than fifty threads claiming *overlapping pairs* from a pool of three. The second setup catches partial
allocation, which is the bug that actually matters now, and it's barely more code.

**Coverage is the wrong metric; invariants are the right one.** You can have 95% line coverage and still
double-book a device. State the bar as "nine invariants hold across every seed CI runs" — a threshold
you can defend when asked *"how did you know it was good enough?"* Keep the seed count you quote on the
slide identical to what CI runs.

---

## 9. Scale evolution: 10 → 1,000 agents, multiple offices

**Stage 1 — today (10–20 agents, ~40 devices, one office).** Single FastAPI process, SQLite WAL,
in-process scheduler and reaper. 20 agents at 3s heartbeats ≈ 7 req/s. Trivial.

**Stage 2 — one office, 200+ agents.** The binding constraint is **SQLite's single writer**, not CPU:
every heartbeat writes `presence_expires_at`, and WAL allows one writer at a time. That's also why
`busy_timeout=5000` sits uneasily next to a sub-second dispatch target — it's the pressure valve you'd
be leaning on. Move to Postgres, run N stateless TSS replicas behind a load balancer.

The claim ports **verbatim** — a conditional `UPDATE ... WHERE state='free'` with a rowcount check works
identically in Postgres, and the multi-row transaction is exactly the same shape. **One caveat that only
bites here:** Postgres takes real row locks, so two concurrent claims touching the same resources in
different orders can deadlock and one gets aborted. SQLite never showed you this because `BEGIN
IMMEDIATE` serializes writers. Sorting the resource IDs in the claim (§3.3) is what makes the port a
non-event instead of a mystery incident in week one. (`SELECT ... FOR UPDATE
SKIP LOCKED` is a different tool, for contended queue *consumption*; don't conflate them just because
both appear in scaling discussions.) **The point that survives any probe: the N-way claim was in the
database from day one rather than in an `asyncio.Lock`, so this is a swap, not a rewrite.**

Two things break here that are easy to miss:
- **The event bus.** A WebSocket client on replica A stops seeing events from replica B the moment
  there's more than one replica. Redis pub/sub or NATS is required *here*, not later.
- **Long-poll connections.** ~200 held per replica. Fine for async, but it caps how far one replica goes.

**Stage 3 — 1,000 agents, multiple global offices.** Don't put a bench in Frankfurt and its scheduler in
San Francisco. Regional TSS per office: agents register locally, low latency, and the office keeps
testing through a WAN partition — **which requires the regional TSS to own its own queue**, not proxy a
global one. A thin global control plane handles submission routing and cross-region visibility; jobs
route to a region by capability availability, and **agents never cross the WAN**.

The co-location rule (§1.2) helps here too: since a job's resources are always on one bench, they are
trivially always in one region. No cross-region allocation exists to go wrong.

This is where pull pays off completely: a push design needs inbound firewall rules into every office.
Pull needs nothing.

**Stage 4 — beyond.** The matcher is currently O(agents × resources) per queued job, which is fine at 40
resources and not at 5,000. Index resources by capability signature so candidate lookup is a hash rather
than a scan — MeteorShower's `_match_devices` has the same O(n) shape and the same ceiling. And if
heartbeat write volume becomes the cost centre, lengthen the interval for agents with no running jobs.

**The honest caveat, worth saying.** At 1,000 benches and 4,000 devices the hard problem stops being
scheduling and becomes **fleet health** — knowing which of your devices are quietly degraded.
Resource-level quarantine (§4.2) is the seed of that; the real answer is a hardware probe loop per device
and a reliability score per unit in the fleet view. Naming this as the next problem, rather than claiming
the design solves it, is the stronger answer.

---

## 10. Build order

Each step is independently demoable, so you always have something to show even if you run out of time.

**Model resources from day one; exercise them at N=1 until the core is green.** The schema, the claim
loop, and `job_resources` are written for N devices from the first commit — but steps 1–4 submit jobs
that need exactly one. Retrofitting the resource model later means rewriting the claim, the reaper
fan-out, and half the invariants; exercising it at N=1 first keeps the early steps simple. Flip on
multi-device jobs at step 5, where the chaos suite can immediately hammer them.

1. **Store + models + schema + `justfile`.** Agents, resources, the N-way claim, and its threaded
   concurrency test. *Nothing else works if this is wrong; everything else is easy if it's right.*
2. **Register with inventory + heartbeat + presence + `tss fleet`.** A bench appears with its devices,
   and gets reaped when it dies — including while idle. First visible win.
3. **Submit + schedule + complete (N=1).** The happy path end to end.
4. **Reaper: fan-out requeue + epochs + fencing.** Kill a bench by hand; watch its jobs come back.
5. **Multi-device jobs + reservations + chaos + invariant checker.** Turn on `--multi-pct 30`. This is
   where I8 and the starvation guard get real, and where the interesting bugs live.
6. **README + `tss watch` TUI + `tss why`.** The README is a required deliverable — write it here,
   while the commands are fresh, not at 1am before the presentation.
7. **Resource-level health, quarantine attribution, drain, operator verbs.** If time allows.

**Everything from step 5 onward is what makes the presentation land.** If time gets tight, cut breadth in
step 7 — never the chaos suite. A working chaos demo with three profiles beats a feature-complete service
you can only describe.

**"How did you prioritize?" — have this answer ready, it's asked directly.** Priority was set by *what a
firmware engineer loses when it's missing*, correctness before surface area:

1. **Not double-booking a device, and never holding a partial set** — silently corrupts results or
   deadlocks the fleet; nothing above it matters if this is broken.
2. **Not losing a job when a bench dies** — the #1 daily pain: re-running a build because hardware flaked.
3. **Not wasting capacity** — one job per bench would idle two-thirds of a multi-device fleet, which the
   engineer experiences as "the queue is always long."
4. **Not blaming the engineer for our hardware** (`INFRA_ERROR`) — determines whether anyone trusts it.
5. **Telling them why they're waiting** (`tss why`) — removes a whole category of Slack interruption.
6. **Everything else** — priority scheduling, artifacts, dashboards. Real, but nobody's blocked on them.

---

## 11. Suggested repo layout

```
tss/
├── README.md             # REQUIRED DELIVERABLE: how to run the service + mock agents
├── CLAUDE.md             # invariants and rules (see CLAUDE-CODE-BRIEF.md §4)
├── justfile              # just serve | test | chaos | lint  — CLAUDE.md depends on these
├── pyproject.toml
├── .pre-commit-config.yaml
├── .github/workflows/ci.yml
├── tss/
│   ├── core/
│   │   ├── models.py     # Agent, Resource, Job, Assignment
│   │   ├── store.py      # SQLite. ALL writes here, including the N-way claim.
│   │   ├── matcher.py    # capability subset + N-of-M resource selection (pure)
│   │   ├── scheduler.py  # queue walk, reservations, LRU; calls store.claim_all()
│   │   ├── reaper.py     # sweep 1: presence expiry + fan-out.  sweep 2: job timeout.
│   │   ├── events.py     # pub/sub abstraction
│   │   └── config.py     # every timing constant, env-overridable
│   ├── api/
│   │   ├── agent.py      # /v1/agents/*
│   │   ├── client.py     # /v1/jobs, /v1/fleet, /v1/queue, operator verbs
│   │   └── ws.py         # /v1/events
│   ├── agent/
│   │   ├── daemon.py     # register(inventory) → heartbeat → run N jobs → report
│   │   ├── executor.py   # runs a job payload against a resource set (simulated)
│   │   └── health.py     # per-device probe → resource_health in the heartbeat
│   ├── chaos/
│   │   ├── profiles.py   # crasher, zombie, flaky, hung, idle_death, resource_flap, liar, flapper
│   │   ├── mock_agent.py # daemon + inventory + profile + ground-truth state for the checker
│   │   ├── runner.py     # spin up N, flood jobs, --multi-pct, seedable
│   │   └── invariants.py # I1–I9
│   └── cli/main.py       # fleet | queue | watch | why | submit | cancel | drain | unquarantine
└── tests/
    ├── test_matcher.py       # N-of-M selection, subset matching
    ├── test_concurrency.py   # threads, separate connections, OVERLAPPING resource sets
    ├── test_allocation.py    # I8 — all-or-nothing, rollback on partial failure
    ├── test_lease.py         # presence expiry, renewal guard, epoch fencing, zombie
    ├── test_fanout.py        # bench with 3 jobs dies → 3 requeues, each epoch bumped ONCE
    ├── test_starvation.py    # multi-device job under a stream of single-device jobs
    ├── test_timeout.py       # job timeout vs presence timeout (I6)
    └── test_chaos.py         # seeded end-to-end
```

**The README is a graded deliverable, not documentation hygiene.** The brief asks for "brief
instructions on how to run your Dispatcher and the Mock Agents." Minimum: `just serve`, how to start one
real agent with an inventory, how to start the chaos fleet, how to submit a single- and a multi-device
job, how to watch the TUI. Six commands someone can paste.

---

## 12. Presentation plan (20 minutes)

The brief enumerates twelve sub-answers in twenty minutes. Budget the time or the demo eats it.

| Time | Section | Content |
|---|---|---|
| 0:00–2:00 | Problem & approach | Four pillars; the thesis from §0; **the bench-vs-device distinction from §1.2** |
| 2:00–4:00 | Architecture | `tss-system-diagram.html` §1; the pull decision; the two-level model |
| 4:00–10:00 | **Live demo** | The script in `CLAUDE-CODE-BRIEF.md` §7 |
| 10:00–13:00 | Customer impact | Prioritization (§10); `INFRA_ERROR`; utilization; `tss why`; next step; how you'd sell it |
| 13:00–17:00 | AI reflection | Process, key prompts, the five races (§7.3), the quality harness |
| 17:00–19:00 | Scale | §9, four stages + the fleet-health caveat |
| 19:00–20:00 | Buffer | Something will run long. It's usually the demo. |

| Their question | Your answer |
|---|---|
| 1a · diagram, component interaction | `tss-system-diagram.html` §1 + §2 here |
| 1b · step-by-step demo and its value | Demo script, `CLAUDE-CODE-BRIEF.md` §7 |
| 2 · how did you prioritize | §10 — the six-item ranking by what the engineer loses |
| 2a · decisions made with the customer in mind | `FAILED` vs `INFRA_ERROR` (§4.3); device-level allocation so the queue isn't blocked by idle hardware (§1.2); `tss why` (§3.9); requeue so nobody re-runs a build over a flaky bench |
| 2b · next step for the customer | Per-device health probes and reliability scoring (§9); result artifacts and log capture |
| 2c · how would you sell it | "Your benches stop sitting half-idle, you stop re-running builds because hardware flaked, and you stop asking in Slack why your test is stuck." |
| 3 · where AI helped | Scaffolding FastAPI + Pydantic, chaos profiles, TUI layout, test boilerplate |
| 3 · where AI was wrong | The five races in §7.3 — especially the acquire-in-a-loop allocation (§7.5) and the fan-out double-requeue. Show the generated code and your fix. |
| 3a–c · process, start, key prompts | `CLAUDE-CODE-BRIEF.md` §1, §2, §6 |
| 3d · how you knew it was good enough | §3.8 — nine invariants across every CI seed. A threshold, not a feeling. |
| 3e · AI tooling kept permanently in the repo | `CLAUDE.md` with the invariants; pre-commit running ruff + the concurrency and allocation tests; chaos as a merge gate |
| 3f · AI in the loop vs deterministic | Scheduling is deterministic and must stay so — an allocator you can't reason about is unauditable. AI belongs in triage (classifying failures, spotting flaky devices), not in the allocation path. |
| 4 · 10 → 1,000 agents | §9, four stages, plus the fleet-health caveat |

**If you only make one point in the architecture section, make it §1.2.** "A bench isn't one thing" is
the observation that separates someone who has run a HIL fleet from someone who has read about one — and
it's the reason your design gets full utilization out of hardware that a bench-level scheduler would
leave idle.

---

## 13. Open questions to decide during the build

1. **Assignment ack.** The agent calls `/start` when it begins, closing most of the "assignment lost in
   flight" window — but the gap between claim and `/start` is real, and it costs a full `PRESENCE_TTL`
   (with the devices held) when it happens. *Recommendation: accept for the POC, name it; the fix is a
   short `assigned` timeout in reaper sweep 2.*
2. **Cross-bench jobs.** Excluded by co-location (§1.2) — on physics and failure-surface grounds, **not**
   because of deadlock; the claim would be the same single transaction and equally safe. MeteorShower
   needs iOS + Android simultaneously, which works there because everything is on one host. The genuinely
   hard parts if Samsara ever needs it are agent-side: two daemons co-executing one test need a sync
   protocol, and the job dies if *either* bench's presence lease expires. *Recommendation: keep
   co-location as one predicate in the matcher so it's a flag, not a redesign; don't build it; be ready
   to say precisely why it's hard and why it isn't a deadlock question.*
3. **Job priority.** The column exists; FIFO for now. Priority without the reservation guard is a bug
   factory — only wire it up if time allows.
4. **Resource affinity / stickiness.** Re-running a job on the same physical device it failed on is
   sometimes what you want (reproducing a device-specific bug) and sometimes exactly what you don't.
   Worth a flag eventually; out of scope now.
5. **Artifact handling.** Real HIL tests produce logs, traces, firmware dumps. Out of scope, and the
   strongest *"what's next"* answer for the customer section.
6. **Historical duration estimates.** Needed before `tss why` can honestly show an ETA (§3.9).
