# TSS — Test Scheduling Service

TSS is a job broker for physical hardware. A **testbed agent** is a machine; the **resources** it
owns are the firmware devices cabled to it. Agents register themselves and their inventory; firmware
engineers submit jobs declaring which devices they need; TSS matches jobs to free, compatible
devices — **all of them or none** — and hands the owning agent a time-limited lease it renews by
heartbeating. If an agent stops heartbeating, its lease expires, TSS marks it offline, takes back
every job holding any of its devices and re-queues them. A bench is not the unit of allocation: two
jobs run side by side on one machine as long as they need different devices.

- **Design and reasoning:** [`TSS-Architecture.md`](TSS-Architecture.md) — starts with a summary and
  a table of contents
- **Invariants and the rules that follow from them:** [`CLAUDE.md`](CLAUDE.md)
- **How it was built with AI:** [`AILOG.md`](AILOG.md) — the log is the commit history. The
  build-step messages carry the prompt verbatim; all of them carry what the AI got wrong and what was
  overridden. `AILOG.md` explains the convention and quotes several in full, including the sabotage
  exercise.
- **The diagram:** [`tss-system-diagram.html`](tss-system-diagram.html) — components and data flow,
  the two-level allocation model, a bench dying mid-job, and the state machines
- **The original build handoff:** [`CLAUDE-CODE-BRIEF.md`](CLAUDE-CODE-BRIEF.md) — written before any
  code existed and deliberately not rewritten since
- **Licence:** [MIT](LICENSE)

**See it survive failure:** [`TOUR.md`](TOUR.md) — kill a bench mid-job and watch the fleet recover,
starve a multi-device job and watch the reservation hold a device that stays free and owned by
nobody, quarantine a device and watch its bench keep working beside it. Every command in it is
runnable as written.

---

## Install

Needs **Python 3.12+**, [`just`](https://github.com/casey/just) and [`uv`](https://docs.astral.sh/uv/).

```bash
just install          # creates .venv and installs the project
```

<details>
<summary>Without <code>just</code> or <code>uv</code></summary>

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

Every `just` recipe below is a one-line wrapper — run `just --list` to see the command each one
runs, and use `.venv/bin/python -m ...` directly if you prefer.
</details>

---

## Run it

Two terminals, or one if you use the demo shortcuts below.

**Terminal 1 — the service** (the dispatcher: API, scheduler and reaper, one process):

```bash
just serve                      # http://127.0.0.1:8000
```

**Terminal 2 — a testbed agent** with three devices cabled to it:

```bash
just agent bench-sf-01 3
```

`just agent <id> <devices>` starts the real daemon. For devices that are not all identical, write an
inventory file and pass it — this is what a real bench reports about itself:

```bash
cat > bench.json <<'JSON'
[{"id": "vg-01", "capabilities": {"product": "vehicle_gateway", "harness": "j1939"}},
 {"id": "vg-02", "capabilities": {"product": "vehicle_gateway", "harness": "obd2"}},
 {"id": "ag-01", "capabilities": {"product": "asset_gateway"}}]
JSON
.venv/bin/python -m tss.agent.daemon --id bench-sf-02 --inventory bench.json
```

### The short way

Four recipes, so the whole demo runs in **one** terminal:

```bash
just start                      # service in the background, live page in the browser
just add bench-sf-01 2 0        # a bench with 2 vehicle gateways
just add bench-ag-01 1 2        # 1 vehicle gateway + 2 asset gateways
just kill bench-sf-01           # stop that bench — watch it go OFFLINE
just stop                       # stop everything and delete the demo's state
```

`just start` backgrounds the service, waits until it answers, opens
<http://127.0.0.1:8000/>, and tells you where the log is. Started twice it says so and opens
the page again rather than stacking a second scheduler onto the same port. If the service
fails to come up it says that too, with the tail of `.demo-logs/serve.log` — a broken start is
never silent. It sets `TSS_STARVATION_THRESHOLD_S=5`, the demo setting: production is 60s, and
five seconds is simply short enough to stand in front of while a multi-device job waits to
reserve.

Nothing runs in the foreground any more, so **`just stop` is the stop path** — it kills the
benches and the service, and deletes `tss.db` and `.demo-logs/`. It is safe to run cold, and
safe to run twice.

`just add NAME VG AG` writes the inventory for you and backgrounds the daemon, with its output
in `.demo-logs/NAME.log` so the terminal stays readable.

**Then submit work and watch it.**

```bash
just submit smoke 1 0                 # one vehicle gateway
just submit gateway-to-gateway 2 0    # TWO vehicle gateways, both on ONE bench
just submit mixed 1 1                 # one of each — needs a bench that has both
just submit soak 1 0 120              # long enough to still be there when you look
just fleet                            # benches with their devices
just queue                            # what is running, what is waiting
just watch                            # the live view — this is the one to have on screen
just why soak                         # where is that job, and what is it waiting for?
```

`tss why` answers from the live queue, so ask about a job that is still in it — a ten-second job has
finished and left before you can type the question. Finished jobs are still there by full id
(`just why job-24778...`); everything else takes the short id or the name off the screen.

`just submit NAME VG AG DURATION` takes its counts in the same order as `just add NAME VG AG`:
a name, vehicle gateways, asset gateways, then how long. If no bench in the fleet could ever run
what you asked for, it says so at once rather than leaving you to find out from `tss why`:

```
mixed-test (a3f91) queued — WARNING: no bench in the fleet can satisfy this
(needs vehicle_gateway + asset_gateway on ONE bench)
It will wait for the fleet to change, and dead-letters if none appears in time.
```

The job is still queued — that is deliberate, because fleets get repaired and extended
(`TSS-Architecture.md` §3.4.1). `just submit-bad NAME VG AG DURATION` is the same thing with a
payload that reports `infra_error`: a chaos control, kept as a separate verb because a customer does
not get to declare their own job's outcome.

The service also serves the live fleet view at **<http://127.0.0.1:8000/>** — the same feed `tss
watch` renders, in a browser: one self-contained page, pushed over `WS /v1/events`, never polled. It
needs no internet, loads nothing from a CDN, and re-snapshots from scratch if the connection drops.

Jobs read as `smoke-1 (2bb76)` on every human surface — the name you chose, plus enough of the id to
tell three `smoke` jobs apart. The full `job-2bb76a1c` stays on the wire and in the logs; `tss why`
and `tss cancel` accept either that or the short id or the name you can see on screen.

The API underneath the wrapper is plain HTTP:

```bash
curl -X POST http://127.0.0.1:8000/v1/jobs -H 'content-type: application/json' -d '{
  "name": "gateway-to-gateway",
  "requirements": [{"product": "vehicle_gateway", "harness": "j1939"},
                   {"product": "asset_gateway"}],
  "payload": {"duration_s": 20}
}'
```

`requirements` is a **list of tag-subsets, one per device**. A device matches a spec if it satisfies
every key in it, so `{"product": "vehicle_gateway"}` matches a j1939 gateway and adding
`{"harness": "obd2"}` does not. Every device for one job comes from a single bench.

### The chaos fleet

Fifteen mock benches with failure profiles — crashers, zombies, flaky networks, hung jobs — plus a
job generator and an invariant checker running throughout:

```bash
just chaos                     # the merge gate: 15 agents, 100 jobs, 30% multi-device, 5 seeds
just chaos-seed 3              # replay one seed, verbosely
just chaos-profile zombie      # one failure mode in isolation
```

Every run prints its seed on the first line and again in any failure output, because a violation you
cannot replay is a violation you cannot fix.

---

## Tests

```bash
just test          # everything, including seeded chaos runs
just test-fast     # skips the slow chaos runs, for an inner loop
just lint
just chaos         # the merge gate — 5 seeds, zero invariant violations
just test-naive    # the deliberately-wrong implementations. FAILURES ARE THE POINT.
```

Four deliberately-wrong implementations are kept in `tests/`: a check-then-act claim (in two
variants, the second with the release-on-failure cleanup that looks fine in review), a fan-out that
requeues per device instead of per job, a scheduler that clears its wakeup flag after a pass instead
of before, and a reservation that takes hardware instead of withholding it. They fail the tests that
were written to catch them, which is the evidence those tests catch anything at all.

`just test-naive` is the human-facing demonstration — it swaps in the claim, fan-out and scheduler
foils and prints the failures for you to read. The reservation foil is not wired to an environment
variable; it is instantiated directly inside `tests/test_starvation.py`, where the deadlock it causes
is the assertion.

The demonstration is not the guarantee. Every line of that recipe is dash-prefixed, so it exits 0
whether the foils fail or pass — it could not tell you the tests had lost their teeth.
`tests/test_foils.py` is the assertion: it runs each foil environment in a subprocess with a clean
environment and requires a **non-zero** exit, so a foil that starts passing fails the build. It runs
in CI.

`just chaos` fails on four things, not one: any invariant violation, any job that never reached a
terminal state, **any profile that did not fire**, and **any run whose safety checks stopped
happening**. A run where nothing broke satisfies every safety invariant trivially, so the gate also
asserts what the run produced — that the crasher crashed, that heartbeats were dropped, that jobs
timed out and were requeued, that a bench re-registered, that a device sickened while its machine
stayed healthy, that a stale report was fenced, that a lost reply was recovered by the inverse
fence, and that at least one job passed. Every profile in the mix has a floor, and a test asserts
that: a profile nobody checks cannot be added.

The watcher that runs those checks has its own floor. It used to be one unguarded loop — a single
exception ended it, the teardown swallowed the error, and the run finished green having checked
nothing since. Now the body is guarded, the errors are counted and printed, and a finished run must
show safety checks consistent with how long it ran.

A failing run writes `chaos-seed-<n>-events.jsonl`: every event it recorded, kept because the
database it came from lived in a temporary directory that is already gone. CI uploads it. The seed
fixes the workload and the fleet — which bench gets which profile, every job spec, every crash and
dropped beat — but **not** the interleaving, which rides on real asyncio scheduling, real sockets and
a real SQLite. A replay re-runs the scenario, not the schedule.

CI runs lint, the suite, the foil meta-test and the chaos gate on every push to `main` and on every
pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

---

## What runs where

| | |
|---|---|
| `tss/core/` | store (all writes, the N-way claim), matcher, scheduler, reaper, events, invariants |
| `tss/api/` | `/v1/agents/*` for benches, `/v1/jobs`, `/v1/fleet`, `/v1/queue`, `/v1/stats` for humans, `WS /v1/events`, and `/` — the web view, one file in `api/static/` |
| `tss/agent/` | the daemon that runs on a bench: register, heartbeat, run, report |
| `tss/chaos/` | mock benches with failure profiles, the seeded runner, the ground-truth checker |
| `tss/cli/` | `tss fleet \| queue \| watch \| why \| drain \| unquarantine \| cancel`, and the `just submit` wrapper |

The `events` table is append-only and never pruned: retention and archival are deliberately out of
scope for the POC, the same call as schema migrations (`TSS-Architecture.md` §13.6). Its hot paths
are indexed so the fleet view and the invariant checker do not degrade as it grows, but on a
long-running deployment it grows without bound and would need a retention policy.

Nine invariants hold across every seed the chaos gate runs, and where each one is checked follows
from what can answer it.

Most are properties of the database and the scheduler, so they are read straight out of them
(`tss/core/invariants.py`): no device held by two jobs, an offline bench holding nothing, a job in
flight holding exactly the devices it asked for and no orphans pointing back at it, hung jobs ended
by the timeout sweep rather than by presence expiry, terminal outcomes never overwritten, and at
most one reservation at a time on one feasible bench.

Two cannot be checked against TSS's database at all. **I1** — at most one agent is authorized to own
a job, and exactly one result is ever accepted — and **I4** — a job never runs on devices lacking its
required capabilities — are questions about what is true on the bench, and TSS's record of what a
device is capable of is precisely what that agent claimed. Both are checked against the mock agents'
ground truth (`tss/chaos/invariants.py`).

**I3** is different again: every submitted job reaches a terminal state. That is liveness, so it is
not true at any given instant and cannot be sampled — it is checked once, at the end of a run,
against the list of jobs that were actually submitted (also `tss/chaos/invariants.py`).

See [`CLAUDE.md`](CLAUDE.md) for the list and [`TSS-Architecture.md`](TSS-Architecture.md) §3.8 for
why each is worded the way it is.
