# TSS — Test Scheduling Service

TSS is a job broker for physical hardware. A **testbed agent** is a machine; the **resources** it
owns are the firmware devices cabled to it. Agents register themselves and their inventory; firmware
engineers submit jobs declaring which devices they need; TSS matches jobs to free, compatible
devices — **all of them or none** — and hands the owning agent a time-limited lease it renews by
heartbeating. If an agent stops heartbeating, its lease expires, TSS marks it offline, takes back
every job holding any of its devices and re-queues them. A bench is not the unit of allocation: two
jobs run side by side on one machine as long as they need different devices.

- **Design and reasoning:** [`TSS-Architecture.md`](TSS-Architecture.md)
- **Invariants and the rules that follow from them:** [`CLAUDE.md`](CLAUDE.md)
- **How it was built with AI:** [`AILOG.md`](AILOG.md) — the log is the commit history; every
  message carries the prompt, what the AI got wrong, and what was overridden. `AILOG.md` explains
  the convention and quotes four of them verbatim.

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
just submit smoke                     # one device
just submit gateway-to-gateway 2      # TWO devices, both on ONE bench
just fleet                            # benches with their devices
just queue                            # what is running, what is waiting
just watch                            # the live view — this is the one to have on screen
just why smoke                        # why is that job not running yet?
```

The service also serves the live fleet view at **<http://127.0.0.1:8000/>** — the same feed `tss
watch` renders, in a browser: one self-contained page, pushed over `WS /v1/events`, never polled. It
needs no internet, loads nothing from a CDN, and re-snapshots from scratch if the connection drops.

Jobs read as `smoke-1 (2bb76)` on every human surface — the name you chose, plus enough of the id to
tell three `smoke` jobs apart. The full `job-2bb76a1c` stays on the wire and in the logs; `tss why`
and `tss cancel` accept either that or the short id or the name you can see on screen.

`just submit NAME DEVICES DURATION` is a convenience wrapper. The API underneath is plain HTTP:

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

## Watch it break

The point of the design is what happens when a machine dies, so:

```bash
just watch                     # leave this running
```

In another terminal, kill a bench that is running something:

```bash
pkill -f "tss.agent.daemon --id bench-sf-01"    # or: just kill bench-sf-01
```

The bench flips to **OFFLINE** on both `tss watch` and the web page, and its jobs re-queue **within
about 14 seconds** — `PRESENCE_TTL`
(12s) plus `REAPER_INTERVAL` (2s). The view is pushed over a WebSocket, so the change appears the
moment it happens rather than on the next poll. Nothing is lost: the jobs land on another bench and
finish, and if the killed agent ever comes back its results are rejected as stale.

### Watch a big job wait for a bench, without deadlocking it

A job needing two devices can starve while single-device jobs take capacity the instant it frees. TSS
handles that by **reserving** — withholding free devices on one bench until the big job can take them
all at once. A reservation never marks a device busy and never gives it an owner, which is why it
cannot deadlock (`TSS-Architecture.md` §3.4.1).

Restart the service with a shorter starvation threshold — 60s is right in production and too long to
stand in front of — and run two benches with *different hardware*, so it is unambiguous which jobs
could go where:

```bash
TSS_STARVATION_THRESHOLD_S=5 just serve                                        # terminal 1
just agent bench-sf-01 2                                                       # terminal 2
.venv/bin/python -m tss.agent.daemon --id bench-ag-01 --devices 2 \
    --product asset_gateway                                                    # terminal 3
```

Then, in a fourth:

```bash
just submit soak 1 90     # takes one of bench-sf-01's two vehicle gateways
just submit gw2gw 2       # needs TWO vehicle gateways on one bench: nowhere to fit
sleep 6
just queue                # -> gw2gw: RESERVING on bench-sf-01 (vg-02 held)
just submit smoke 1 20    # another vehicle-gateway job — it does NOT take the reserved device
just fleet                # -> bench-sf-01 vg-02 still free, and still nobody's
```

```
gw2gw (54cbe)  QUEUED  10s waited  — RESERVING on bench-sf-01 (vg-02)
  needs: 2 devices, all on ONE bench
           product=vehicle_gateway
           product=vehicle_gateway
  feasible benches (could ever satisfy this):
    bench-sf-01
      vg-01   BUSY soak (f347e) (11s / 600s budget)
      vg-02   free  RESERVED FOR YOU
  not feasible:
    bench-ag-01  only 0 healthy matching device(s), needs 2
  waiting on: vg-01 on bench-sf-01 to free (~589s of its budget left)
  nothing else can take those devices while you wait
```

Meanwhile an asset-gateway job dispatches to `bench-ag-01` immediately — the reservation withholds
devices on **one** bench, not the whole fleet:

```bash
curl -X POST http://127.0.0.1:8000/v1/jobs -H 'content-type: application/json' \
  -d '{"name":"ag-check","requirements":[{"product":"asset_gateway"}],"payload":{"duration_s":20}}'
```

When `soak` finishes, `gw2gw` takes both devices in a single transaction — all of them or none.

---

## Tests

```bash
just test          # everything, including seeded chaos runs
just test-fast     # skips the slow chaos runs, for an inner loop
just lint
just chaos         # the merge gate — 5 seeds, zero invariant violations
just test-naive    # the deliberately-wrong implementations. FAILURES ARE THE POINT.
```

`just test-naive` runs the real test suite against four foils kept in `tests/`: a check-then-act
claim, a fan-out that requeues per device instead of per job, a scheduler that clears its wakeup flag
after a pass instead of before, and a reservation that takes hardware instead of withholding it. They
all fail, which is the evidence that the tests catch what they were written for.

That recipe is for reading; `tests/test_foils.py` is the assertion. Every line of `just test-naive`
is dash-prefixed, so it exits 0 whether the foils fail or pass — it could not tell you the tests had
lost their teeth. The meta-test runs each foil in a subprocess with a clean environment and requires
a non-zero exit, so a foil that starts passing fails the build. It runs in CI.

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

CI runs lint, the suite, and the chaos gate on every push
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

---

## What runs where

| | |
|---|---|
| `tss/core/` | store (all writes, the N-way claim), matcher, scheduler, reaper, events, invariants |
| `tss/api/` | `/v1/agents/*` for benches, `/v1/jobs`, `/v1/fleet`, `/v1/queue` for humans, `WS /v1/events`, and `/` — the web view, one file in `api/static/` |
| `tss/agent/` | the daemon that runs on a bench: register, heartbeat, run, report |
| `tss/chaos/` | mock benches with failure profiles, the seeded runner, the ground-truth checker |
| `tss/cli/` | `tss fleet | queue | watch | why` |

The `events` table is append-only and never pruned: retention and archival are deliberately out of
scope for the POC, the same call as schema migrations (`TSS-Architecture.md` §13.6). Its hot paths
are indexed so the fleet view and the invariant checker do not degrade as it grows, but on a
long-running deployment it grows without bound and would need a retention policy.

Nine invariants hold across every seed the chaos gate runs. Seven are database or scheduler
properties (`tss/core/invariants.py`); **I1** — at most one agent is authorized to own a job, and
exactly one result is ever accepted — and **I4** — a job never runs on devices lacking its required
capabilities — cannot be checked against TSS's database at all, because TSS's record of what a device
is capable of is precisely what that agent claimed. Those two are checked against the mock agents'
ground truth (`tss/chaos/invariants.py`). See [`CLAUDE.md`](CLAUDE.md) for the list and
[`TSS-Architecture.md`](TSS-Architecture.md) §3.8 for why each is worded the way it is.
