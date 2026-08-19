# TSS — the guided tour

Three failure stories, each runnable exactly as written. They are the point of the design: what
happens when a machine dies, when a big job cannot fit, and when one device goes bad.

For install and everyday operation, see [`README.md`](README.md).

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
just submit soak 1 0 90     # takes one of bench-sf-01's two vehicle gateways
just submit gw2gw 2 0       # needs TWO vehicle gateways on one bench: nowhere to fit
sleep 6
just queue                  # -> gw2gw: RESERVING on bench-sf-01 (vg-02 held)
just submit smoke 1 0 20    # another vehicle-gateway job — it does NOT take the reserved device
just fleet                  # -> bench-sf-01 vg-02 still free, and still nobody's
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

## Watch one bad device get benched, without losing the machine

A dead J-Link costs you one device, not the bench it is plugged into. TSS decides which by the
**spread** of the failures: three infra errors clustered on one device quarantine that device;
three spread across several devices on one machine quarantine the machine (`TSS-Architecture.md`
§4.2). Here is the first case.

Start a service and one asset bench with two devices:

```bash
just start                  # terminal 1 — service in the background, page in the browser
just add bench-ag-01 0 2    # ag-01 and ag-02
```

Pin one device with a long job, so every failure below is forced onto the *other* one — that is
what makes the failures cluster:

```bash
just submit pin 0 1 120      # takes ag-01 for two minutes
just fleet
```

```
BENCH        STATE   BEAT  RESOURCES                            LOAD
bench-ag-01  online    2s  ag-01 BUSY pin (a0229) · ag-02 free   1/2
```

Now three jobs that report `infra_error` — the environment's fault, not the test's. `just submit-bad`
is a chaos control, not a customer verb: it scripts the outcome the bench will report, which is
something no real submitter gets to do.

```bash
just submit-bad flaky1 0 1 2
just submit-bad flaky2 0 1 2
just submit-bad flaky3 0 1 2
just fleet
```

```
BENCH        STATE   BEAT  RESOURCES                                   LOAD
bench-ag-01  online    3s  ag-01 BUSY pin (a0229) · ag-02 QUARANTINED   1/2
```

**`ag-02` is out and the bench is still ONLINE, still running `pin` on `ag-01`.** That is the whole
point: the machine is fine, one device on it is not, and conflating the two is how one unplugged
cable costs you a third of the fleet. The failed jobs are back in the queue waiting for capacity —
`infra_error` retries, because it was never the engineer's fault.

The device is now `unhealthy` with a `quarantined_at` timestamp, and that timestamp is what makes
this TSS's verdict rather than the bench's own health report. A bench can withdraw its own report by
saying it is healthy again; it cannot withdraw this one. Only an operator can:

```bash
.venv/bin/python -m tss.cli.main unquarantine bench-ag-01:ag-02
```

```
bench-ag-01:ag-02 is back in rotation (device, failure count reset).
```

The failure count resets to zero, so the device comes back with a clean slate rather than one bad
job away from going out again — and the queue notices immediately:

```
BENCH        STATE   BEAT  RESOURCES                                           LOAD
bench-ag-01  online    1s  ag-01 BUSY pin (a0229) · ag-02 BUSY flaky1 (06f68)   2/2
```

One command in, one device back, nothing else disturbed. When you are finished:

```bash
just stop
```
