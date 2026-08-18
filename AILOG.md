# AI log

**The AI log is the commit history.** That was a deliberate choice at the start of
this project and it is worth stating plainly, because the alternative — a
narrative written afterwards — is a summary of what someone remembers rather than
a record of what happened.

Every commit message in this repository carries three things:

1. **the prompt** it was built from, as `prompt:` at the top;
2. **what the AI produced that was wrong**, named specifically — not "there were
   some issues", but which line, which assumption, and what it cost;
3. **what was overridden and why**, including the places where the specification
   itself turned out to be wrong and was corrected in the same commit.

Read in order, `git log --reverse` is the process: seven build steps, then three
hardening commits from an adversarial review of the finished system. The
messages are long on purpose. A one-line commit message would have made this
file necessary.

What follows are four of them, **copied exactly**. They are the artifact; a
paraphrase would be the thing this file exists to avoid.

---

## Step 1 — the atomic claim

*The first thing built, and the one everything else rests on. The prompt asked for the tests before the implementation; what came back was the bug the spec predicts, twice.*

`git show 492bad8`

```
prompt: implement claim_all() so a job takes every resource it needs or
none. write the failing tests first — 50 OS threads on separate
connections claiming overlapping pairs, plus a forced failure on the
last resource of a set.

AI first produced a check-then-act loop with one autocommitted UPDATE
per resource. Pointing that out got the release-on-failure variant,
which still double-books — the window is between SELECT and UPDATE, not
in the cleanup. Both are kept in tests/naive_claim.py so the tests can
be run against them: every test in test_allocation.py and
test_concurrency.py fails against both variants, which is the evidence
they catch what they were written for.

Two additions I made beyond the spec: resource_count in the job UPDATE's
WHERE clause (makes I8 structural at claim time), and checking
double-booking against job_resources rather than resources.current_job_id
— the latter is a single column, so a second claimer overwrites the first
and the final state looks clean.
```

---

## Step 2 — presence and the fan-out

*Where a foil earned its place: the fan-out bug is silent, so the only way to know the test catches it is to watch it fail against a version that has it.*

`git show ac085a8`

```
presence: per-agent leases, guarded renewal, reaper fan-out

prompt: step 2 — register with inventory, heartbeat, presence sweep,
tss fleet. Five specific checks, including idle-bench reaping and
per-job fan-out.

Three corrections back to the spec, all found while building:

§7.3 claimed the fan-out bug "burns the retry budget" by appending
tried_agents three times. It doesn't — the append happens in the claim
(§3.3), so appending again on requeue would count one bench twice. The
real harm is 3x epoch bumps and, without a state guard, yanking back a
run the scheduler had already reassigned mid-sweep.

Vanished devices are `retired`, not `unhealthy` — present-but-broken and
not-there need different human responses. One rule whether or not the
device has history.

I5 said "an offline agent's resources are all free", which retired made
false. Restated negatively: nothing there is busy or holds a job. That
also fixed the reap, which was resetting unhealthy devices to free —
TSS inferring hardware state it can't observe. Reap now releases claims
and touches nothing else, which deleted the retired exception too.

tests/naive_reap.py is the foil for the fan-out (no DISTINCT, no state
guard): it reports ['job-X','job-X','job-X','job-Y'] where the correct
version requeues twice, and fails the tests it was written against.
```

---

## Step 3 — dispatch, and the bugs tests do not have opinions about

*Two bugs found by running the thing rather than by testing it. Correctness assertions have nothing to say about latency.*

`git show 4559a93`

```
scheduling: matcher, dispatch, long-poll, complete

prompt: step 3 — submit, match, dispatch, run, complete. Single-device
only; multi-requirement jobs rejected at the API rather than half-built.

Two spec corrections:

§3.4 step 6 said stop when no agent can satisfy the head of the queue.
Taken literally that idles a bench holding a free compatible device
while a matching job waits — the §1.2 utilisation target. The scheduler
skips and walks on. Safe at N=1 because a job is only skipped when no
free device matches it; at N>1 this is exactly what starves multi-device
jobs, which is what step 5's reservation guard is for.

Dead letters record outcome='infra_error', not outcome='dead_letter'.
State says what happened, outcome says whose problem it is; repeating the
state in the outcome discarded the FAILED-vs-INFRA_ERROR distinction on
the jobs that failed worst. Under the old code, SELECT ... WHERE
outcome='infra_error' returned nothing while dead-lettered jobs sat in
the table — an infra dashboard would have reported zero. That query is
now the assertion, and 'dead_letter' is out of both the outcome CHECK
and the Outcome enum so it cannot come back.

Two latency bugs the tests missed and the demo found, both agent-side
dead air: the daemon sat out a full heartbeat interval after registering
and again after finishing a job. 963ms -> 6ms steady state. The
sub-second test passed throughout because it measured an idle bench
already settled into a long-poll — the steady state, not the transitions
a user actually feels.

Also a schema version guard, since the dead-letter fix changed a CHECK
constraint: SCHEMA_VERSION stamped via PRAGMA user_version, checked once
per connection, refusing to open anything that doesn't match. Not
migrations — those are deliberately out of scope (§13.6) and the fleet
is self-healing enough to afford it, since agents push their own
inventory and presence leases expire on their own after a restart. What
the guard converts is a silent wrong-constraint database into a startup
failure: CREATE TABLE IF NOT EXISTS meant a stale file kept its old
constraints and accepted writes this build forbids, with nothing
anywhere erroring.

tests/naive_scheduler.py is the third foil: clear-after-pass instead of
clear-before-read, and it hangs the queue.
```

---

## Step 7 — an edge predicted from the state machine

*Read the two state machines together and the hole is visible before the code is: the reap wipes the state the retention check reads.*

`git show f568f28`

```
operators: drain, unquarantine, cancel — and quarantine survives a reboot

prompt: step 7 — drain, unquarantine for agents and devices, tss cancel,
feasibility exclusion of draining/quarantined benches. With a specific
edge named: quarantine must survive a presence expiry and a same-version
re-register.

That edge was wrong, exactly as predicted from reading the state
machine. The reap wipes state; the retention check read state; so a
quarantined bench that rebooted came back online un-quarantined and was
handed work nobody had fixed. Restarting a broken machine is the first
thing anyone tries — this was the likely path, not an exotic one.
Retention now keys off quarantined_at, which survives the reap, and
keeps the original timestamp: "quarantined since 14:02" is the useful
fact and a reboot must not reset it. Retention triggers on either
signal rather than the narrow one, because the failure mode of getting
this wrong is handing work to a broken bench.

Drain has no delivery protocol because the directive IS a state: it is
derived from agents.state on every heartbeat, not queued, so it
survives a lost response and a TSS restart for free. And there is no
DRAINING->OFFLINE transition — the daemon finishes its jobs and exits,
presence expires, and the ordinary sweep takes it the rest of the way.
One mechanism, again.

Unquarantine resets consecutive_fails so a bench comes back at zero
rather than one bad job from going out again. Un-quarantining a
retired device is refused: retired is not a health state, and
reviving one would invent hardware.

Feasibility already excluded draining and quarantined benches, so that
was two tests rather than a change: a reserver whose target drains
retargets on the next pass, and a job whose only capable bench is
quarantined runs after tss unquarantine.

Also: conftest's submit takes an optional `now`, ending the
synthetic-vs-real clock mismatch that cost three debugging rounds.
```

---

## The full record

Four messages is a sample. The rest are in the history, and the ones not
excerpted here include the sabotage exercise (§8.1 of `TSS-Architecture.md`),
where deliberately breaking three things showed that two of them were caught by
nothing — and what was added as a result.

```bash
git log --reverse                 # the whole process, in order
git log --format='%h %s'          # just the shape of it
git show <sha>                    # any one prompt and what it produced
```

Two conventions worth knowing when reading it:

- **Foils are kept, not deleted.** `tests/naive_claim.py`, `naive_reap.py`,
  `naive_scheduler.py` and `naive_reservation.py` are the wrong implementations
  the AI produced first. `just test-naive` runs the real suite against them and
  they fail — which is the only evidence that the tests catch what they were
  written for.
- **Corrections to the specification are in the commit that made them.**
  `TSS-Architecture.md` was edited during the build several times; each edit is
  argued in the message of the commit that changed the code alongside it.
