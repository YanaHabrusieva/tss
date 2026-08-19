"""Failure profiles — each one targets a specific design decision (§3.7).

A chaos suite that just adds randomness tells you the system survived some
randomness. Each profile here exists to break one mechanism, so a violation
points at a decision rather than at "the fleet".

| profile        | behaviour                          | what it proves            |
|----------------|------------------------------------|---------------------------|
| clean          | always works                       | the happy path            |
| crasher        | whole machine dies mid-job, p=0.3  | presence expiry -> FAN-OUT|
| flaky_network  | drops 30% of heartbeats            | TTL tolerates loss (§7.1) |
| zombie         | silent past expiry, then reports   | EPOCH FENCING             |
| slowpoke       | jobs take 3-10x expected           | slow != dead              |
| hung           | heartbeats forever, never finishes | JOB-TIMEOUT sweep         |
| idle_death     | vanishes without ever taking a job | idle agents are reaped    |
| resource_flap  | one device sickens, machine fine   | device vs agent health    |
| flapper        | re-registers every few seconds     | registration is idempotent|
| deaf           | /start lands; its REPLY is dropped | the INVERSE FENCE (§3.5)  |
| liar           | declares capabilities it lacks     | graceful failure + I4     |

`deaf` is the one that exercises the inverse fence under load. `flaky_network`
drops REQUESTS, which TSS never sees and the presence TTL absorbs; `deaf` lets the
request land and discards the REPLY, so TSS commits a /start the bench never
learns about. The bench then holds no job and mentions none, while TSS believes it
is running one — the disagreement no lease can detect, because the bench is
perfectly healthy. Only the inverse fence closes it.

It targets /start and nothing else. Losing the reply to /complete proves nothing:
the result is already committed and the job already terminal, so there is nothing
for anyone to take back. Before this profile existed the inverse fence was covered
by two hand-scripted sequences and never met randomized load at all.

`liar` is deliberately NOT in the gate mix. It produces I4 violations by
construction — that is the profile working, not TSS failing — and putting it in
the merge gate would force an allow-list. A gate with exceptions stops being a
gate. It gets its own test, which asserts the useful property instead: repeated
failures quarantine the bench rather than TSS routing to it forever.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    proves: str

    #: p(die) each time this bench starts a job. It stays dead until it reboots.
    crash_probability: float = 0.0
    reboot_after_ttls: float = 2.5
    #: p(this heartbeat never leaves the bench)
    heartbeat_drop_rate: float = 0.0
    #: multiply the payload's duration by a factor drawn from this range
    duration_multiplier: tuple[float, float] = (1.0, 1.0)
    #: run forever: the job-timeout sweep is the only thing that can end it
    never_completes: bool = False
    #: go quiet for this many TTLs while running, then report anyway
    silent_ttls: float = 0.0
    #: register, heartbeat, then vanish — without ever accepting work
    idle_death_after_ttls: float = 0.0
    #: re-register this often, whatever it is in the middle of
    flap_every_ttls: float = 0.0
    #: report one device unhealthy, then healthy again, on this cycle
    flap_device_every_ttls: float = 0.0
    #: p(the reply to /start is discarded after the server has already recorded
    #: it) — a lost RESPONSE, not a lost request
    deaf_probability: float = 0.0
    #: declare capabilities the hardware does not have
    lies_about_capabilities: bool = False
    #: fail every job it is given (a liar's tests cannot pass)
    fails_every_job: bool = False

    @property
    def can_pass_a_job(self) -> bool:
        """Could a bench on this profile ever report a PASSED result?

        `hung` never finishes, `liar` fails everything, `idle_death` vanishes
        before taking work, and a `zombie` is ALWAYS fenced out — it goes silent
        past its lease on every job, so its report is always rejected and the job
        always goes back to the queue. A fleet made only of these cannot produce
        a pass, so the gate's "at least one job passed" floor does not apply to
        it. In any mixed fleet some bench can, and then it does.
        """
        return not (
            self.never_completes
            or self.fails_every_job
            or self.idle_death_after_ttls
            or self.silent_ttls
        )

    @property
    def can_die(self) -> bool:
        """Does this profile stop heartbeating on its own? (-> agent.offline)"""
        return bool(self.crash_probability or self.idle_death_after_ttls or self.silent_ttls)

    @property
    def can_strand_a_job(self) -> bool:
        """...while holding one, so the reap has something to requeue."""
        return bool(self.crash_probability or self.silent_ttls)


CLEAN = Profile("clean", "the happy path")
CRASHER = Profile("crasher", "presence expiry and the fan-out requeue", crash_probability=0.3)
FLAKY_NETWORK = Profile(
    "flaky_network", "PRESENCE_TTL tolerates transient loss", heartbeat_drop_rate=0.3
)
ZOMBIE = Profile("zombie", "epoch fencing rejects the stale report", silent_ttls=2.0)
SLOWPOKE = Profile("slowpoke", "slow is not dead", duration_multiplier=(3.0, 10.0))
HUNG = Profile("hung", "the job-timeout sweep fires independently", never_completes=True)
IDLE_DEATH = Profile(
    "idle_death",
    "idle agents are reaped too — the gap most designs have",
    idle_death_after_ttls=1.0,
)
RESOURCE_FLAP = Profile(
    "resource_flap", "device health and machine health are different", flap_device_every_ttls=3.0
)
FLAPPER = Profile(
    "flapper", "registration is idempotent; no duplicates, no orphans", flap_every_ttls=4.0
)
DEAF = Profile(
    "deaf",
    "the inverse fence takes back a job the bench never learned it owned",
    deaf_probability=0.5,
)
LIAR = Profile(
    "liar",
    "graceful failure, then quarantine — and I4 against ground truth",
    lies_about_capabilities=True,
    fails_every_job=True,
)

ALL: dict[str, Profile] = {
    p.name: p
    for p in (
        CLEAN,
        CRASHER,
        FLAKY_NETWORK,
        ZOMBIE,
        SLOWPOKE,
        HUNG,
        IDLE_DEATH,
        RESOURCE_FLAP,
        FLAPPER,
        DEAF,
        LIAR,
    )
}

#: The merge gate's fleet. Weighted so most benches work and the fleet can still
#: finish its queue, while every failure mechanism is exercised. `liar` is absent
#: on purpose — see the module docstring.
MIXED: tuple[tuple[Profile, int], ...] = (
    (CLEAN, 5),
    (CRASHER, 3),
    (FLAKY_NETWORK, 2),
    (ZOMBIE, 1),
    (SLOWPOKE, 1),
    (HUNG, 1),
    (IDLE_DEATH, 1),
    (RESOURCE_FLAP, 1),
    (FLAPPER, 1),
    (DEAF, 2),
)


def fleet_profiles(count: int, mix: str = "mixed") -> list[Profile]:
    """Assign a profile to each bench. Deterministic given the count and mix."""
    if mix != "mixed":
        if mix not in ALL:
            raise ValueError(f"unknown profile {mix!r}; choose from {sorted(ALL)} or 'mixed'")
        return [ALL[mix]] * count

    # Interleaved, not blocked: taking one of each in turn means every profile
    # appears as soon as the fleet is big enough, instead of the rarest ones
    # falling off the end when `count` lands mid-cycle. A profile silently
    # missing from the gate is a mechanism silently untested.
    remaining = {profile.name: weight for profile, weight in MIXED}
    by_name = {profile.name: profile for profile, _ in MIXED}
    expanded: list[Profile] = []
    while any(remaining.values()):
        for name in list(remaining):
            if remaining[name]:
                remaining[name] -= 1
                expanded.append(by_name[name])
    return [expanded[i % len(expanded)] for i in range(count)]
