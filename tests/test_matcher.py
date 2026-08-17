"""The matcher, unit-tested with no database and no clock (§8, "Unit").

Every test here builds `Resource` objects by hand. If any of this needed a store
or a running service, the boundary in §3.2 would be wrong — the interesting part
of scheduling is "does this device satisfy this spec, and which of the ones that
do should we use", and that should be testable without SQL.
"""

from __future__ import annotations

from tss.core import matcher
from tss.core.models import Resource, ResourceState

VG_J1939 = {"product": "vehicle_gateway", "hw_rev": "B", "harness": "j1939"}
VG_OBD2 = {"product": "vehicle_gateway", "hw_rev": "C", "harness": "obd2"}
AG = {"product": "asset_gateway", "hw_rev": "A"}


def resource(
    rid: str,
    caps: dict,
    *,
    agent: str = "bench-01",
    state: ResourceState = ResourceState.FREE,
    last_assigned_at: float | None = None,
) -> Resource:
    return Resource(
        id=f"{agent}:{rid}",
        agent_id=agent,
        capabilities=caps,
        state=state,
        last_assigned_at=last_assigned_at,
    )


# ------------------------------------------------------------ the subset test
def test_a_spec_matches_a_device_that_has_every_key_it_names():
    vg = resource("vg-01", VG_J1939)
    assert matcher.satisfies(vg, {"product": "vehicle_gateway"})
    assert matcher.satisfies(vg, {"product": "vehicle_gateway", "harness": "j1939"})
    assert matcher.satisfies(vg, {}), "an empty spec matches anything"


def test_extra_capabilities_on_the_device_are_fine_but_missing_ones_are_not():
    vg = resource("vg-01", VG_J1939)
    # The spec is a filter, not an equality check: hw_rev B is not mentioned.
    assert matcher.satisfies(vg, {"harness": "j1939"})
    assert not matcher.satisfies(vg, {"harness": "obd2"})
    assert not matcher.satisfies(vg, {"product": "asset_gateway"})
    assert not matcher.satisfies(vg, {"bezel_colour": "black"}), "unknown key cannot match"


# --------------------------------------------------------- what is offerable
def test_only_free_devices_are_offerable_and_only_working_ones_are_capacity():
    pool = [
        resource("vg-01", VG_J1939),
        resource("vg-02", VG_J1939, state=ResourceState.BUSY),
        resource("vg-03", VG_J1939, state=ResourceState.UNHEALTHY),
        resource("vg-04", VG_J1939, state=ResourceState.RETIRED),
    ]

    assert [r.id for r in matcher.offerable(pool)] == ["bench-01:vg-01"]
    # busy is capacity that happens to be taken; unhealthy and retired are not
    # capacity at all, which is the distinction the feasibility filter needs.
    assert [r.id for r in matcher.installed(pool)] == ["bench-01:vg-01", "bench-01:vg-02"]


def test_a_job_cannot_be_matched_to_a_broken_or_departed_device():
    for state in (ResourceState.UNHEALTHY, ResourceState.RETIRED, ResourceState.BUSY):
        pool = [resource("vg-01", VG_J1939, state=state)]
        assert matcher.match_on_agent([{"product": "vehicle_gateway"}], pool) is None, state


# ------------------------------------------------------------------- matching
def test_a_single_device_job_takes_a_matching_device():
    pool = [resource("ag-01", AG), resource("vg-01", VG_J1939)]

    match = matcher.match_on_agent([{"product": "vehicle_gateway"}], pool)

    assert match is not None
    assert match.resource_ids == ["bench-01:vg-01"]
    assert match.agent_id == "bench-01"


def test_no_match_when_nothing_satisfies_the_spec():
    pool = [resource("vg-01", VG_OBD2)]
    assert matcher.match_on_agent([{"harness": "j1939"}], pool) is None


def test_each_requirement_consumes_a_distinct_device():
    """Two requirements must not both be satisfied by the same device."""
    one = [resource("vg-01", VG_J1939)]
    two = [resource("vg-01", VG_J1939), resource("vg-02", VG_J1939)]
    spec = [{"product": "vehicle_gateway"}, {"product": "vehicle_gateway"}]

    assert matcher.match_on_agent(spec, one) is None, "one device cannot satisfy two specs"
    match = matcher.match_on_agent(spec, two)
    assert match is not None
    assert len(set(match.resource_ids)) == 2


# ---------------------------------------------------------------- co-location
def test_co_location_is_one_predicate():
    """§1.2: every resource for one job comes from a single agent. Relaxing this
    should be deleting one predicate, not unpicking assumptions from the claim,
    the reaper and the reservation logic."""
    mixed = [
        resource("vg-01", VG_J1939, agent="bench-01"),
        resource("vg-01", VG_J1939, agent="bench-02"),
    ]
    assert not matcher.co_located(mixed)
    assert matcher.co_located(mixed[:1])
    assert matcher.co_located([])

    spec = [{"product": "vehicle_gateway"}, {"product": "vehicle_gateway"}]
    assert matcher.match_on_agent(spec, mixed) is None


def test_ranking_groups_by_bench_so_a_match_is_always_co_located():
    by_agent = {
        "bench-01": [resource("vg-01", VG_J1939, agent="bench-01")],
        "bench-02": [resource("vg-01", VG_J1939, agent="bench-02")],
    }

    matches = matcher.rank_matches([{"product": "vehicle_gateway"}], by_agent)

    assert len(matches) == 2
    for match in matches:
        assert matcher.co_located(match.resources)


# ------------------------------------------------------------------------ LRU
def test_lru_picks_the_device_that_has_gone_longest_without_work():
    """Not first-fit. First-fit hammers one device while its siblings idle: that
    unit wears out first, and if it is subtly broken every job fails and you
    conclude the firmware is broken."""
    pool = [
        resource("vg-01", VG_J1939, last_assigned_at=500.0),
        resource("vg-02", VG_J1939, last_assigned_at=100.0),  # oldest
        resource("vg-03", VG_J1939, last_assigned_at=300.0),
    ]

    match = matcher.match_on_agent([{"product": "vehicle_gateway"}], pool)

    assert match.resource_ids == ["bench-01:vg-02"]


def test_a_device_that_has_never_run_anything_sorts_oldest():
    pool = [
        resource("vg-01", VG_J1939, last_assigned_at=100.0),
        resource("vg-02", VG_J1939, last_assigned_at=None),  # never used
    ]

    match = matcher.match_on_agent([{"product": "vehicle_gateway"}], pool)

    assert match.resource_ids == ["bench-01:vg-02"]


def test_first_fit_would_pick_differently_which_is_the_whole_point():
    """The device declared first is the most recently used one. A first-fit
    matcher returns vg-01 here; LRU returns vg-02."""
    pool = [
        resource("vg-01", VG_J1939, last_assigned_at=900.0),
        resource("vg-02", VG_J1939, last_assigned_at=100.0),
    ]

    first_fit = next(r for r in pool if matcher.satisfies(r, {"product": "vehicle_gateway"}))
    match = matcher.match_on_agent([{"product": "vehicle_gateway"}], pool)

    assert first_fit.id == "bench-01:vg-01"
    assert match.resource_ids == ["bench-01:vg-02"]


def test_benches_are_ranked_by_how_long_their_devices_have_idled():
    by_agent = {
        "bench-01": [resource("vg-01", VG_J1939, agent="bench-01", last_assigned_at=900.0)],
        "bench-02": [resource("vg-01", VG_J1939, agent="bench-02", last_assigned_at=100.0)],
        "bench-03": [resource("vg-01", VG_J1939, agent="bench-03", last_assigned_at=None)],
    }

    matches = matcher.rank_matches([{"product": "vehicle_gateway"}], by_agent)

    assert [m.agent_id for m in matches] == ["bench-03", "bench-02", "bench-01"]


def test_ranking_skips_benches_that_cannot_satisfy_the_job():
    by_agent = {
        "bench-01": [resource("ag-01", AG, agent="bench-01")],
        "bench-02": [resource("vg-01", VG_J1939, agent="bench-02")],
    }

    matches = matcher.rank_matches([{"product": "vehicle_gateway"}], by_agent)

    assert [m.agent_id for m in matches] == ["bench-02"]


# ------------------------------------------------- N-of-M: the greedy failure
def test_a_permissive_spec_must_not_eat_the_device_a_strict_one_needs():
    """The bug greedy has and backtracking does not.

    One bench, two vehicle gateways on different harnesses. The job needs "any
    VG" and "a j1939 VG" — vg-02 + vg-01 satisfies it exactly. A greedy walk
    hands vg-01 (which sorts first by LRU) to the permissive spec, then finds
    nothing left with a j1939 harness and reports no match: a job left queued in
    front of hardware that could run it.

    This is maximum bipartite matching, and greedy is not an algorithm for it.
    """
    pool = [
        resource("vg-01", VG_J1939, last_assigned_at=100.0),  # sorts first
        resource("vg-02", VG_OBD2, last_assigned_at=200.0),
    ]
    specs = [{"product": "vehicle_gateway"}, {"product": "vehicle_gateway", "harness": "j1939"}]

    match = matcher.match_on_agent(specs, pool)

    assert match is not None, "a valid assignment exists: vg-02 for spec 0, vg-01 for spec 1"
    assert match.resource_ids == ["bench-01:vg-02", "bench-01:vg-01"]


def test_the_same_job_matches_whichever_order_its_requirements_are_written_in():
    """The asymmetry is the tell. Under greedy, the strict-first ordering happens
    to work and the permissive-first ordering does not — so whether a job runs
    depends on the order an engineer typed its requirements, which is not
    something anyone could be expected to know."""
    pool = [
        resource("vg-01", VG_J1939, last_assigned_at=100.0),
        resource("vg-02", VG_OBD2, last_assigned_at=200.0),
    ]
    strict_first = [{"harness": "j1939"}, {"product": "vehicle_gateway"}]
    permissive_first = [{"product": "vehicle_gateway"}, {"harness": "j1939"}]

    one = matcher.match_on_agent(strict_first, pool)
    two = matcher.match_on_agent(permissive_first, pool)

    assert one is not None and two is not None
    assert set(one.resource_ids) == set(two.resource_ids) == {"bench-01:vg-01", "bench-01:vg-02"}


def test_backtracking_gives_up_only_when_no_assignment_exists():
    pool = [
        resource("vg-01", VG_OBD2),
        resource("vg-02", VG_OBD2),
    ]
    # Two devices, but only obd2 ones: a j1939 requirement genuinely cannot be met.
    specs = [{"product": "vehicle_gateway"}, {"harness": "j1939"}]
    assert matcher.match_on_agent(specs, pool) is None
    # ...and one j1939 device cannot satisfy two j1939 specs.
    one_each = [resource("vg-01", VG_J1939), resource("vg-02", VG_OBD2)]
    assert matcher.match_on_agent([{"harness": "j1939"}, {"harness": "j1939"}], one_each) is None


def test_a_three_device_job_takes_three_distinct_devices():
    pool = [resource(f"vg-{i:02d}", VG_J1939, last_assigned_at=float(i)) for i in range(1, 5)]
    specs = [{"harness": "j1939"}] * 3

    match = matcher.match_on_agent(specs, pool)

    assert len(set(match.resource_ids)) == 3
    assert match.resource_ids == ["bench-01:vg-01", "bench-01:vg-02", "bench-01:vg-03"], (
        "still LRU-ordered: backtracking tries the oldest device first"
    )


def test_a_mixed_job_matches_across_products():
    """The shape the two-level model exists for: a gateway-to-gateway test needing
    a heavy-duty VG and an AG on the SAME bench (§3.4)."""
    pool = [
        resource("vg-01", VG_OBD2),
        resource("vg-02", VG_J1939),
        resource("ag-01", AG),
    ]
    specs = [{"product": "vehicle_gateway", "harness": "j1939"}, {"product": "asset_gateway"}]

    match = matcher.match_on_agent(specs, pool)

    assert match.resource_ids == ["bench-01:vg-02", "bench-01:ag-01"]


# ------------------------------------------------------- the feasibility filter
def test_feasibility_ignores_whether_devices_are_free_right_now():
    """§3.4.1 step 1, and the easy one to skip. A bench with three healthy VGs can
    satisfy a 3-VG job eventually, however busy it is at this instant."""
    busy = [resource(f"vg-{i:02d}", VG_J1939, state=ResourceState.BUSY) for i in range(1, 4)]

    assert matcher.could_ever_satisfy([{"harness": "j1939"}] * 3, busy)
    assert matcher.match_on_agent([{"harness": "j1939"}] * 3, busy) is None, "not right now"


def test_a_bench_with_too_few_healthy_devices_can_never_satisfy_the_job():
    """Reserving here would idle a device forever, for nothing."""
    pool = [
        resource("vg-01", VG_J1939),
        resource("vg-02", VG_J1939, state=ResourceState.UNHEALTHY),
        resource("vg-03", VG_J1939, state=ResourceState.RETIRED),
    ]

    assert matcher.could_ever_satisfy([{"harness": "j1939"}], pool)
    assert not matcher.could_ever_satisfy([{"harness": "j1939"}] * 2, pool), (
        "broken and departed devices are not capacity"
    )


def test_feasibility_uses_the_same_assignment_logic_as_matching():
    """A bench that only *looks* feasible by counting devices is not feasible."""
    pool = [resource("vg-01", VG_OBD2), resource("vg-02", VG_OBD2)]

    assert not matcher.could_ever_satisfy([{"harness": "j1939"}, {"harness": "obd2"}], pool)


def test_the_matcher_touches_no_store(monkeypatch):
    """A guard against the boundary rotting: the matcher must not learn to read."""
    import tss.core.matcher as matcher_module

    assert not hasattr(matcher_module, "Store")
    assert "sqlite3" not in dir(matcher_module)
    assert "time" not in dir(matcher_module)
