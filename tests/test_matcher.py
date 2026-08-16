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


def test_the_matcher_touches_no_store(monkeypatch):
    """A guard against the boundary rotting: the matcher must not learn to read."""
    import tss.core.matcher as matcher_module

    assert not hasattr(matcher_module, "Store")
    assert "sqlite3" not in dir(matcher_module)
    assert "time" not in dir(matcher_module)
