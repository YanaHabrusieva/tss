"""The foils must still fail. This is the test that makes `just test-naive` mean something.

`tests/naive_claim.py`, `naive_reap.py` and `naive_scheduler.py` are deliberately
wrong implementations kept in the repo as evidence: the claim tests, the fan-out
tests and the scheduler tests were written to catch specific bugs, and the proof
that they do is that they FAIL against code that has those bugs.

That evidence was a ritual, not an assertion. Every line of the `just test-naive`
recipe is dash-prefixed, so the recipe exits 0 whether the foils fail (the tests
have teeth) or pass (the tests have quietly lost them) — and CI never ran it at
all. A test suite that stopped catching its own bug would have looked exactly
like a normal green build.

So: for each foil, run the relevant files in a subprocess and assert a NON-ZERO
exit. If a foil ever passes, this fails, and the suite says which one and what it
was supposed to catch.

MARKED SLOW because each case is a pytest run of its own. The recipe stays as it
is, for demoing to a human — that output is meant to be read, not asserted on.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

#: (env var, value, the files whose failure is the evidence, what it must catch)
FOILS = [
    pytest.param(
        "TSS_CLAIM_IMPL",
        "naive",
        ["tests/test_allocation.py", "tests/test_concurrency.py"],
        "a check-then-act claim double-books a device under a real race",
        id="claim-naive",
    ),
    pytest.param(
        "TSS_CLAIM_IMPL",
        "naive_cleanup",
        ["tests/test_allocation.py", "tests/test_concurrency.py"],
        "the release-on-failure loop still strands devices when it dies mid-loop",
        # The variant that looks fine in review, and until now the one nobody
        # ever ran: the recipe only reaches it if a human sets the var by hand.
        id="claim-naive-cleanup",
    ),
    pytest.param(
        "TSS_REAP_IMPL",
        "naive",
        ["tests/test_fanout.py"],
        "a per-resource requeue bumps the epoch once per device, not once per job",
        id="reap-naive",
    ),
    pytest.param(
        "TSS_SCHEDULER_IMPL",
        "naive",
        ["tests/test_scheduler.py"],
        "clearing the wakeup flag after a pass loses the wakeup that arrived during it",
        id="scheduler-naive",
    ),
]


def _clean_env(name: str, value: str) -> dict[str, str]:
    """The outer environment must not leak in.

    If this suite is itself running under `TSS_CLAIM_IMPL=naive`, inheriting it
    would silently turn every case below into the same case — and they would all
    still "fail", so the assertion would pass while testing one foil four times.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("TSS_")}
    env[name] = value
    return env


@pytest.mark.slow
@pytest.mark.parametrize(("name", "value", "files", "catches"), FOILS)
def test_the_foil_still_fails(name: str, value: str, files: list[str], catches: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *files, "-x", "-q", "-p", "no:cacheprovider"],
        cwd=REPO,
        env=_clean_env(name, value),
        capture_output=True,
        text=True,
        timeout=900,
    )

    assert result.returncode != 0, (
        f"{name}={value} PASSED the suite.\n"
        f"That foil exists because {catches}. If it now passes, the tests that were "
        f"written to catch it no longer do — fix the tests, not this assertion.\n"
        f"--- stdout ---\n{result.stdout[-3000:]}"
    )


@pytest.mark.slow
def test_every_foil_env_the_recipe_uses_is_covered() -> None:
    """The recipe and this test must not drift apart: a foil added to one and not
    the other is a foil nobody checks."""
    recipe = (REPO / "justfile").read_text()
    body = recipe[recipe.index("test-naive") :].split("\nlint:")[0]

    covered = {f"{p.values[0]}={p.values[1]}" for p in FOILS}
    for name in ("TSS_CLAIM_IMPL", "TSS_REAP_IMPL", "TSS_SCHEDULER_IMPL"):
        assert name in body, f"{name} vanished from the just test-naive recipe"
        assert any(c.startswith(name) for c in covered), f"{name} is not covered here"
