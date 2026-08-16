set shell := ["bash", "-uc"]

venv := justfile_directory() / ".venv"
py   := venv / "bin/python"

default: test

# Create the venv and install the project (Python 3.12+, §pyproject).
install:
    uv venv --python 3.12 {{venv}}
    uv pip install --python {{py}} -e ".[dev]"

# The full suite. Must pass before any commit (CLAUDE.md).
test *ARGS:
    {{py}} -m pytest {{ARGS}}

# The same tests against the check-then-act claim in tests/naive_claim.py.
# FAILURES ARE THE POINT: this is the evidence that test_allocation.py and
# test_concurrency.py catch the bug they were written for (§7.5).
#   TSS_CLAIM_IMPL=naive_cleanup just test-naive   # the release-on-failure variant
test-naive *ARGS:
    @echo "--- running against the NAIVE claim/reap/scheduler: failures here are expected ---"
    -TSS_CLAIM_IMPL="${TSS_CLAIM_IMPL:-naive}" {{py}} -m pytest tests/test_allocation.py tests/test_concurrency.py {{ARGS}}
    -TSS_REAP_IMPL=naive {{py}} -m pytest tests/test_fanout.py {{ARGS}}
    -TSS_SCHEDULER_IMPL=naive {{py}} -m pytest tests/test_scheduler.py {{ARGS}}

lint:
    {{py}} -m ruff check .
    {{py}} -m ruff format --check .

fmt:
    {{py}} -m ruff format .
    {{py}} -m ruff check --fix .

# The service: API + reaper, one process.
serve PORT="8000":
    {{py}} -m uvicorn tss.api.app:app --host 127.0.0.1 --port {{PORT}}

# One testbed agent with N devices cabled to it.
agent ID="bench-sf-01" DEVICES="3":
    {{py}} -m tss.agent.daemon --id {{ID}} --devices {{DEVICES}}

# Benches and their devices.
fleet:
    {{py}} -m tss.cli.main fleet

# --- arriving in later steps of the build order (§10) -------------------------

# step 5: 15 agents x 2-4 devices, 100 jobs at 30% multi-device, 5 seeds — the merge gate
chaos:
    @echo "not yet: the chaos suite lands in step 5 (§10)."
    @exit 1
