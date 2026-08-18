set shell := ["bash", "-uc"]

venv := justfile_directory() / ".venv"
py   := venv / "bin/python"

default: test

# Create the venv and install the project (Python 3.12+, §pyproject).
install:
    uv sync --extra dev

# The full suite. Must pass before any commit (CLAUDE.md).
test *ARGS:
    {{py}} -m pytest {{ARGS}}

# Everything except the seeded chaos runs, for a fast inner loop.
test-fast *ARGS:
    {{py}} -m pytest -m "not slow" {{ARGS}}

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
    @{{py}} -m tss.cli.main fleet

# What is running and what is waiting.
queue:
    @{{py}} -m tss.cli.main queue

# The live view. This is the one to have on screen.
watch:
    @{{py}} -m tss.cli.main watch

# Why is that job not running yet?
why JOB:
    @{{py}} -m tss.cli.main why {{JOB}}

# Submit a job: `just submit smoke` or `just submit gw2gw 2`.
submit NAME="smoke" DEVICES="1" DURATION="10":
    @{{py}} -c "import json,sys,urllib.request as u;         reqs=[{'product':'vehicle_gateway'}]*int('{{DEVICES}}');         body=json.dumps({'name':'{{NAME}}','requirements':reqs,'payload':{'duration_s':float('{{DURATION}}')}}).encode();         r=u.Request('http://127.0.0.1:8000/v1/jobs',data=body,headers={'content-type':'application/json'});         print(u.urlopen(r).read().decode())"

# THE MERGE GATE: 15 agents x 2-4 devices, 100 jobs at 30% multi-device, 5 seeds.
# Zero invariant violations, or the build fails.
chaos:
    {{py}} -m tss.chaos.runner --agents 15 --jobs 100 --multi-pct 30 --seeds 5 --seed 1

# Replay one failing seed, with the fleet's log lines.
chaos-seed SEED:
    {{py}} -m tss.chaos.runner --agents 15 --jobs 100 --multi-pct 30 --seed {{SEED}} -v

# One profile in isolation — `just chaos-profile zombie`.
chaos-profile PROFILE AGENTS="6" JOBS="20":
    {{py}} -m tss.chaos.runner --agents {{AGENTS}} --jobs {{JOBS}} --profile {{PROFILE}} --seed 1 -v
