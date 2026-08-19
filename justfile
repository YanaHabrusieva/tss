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

# Start the service in the BACKGROUND and open the live page. `just stop` stops it.
start PORT="8000":
    #!/usr/bin/env bash
    set -uo pipefail
    url="http://127.0.0.1:{{PORT}}/"

    # Poll until the service answers, or give up. One process, not one per attempt.
    wait_up() {
        {{py}} - "$1" "$2" <<'PY' 2>/dev/null
    import sys, time, urllib.request
    url, deadline = sys.argv[1], time.time() + float(sys.argv[2])
    while True:
        try:
            urllib.request.urlopen(url, timeout=1)
            sys.exit(0)
        except Exception:
            if time.time() >= deadline:
                sys.exit(1)
            time.sleep(0.2)
    PY
    }
    browse() { open "$1" >/dev/null 2>&1 || xdg-open "$1" >/dev/null 2>&1 || true; }

    # A second `just start` must never stack another uvicorn onto the same port:
    # two schedulers and two reapers on one database is a demo that fails in a way
    # nobody can explain from the screen.
    if wait_up "$url" 0; then
        echo "TSS already running at $url"
        browse "$url"
        exit 0
    fi

    mkdir -p .demo-logs
    # 5s is the DEMO starvation threshold, not production: 60s is right on a real
    # fleet and far too long to stand in front of while a big job waits to reserve.
    TSS_STARVATION_THRESHOLD_S=5 nohup {{py}} -m uvicorn tss.api.app:app \
        --host 127.0.0.1 --port {{PORT}} >> .demo-logs/serve.log 2>&1 &

    if ! wait_up "$url" 15; then
        echo "WARNING: TSS did not answer at $url within 15s. Last lines of the log:" >&2
        tail -n 8 .demo-logs/serve.log >&2 || true
        exit 1
    fi
    browse "$url"
    echo "TSS is running at $url — log: .demo-logs/serve.log — stop it with: just stop"

# Stop everything and delete the demo's state. Safe to run cold.
stop:
    #!/usr/bin/env bash
    set -uo pipefail
    pkill -f "tss.agent.daemon" || true
    pkill -f "uvicorn tss.api.app:app" || true
    rm -f tss.db tss.db-wal tss.db-shm bench.json
    rm -rf .demo-logs
    echo "demo stopped — benches and service killed, database and logs removed."

# Add a bench in the BACKGROUND: `just add bench-sf-01 2 1` -> vg-01 vg-02 ag-01.
add NAME VG="2" AG="0":
    @mkdir -p .demo-logs
    @{{py}} -c "import json,sys; vg=int(sys.argv[1]); ag=int(sys.argv[2]); sys.exit('just add: {{NAME}} needs at least one device — try: just add {{NAME}} 2 0') if vg+ag < 1 else None; items=[{'id':f'vg-{i:02d}','capabilities':{'product':'vehicle_gateway'}} for i in range(1,vg+1)]+[{'id':f'ag-{i:02d}','capabilities':{'product':'asset_gateway'}} for i in range(1,ag+1)]; open(sys.argv[3],'w').write(json.dumps(items,indent=1))" {{VG}} {{AG}} .demo-logs/{{NAME}}.inventory.json
    @nohup {{py}} -m tss.agent.daemon --id {{NAME}} --inventory .demo-logs/{{NAME}}.inventory.json >> .demo-logs/{{NAME}}.log 2>&1 &
    @echo "{{NAME}}: {{VG}} vehicle_gateway + {{AG}} asset_gateway — log: .demo-logs/{{NAME}}.log"

# Stop a bench started with `just add` — the same pattern the README kills by.
kill NAME:
    @pkill -f "tss.agent.daemon --id {{NAME}}" && echo "{{NAME}} killed — it goes OFFLINE in about 14s." || echo "no bench called {{NAME}} is running."

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

# Submit a job: `just submit smoke 1 0` -> 1 vehicle gateway. Same order as `just add`.
submit NAME="smoke" VG="1" AG="0" DURATION="10":
    @{{py}} -m tss.cli.submit {{NAME}} {{VG}} {{AG}} {{DURATION}}

# A job that reports infra_error. A CHAOS CONTROL, not a customer verb: a customer
# does not get to declare their own job's outcome, which is why this is separate.
submit-bad NAME="flaky" VG="1" AG="0" DURATION="10":
    @{{py}} -m tss.cli.submit {{NAME}} {{VG}} {{AG}} {{DURATION}} --outcome infra_error

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
