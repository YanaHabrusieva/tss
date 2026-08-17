"""Every timing constant in one place, env-overridable (§7.1).

The numbers are not arbitrary and the reasoning is quoted in the demo, so it lives
next to the value rather than in a doc that drifts:

  PRESENCE_TTL = 4 x HEARTBEAT_INTERVAL   tolerates 3 consecutive lost beats.
                                          At a 30% drop rate a false reap needs 4
                                          losses in a row: 0.3^4 = 0.8% per window.
                                          At 3x it would be 2.7%, at 2x 9%.
  REAPER_INTERVAL                         bounds detection to TTL + interval = 14s.
  LONGPOLL_TIMEOUT + HEARTBEAT_INTERVAL
      < PRESENCE_TTL                      an agent's own long-poll must never be
                                          able to let its presence lapse. Enforced
                                          in __post_init__, not just documented.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

ENV_PREFIX = "TSS_"


@dataclass(frozen=True)
class Config:
    # --- presence and dispatch (§7.1) ---------------------------------------
    heartbeat_interval_s: float = 3.0
    presence_ttl_s: float = 12.0
    reaper_interval_s: float = 2.0
    longpoll_timeout_s: float = 8.0

    # --- retry, poison, quarantine ------------------------------------------
    max_distinct_agents: int = 3  # distinct benches tried before DEAD_LETTER
    quarantine_threshold: int = 3  # consecutive failures

    # --- starvation and reservation (§3.4.1) --------------------------------
    starvation_threshold_s: float = 60.0
    unsatisfiable_timeout_s: float = 1800.0  # 30 min

    # --- scheduling -----------------------------------------------------------
    #: Backstop tick. The scheduler is event-driven; this is the safety net that
    #: bounds a lost wakeup to one second instead of forever (§7.3).
    scheduler_tick_s: float = 1.0

    # --- jobs ----------------------------------------------------------------
    default_max_duration_s: int = 600
    #: Matching is a backtracking search; real HIL tests need a handful of
    #: devices, and this keeps one absurd job from pinning a scheduling pass.
    max_resources_per_job: int = 8

    # --- storage (§3.3) -------------------------------------------------------
    db_path: str = "tss.db"
    busy_timeout_ms: int = 5000

    def __post_init__(self) -> None:
        if self.presence_ttl_s <= self.longpoll_timeout_s + self.heartbeat_interval_s:
            raise ValueError(
                "PRESENCE_TTL must exceed LONGPOLL_TIMEOUT + HEARTBEAT_INTERVAL "
                f"({self.presence_ttl_s} <= {self.longpoll_timeout_s} + "
                f"{self.heartbeat_interval_s}); an agent's own long-poll would be "
                "able to expire its presence lease (§7.1)."
            )
        if self.presence_ttl_s <= self.heartbeat_interval_s:
            raise ValueError("PRESENCE_TTL must exceed HEARTBEAT_INTERVAL")

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        """Build from TSS_* environment variables, e.g. TSS_PRESENCE_TTL_S=4."""
        src = os.environ if env is None else env
        kwargs: dict[str, object] = {}
        for f in fields(cls):
            raw = src.get(ENV_PREFIX + f.name.upper())
            if raw is None:
                continue
            kwargs[f.name] = str(raw) if f.type == "str" else _coerce(f.type, raw)
        return cls(**kwargs)  # type: ignore[arg-type]


def _coerce(type_name: object, raw: str) -> object:
    if type_name == "int":
        return int(raw)
    if type_name == "float":
        return float(raw)
    return raw


DEFAULT = Config()
