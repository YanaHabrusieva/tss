"""Human- and CI-facing HTTP surface (§3.2, §6).

Low frequency, low stakes, separate router. `/v1/jobs`, `/v1/queue` and the
operator verbs arrive with the scheduler in step 3.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from tss.api.deps import get_store
from tss.core.models import FleetView
from tss.core.store import Store

router = APIRouter(prefix="/v1", tags=["client"])

StoreDep = Annotated[Store, Depends(get_store)]


@router.get("/fleet", response_model=FleetView)
async def fleet(store: StoreDep) -> FleetView:
    """Benches, each with the devices cabled to it (§3.9) — including benches
    that died while holding nothing, which is the case a bench-level view
    misses."""
    return store.fleet()
