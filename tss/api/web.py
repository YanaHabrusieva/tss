"""`GET /` — the live fleet view, served by the service itself.

One self-contained HTML file, no build step, no framework, no external request
of any kind. It is the same feed `tss watch` renders: `WS /v1/events`, snapshot
then deltas, pushed. The page never polls — see the comment at the top of
`static/index.html` for why that is not a detail.

Served from the package rather than a CDN or a separate static host so that the
demo machine needs nothing but this process. `importlib.resources` rather than a
path relative to the repo, because the file has to be found the same way from an
editable checkout and from an installed wheel.
"""

from __future__ import annotations

from importlib import resources

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter()


def index_path() -> str:
    """Where the page lives. Also the packaging check: if the wheel ever ships
    without it, this raises here rather than 404ing in front of an audience."""
    path = resources.files("tss.api") / "static" / "index.html"
    return str(path)


@router.get("/", include_in_schema=False)
def index() -> FileResponse:
    try:
        path = index_path()
    except (FileNotFoundError, ModuleNotFoundError) as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"static page missing: {exc}") from exc
    # No cache: the page is tiny and a stale one during a demo is unexplainable.
    return FileResponse(path, media_type="text/html", headers={"Cache-Control": "no-store"})
