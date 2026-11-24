"""Web UI and comparison API for the two-origin layer check.

Exposes ``/api/check`` (start / poll / cancel), health and read-only origin
state views.  Each check resets both origins, locks their strong ETags, then
compares metadata only.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .checker import BLOCK, SCENARIOS, CheckManager, CheckParams

STATIC_DIR = Path(__file__).resolve().parent / "static"

DEFAULT_LAYER_BYTES = int(os.environ.get("DRILL_LAYER_BYTES", 8 * 1024 * 1024))
DEFAULT_ORIGIN_A = os.environ.get("DRILL_ORIGIN_A", "http://127.0.0.1:8001")
DEFAULT_ORIGIN_B = os.environ.get("DRILL_ORIGIN_B", "http://127.0.0.1:8002")


class CheckRequest(BaseModel):
    size_bytes: int = Field(
        default=DEFAULT_LAYER_BYTES, ge=BLOCK, le=128 * 1024 * 1024
    )
    scenario: str = Field(
        default="match",
        pattern=r"^(match|single_block_diff|swap_midway|origin_fault)$",
    )
    diff_block: Optional[int] = Field(default=None, ge=-(128 * 1024 * 1024 // BLOCK))
    max_requests_per_side: int = Field(default=16, ge=2, le=16)
    max_combined_body_bytes: int = Field(default=16 * 1024, ge=128, le=16 * 1024)


def create_app(
    origin_a: Optional[str] = None, origin_b: Optional[str] = None
) -> FastAPI:
    app = FastAPI(
        title="跨机房镜像层核对",
        description=(
            "在网页发起核对：两端源站各自锁定首次应答的强 ETag，只取元数据，"
            "按 64 KiB 分块列出全部差异字节区间。"
        ),
        version="2.0.0",
    )
    manager = CheckManager(
        origin_a or os.environ.get("DRILL_ORIGIN_A", DEFAULT_ORIGIN_A),
        origin_b or os.environ.get("DRILL_ORIGIN_B", DEFAULT_ORIGIN_B),
    )
    app.state.manager = manager

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/health")
    async def health() -> dict:
        origins = {"a": manager.origin_a, "b": manager.origin_b}
        reachable: dict[str, bool] = {}
        for key, base in origins.items():
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
                    resp = await client.get(base + "/healthz")
                    resp.raise_for_status()
                reachable[key] = True
            except Exception:  # noqa: BLE001
                reachable[key] = False
        return {
            "ok": all(reachable.values()),
            "origins": origins,
            "reachable": reachable,
            "scenarios": list(SCENARIOS),
        }

    @app.get("/api/check")
    async def check_state() -> dict:
        return manager.snapshot()

    @app.post("/api/check")
    async def check_start(payload: CheckRequest) -> dict:
        try:
            params = CheckParams.from_payload(payload.model_dump())
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        run = await manager.start(params)
        return {"ok": True, "run_id": run.run_id, "state": manager.snapshot()}

    @app.post("/api/check/cancel")
    async def check_cancel() -> dict:
        await manager.cancel()
        return {"ok": True, "state": manager.snapshot()}

    @app.get("/api/origins")
    async def origins_state() -> dict:
        out: dict = {}
        for name, base in (("a", manager.origin_a), ("b", manager.origin_b)):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                    resp = await client.get(base + "/origin/state")
                    resp.raise_for_status()
                    out[name] = resp.json()
            except Exception as exc:  # noqa: BLE001
                out[name] = {"error": f"{type(exc).__name__}: {exc}"}
        return out

    return app


app = create_app()
