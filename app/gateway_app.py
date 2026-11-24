"""Gateway service: web UI + comparison API, talking to two origin services."""

from __future__ import annotations

import os

from fastapi import FastAPI

from .main import create_app

ORIGIN_A = os.environ.get("DRILL_ORIGIN_A", "http://origin-a:8001")
ORIGIN_B = os.environ.get("DRILL_ORIGIN_B", "http://origin-b:8002")

app: FastAPI = create_app(ORIGIN_A, ORIGIN_B)
