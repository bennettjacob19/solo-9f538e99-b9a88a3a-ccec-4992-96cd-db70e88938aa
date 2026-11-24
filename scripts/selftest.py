#!/usr/bin/env python3
"""Self-test: start two origins + the web gateway and drive real scenarios.

Topology matches ``docker compose``: two origin processes and one gateway.

    origin-a : HTTP origin A
    origin-b : HTTP origin B
    gateway  : web UI + comparison API

Scenarios:

1. ``match``            layers identical -> identical, one probe round
2. ``single_block_diff`` one 64 KiB block differs -> exact byte interval
3. ``swap_midway``      B switches after locking -> still compares locked v2
4. ``origin_fault``     B starts failing after locking -> incomplete
5. ``single_block_diff`` on a 128 MiB layer -> budget limits still respected

Exit code is non-zero when any expectation fails.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
BLOCK = 64 * 1024


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Services:
    def __init__(self) -> None:
        self.a_port = free_port()
        self.b_port = free_port()
        self.gw_port = free_port()
        self.procs: list[subprocess.Popen] = []

    def _spawn(self, args: list[str], env: dict[str, str], log_path: Path) -> None:
        handle = log_path.open("w")
        self._logs.append(handle)
        self.procs.append(
            subprocess.Popen(
                [PYTHON, "-m", "uvicorn", *args],
                cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT,
            )
        )

    def __enter__(self) -> "Services":
        self._logs = []
        log_dir = Path("/tmp/layer-check-selftest")
        log_dir.mkdir(exist_ok=True)
        base_env = dict(os.environ)
        base_env["PYTHONUNBUFFERED"] = "1"
        base_env["DRILL_LAYER_BYTES"] = str(1024 * 1024)
        self._spawn(
            ["app.origin_app:app", "--host", "127.0.0.1",
             "--port", str(self.a_port), "--log-level", "warning"],
            base_env, log_dir / "origin-a.log",
        )
        self._spawn(
            ["app.origin_app:app", "--host", "127.0.0.1",
             "--port", str(self.b_port), "--log-level", "warning"],
            base_env, log_dir / "origin-b.log",
        )
        gw_env = dict(base_env)
        gw_env["DRILL_ORIGIN_A"] = f"http://127.0.0.1:{self.a_port}"
        gw_env["DRILL_ORIGIN_B"] = f"http://127.0.0.1:{self.b_port}"
        self._spawn(
            ["app.gateway_app:app", "--host", "127.0.0.1",
             "--port", str(self.gw_port), "--log-level", "warning"],
            gw_env, log_dir / "gateway.log",
        )
        self.gateway = f"http://127.0.0.1:{self.gw_port}"
        deadline = time.time() + 25
        while time.time() < deadline:
            try:
                if httpx.get(self.gateway + "/api/health", timeout=1).status_code == 200:
                    return self
            except httpx.HTTPError:
                pass
            time.sleep(0.15)
        raise RuntimeError(f"services did not become healthy; see {log_dir}")

    def __exit__(self, *exc) -> None:
        for proc in self.procs:
            proc.terminate()
        for proc in self.procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for handle in self._logs:
            handle.close()


def run_check(base: str, timeout: float = 120, **payload) -> dict:
    body = {"size_bytes": 1024 * 1024, "scenario": "match"}
    body.update(payload)
    with httpx.Client(base_url=base, timeout=30) as client:
        resp = client.post("/api/check", json=body)
        resp.raise_for_status()
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = client.get("/api/check").json()
            if state["status"] != "running":
                return state
            time.sleep(0.02)
    raise RuntimeError("check timed out")


def summarize(name: str, state: dict) -> dict:
    a, b = state["sides"]["a"], state["sides"]["b"]
    return {
        "scenario": name,
        "outcome": state["outcome"],
        "diffs": state["difference_count"],
        "diff_bytes": state["difference_bytes"],
        "a_req": a["requests"],
        "b_req": b["requests"],
        "body_bytes": state["budget"]["combined_body_bytes"],
        "a_etag": a["locked_etag"],
        "b_etag": b["locked_etag"],
        "a_ver": a["locked_version"],
        "b_ver": b["locked_version"],
        "b_now": b["current_version"],
        "b_locked_served": b["locked_served"],
        "error": state["error"],
    }


def main() -> int:
    failures: list[str] = []
    results: list[dict] = []
    size = 1024 * 1024
    diff_block = 3

    with Services() as svc:
        state = run_check(svc.gateway, scenario="match", size_bytes=size)
        results.append(summarize("match", state))
        if not (
            state["outcome"] == "identical"
            and state["difference_count"] == 0
            and state["sides"]["a"]["locked_etag"]
            and state["sides"]["a"]["locked_etag"] == state["sides"]["b"]["locked_etag"]
            and state["sides"]["a"]["requests"] <= 16
            and state["sides"]["b"]["requests"] <= 16
        ):
            failures.append("match")

        state = run_check(
            svc.gateway, scenario="single_block_diff",
            size_bytes=size, diff_block=diff_block,
        )
        results.append(summarize("single_block_diff", state))
        diff = (state["differences"] or [{}])[0]
        if not (
            state["outcome"] == "different"
            and state["difference_count"] == 1
            and diff.get("start") == diff_block * BLOCK
            and diff.get("end") == diff_block * BLOCK + BLOCK - 1
            and diff.get("length") == BLOCK
            and state["sides"]["a"]["requests"] <= 16
            and state["sides"]["b"]["requests"] <= 16
            and state["budget"]["combined_body_bytes"] <= 16 * 1024
        ):
            failures.append("single_block_diff")

        state = run_check(
            svc.gateway, scenario="swap_midway",
            size_bytes=size, diff_block=diff_block,
        )
        results.append(summarize("swap_midway", state))
        if not (
            state["outcome"] == "different"
            and state["sides"]["a"]["locked_version"] == "v1"
            and state["sides"]["b"]["locked_version"] == "v2"
            and state["sides"]["b"]["current_version"] == "v1"
            and state["sides"]["b"]["locked_served"] is True
            and [d["block"] for d in state["differences"]] == [diff_block]
        ):
            failures.append("swap_midway")

        state = run_check(svc.gateway, scenario="origin_fault", size_bytes=size)
        results.append(summarize("origin_fault", state))
        if not (
            state["outcome"] == "incomplete"
            and state["error"]
            and state["sides"]["b"]["locked_etag"]
            and "503" in (state["sides"]["b"]["error"] or "")
        ):
            failures.append("origin_fault")

        big = 128 * 1024 * 1024
        state = run_check(
            svc.gateway, timeout=180, scenario="single_block_diff", size_bytes=big
        )
        results.append(summarize("128MiB single_block_diff", state))
        if not (
            state["outcome"] == "different"
            and state["difference_count"] == 1
            and state["differences"][0]["length"] == BLOCK
            and state["sides"]["a"]["requests"] <= 16
            and state["sides"]["b"]["requests"] <= 16
            and state["budget"]["combined_body_bytes"] <= 16 * 1024
        ):
            failures.append("128MiB single_block_diff")

    print(json.dumps(results, indent=2, ensure_ascii=False))
    print()
    header = (
        f"{'scenario':26} {'outcome':11} {'diffs':>5} {'diffB':>8} "
        f"{'a_req':>5} {'b_req':>5} {'bodyB':>7} {'b_locked':>9} {'b_now':>6}"
    )
    print(header)
    for r in results:
        print(
            f"{r['scenario']:26} {r['outcome']:11} {r['diffs']:>5} "
            f"{r['diff_bytes']:>8} {r['a_req']:>5} {r['b_req']:>5} "
            f"{r['body_bytes']:>7} {str(r['b_locked_served']):>9} "
            f"{str(r['b_now']):>6}"
        )
    print()
    if failures:
        print("FAILED:", ", ".join(failures))
        return 1
    print("ALL SCENARIOS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
