"""Tests for the two-origin layer check.

The suite starts two real origin servers on free ports and drives the checker
over HTTP, so locking, If-Match probes, window splitting, budget accounting and
failure handling are all exercised end to end.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from app.checker import (
    BLOCK,
    CheckManager,
    CheckParams,
    OUTCOME_DIFFERENT,
    OUTCOME_IDENTICAL,
    OUTCOME_INCOMPLETE,
)
from app.origin import OriginStore, build_version, default_diff_block
from app.origin_app import create_origin_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LiveOrigin:
    def __init__(self, size: int, chunk_size: int = BLOCK) -> None:
        self.port = _free_port()
        self.app = create_origin_app(size=size, chunk_size=chunk_size)
        self.store: OriginStore = self.app.state.origin_store
        config = uvicorn.Config(
            self.app, host="127.0.0.1", port=self.port, log_level="warning"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> "LiveOrigin":
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started and time.time() < deadline:
            time.sleep(0.02)
        assert self.server.started, "origin server did not start"
        return self

    def __exit__(self, *exc) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture()
def origins():
    with LiveOrigin(size=1024 * 1024) as a, LiveOrigin(size=1024 * 1024) as b:
        yield a, b


def run_check(
    a: LiveOrigin,
    b: LiveOrigin,
    *,
    timeout: float = 30.0,
    **overrides,
) -> tuple[dict, CheckManager]:
    payload = {"size_bytes": 1024 * 1024, "scenario": "match"}
    payload.update(overrides)
    params = CheckParams.from_payload(payload)
    manager = CheckManager(a.base, b.base)

    async def go():
        await manager.start(params)
        deadline = time.time() + timeout
        while manager.current.status == "running" and time.time() < deadline:
            await asyncio.sleep(0.02)
        assert manager.current.status != "running", "check timed out"
        return manager.snapshot()

    return asyncio.run(go()), manager


# ------------------------------------------------------------------ unit tests
def test_versions_are_equal_length_with_one_replaced_block():
    size = 1024 * 1024 + 123  # deliberately not a multiple of 64 KiB
    v1 = build_version("v1", size, 7)
    v2 = build_version("v2", size, 7, variant=True)
    assert v1.size == v2.size == size
    assert v1.block_count == v2.block_count
    assert v2.diff_block == default_diff_block(size)
    differing = [
        i for i in range(v1.block_count) if v1.block_digest(i) != v2.block_digest(i)
    ]
    assert differing == [v2.diff_block]
    assert v1.block_length(v1.block_count - 1) == size % BLOCK
    assert v1.sha256 != v2.sha256
    assert v1.etag.startswith('"') and not v1.etag.startswith("W/")
    assert v2.etag.startswith('"') and not v2.etag.startswith("W/")


def test_origin_icon_headers_and_if_match(origins):
    a, _ = origins
    with httpx.Client(base_url=a.base, timeout=10) as client:
        head = client.head("/origin/layer")
        assert head.status_code == 200
        etag = head.headers["etag"]
        assert head.headers["x-layer-size"] == str(1024 * 1024)
        assert head.headers["x-layer-blocks"] == "16"
        assert head.headers["x-layer-block-size"] == str(BLOCK)

        client.post("/origin/switch", params={"version": "v2"})
        locked = client.get(
            "/origin/probe", params={"windows": "0:16"}, headers={"If-Match": etag}
        )
        assert locked.status_code == 200
        assert locked.headers["x-origin-version"] == "v1"
        assert locked.headers["x-version-locked"] == "true"
        assert locked.headers["etag"] == etag
        assert len(locked.content) == 64  # one window -> one digest

        unknown = client.get(
            "/origin/probe", params={"windows": "0:16"},
            headers={"If-Match": '"does-not-exist"'},
        )
        assert unknown.status_code == 412

        full = client.get(
            "/origin/layer", headers={"Range": f"bytes=0-{BLOCK - 1}"}
        )
        assert full.status_code == 206
        assert len(full.content) == BLOCK


def test_probe_digest_matches_bytes_for_short_last_block(origins):
    a, _ = origins
    version = a.store.current()
    last = version.block_count - 1
    with httpx.Client(base_url=a.base, timeout=10) as client:
        response = client.get("/origin/probe", params={"windows": f"{last}:1,0:2"})
        assert response.status_code == 200
        text = response.content.decode("ascii")
        assert len(text) == 128
        # A window digest is sha256 over the concatenated block digests.
        assert text[:64] == version.window_digest(last, 1).hex()
        assert text[64:] == version.window_digest(0, 2).hex()
        assert version.block_length(last) > 0
        # and it derives from the real block bytes, not padded ones
        import hashlib

        assert version.block_digest(last) == hashlib.sha256(
            version.block_bytes(last)
        ).digest()
        assert len(version.block_bytes(last)) == version.block_length(last)


# --------------------------------------------------------------- check: match
def test_match_reports_identical_and_locked_etags(origins):
    a, b = origins
    state, _ = run_check(a, b, scenario="match")
    assert state["status"] == "done"
    assert state["outcome"] == OUTCOME_IDENTICAL
    assert state["differences"] == []
    assert state["blocks_compared"] == 16
    for key in ("a", "b"):
        side = state["sides"][key]
        assert side["lock_status"] == "locked"
        assert side["locked_etag"] == a.store.get("v1").etag
        assert side["locked_version"] == "v1"
        assert side["requests"] <= 16
        assert side["body_bytes"] <= 16 * 1024
    # only metadata left either origin: no layer body was downloaded
    for origin in (a, b):
        stats = origin.store.stats()
        assert stats["layer_body_bytes"] == 0
        assert stats["head_requests"] + stats["probe_requests"] == stats["requests"]


# ------------------------------------------------------ check: one block diff
def test_single_block_diff_lists_exact_byte_interval(origins):
    a, b = origins
    size = a.store.size
    diff_block = default_diff_block(size)
    state, _ = run_check(
        a, b, scenario="single_block_diff", diff_block=diff_block
    )
    assert state["outcome"] == OUTCOME_DIFFERENT
    assert state["difference_count"] == 1
    diff = state["differences"][0]
    assert diff["block"] == diff_block
    assert diff["start"] == diff_block * BLOCK
    assert diff["end"] == min(size, (diff_block + 1) * BLOCK) - 1
    assert diff["length"] == diff["end"] - diff["start"] + 1
    assert state["difference_bytes"] == diff["length"]
    assert state["sides"]["a"]["locked_etag"] != state["sides"]["b"]["locked_etag"]


def test_short_last_block_diff_uses_real_length(origins):
    a, b = origins
    size = a.store.size
    blocks = (size + BLOCK - 1) // BLOCK
    state, _ = run_check(
        a, b, scenario="single_block_diff", diff_block=blocks - 1
    )
    assert state["outcome"] == OUTCOME_DIFFERENT
    diff = state["differences"][0]
    assert diff["block"] == blocks - 1
    assert diff["start"] == (blocks - 1) * BLOCK
    assert diff["end"] == size - 1
    assert diff["length"] == size - (blocks - 1) * BLOCK


def test_reported_interval_matches_actual_bytes(origins):
    """Independent check: fetch the reported block from both origins and differ."""
    a, b = origins
    diff_block = 3
    state, _ = run_check(
        a, b, scenario="single_block_diff", diff_block=diff_block
    )
    diff = state["differences"][0]
    headers_a = {"If-Match": state["sides"]["a"]["locked_etag"]}
    headers_b = {"If-Match": state["sides"]["b"]["locked_etag"]}
    rng = {"Range": f"bytes={diff['start']}-{diff['end']}"}
    with httpx.Client(timeout=10) as client:
        body_a = client.get(
            a.base + "/origin/layer", headers={**rng, **headers_a}
        ).content
        body_b = client.get(
            b.base + "/origin/layer", headers={**rng, **headers_b}
        ).content
        before_a = client.get(
            a.base + "/origin/layer",
            headers={"Range": f"bytes=0-{BLOCK - 1}", **headers_a},
        ).content
        before_b = client.get(
            b.base + "/origin/layer",
            headers={"Range": f"bytes=0-{BLOCK - 1}", **headers_b},
        ).content
    assert len(body_a) == diff["length"] == len(body_b)
    assert body_a != body_b
    assert before_a == before_b  # a block outside the interval is identical


# -------------------------------------------------------- check: swap midway
def test_swap_midway_keeps_comparing_locked_versions(origins):
    a, b = origins
    diff_block = default_diff_block(a.store.size)
    state, _ = run_check(a, b, scenario="swap_midway")
    assert state["outcome"] == OUTCOME_DIFFERENT
    assert state["sides"]["b"]["locked_version"] == "v2"
    assert state["sides"]["b"]["current_version"] == "v1"
    assert state["sides"]["b"]["locked_served"] is True
    assert state["sides"]["a"]["locked_version"] == "v1"
    assert state["swapped"] and state["swapped"][0]["from"] == "v2"
    assert [d["block"] for d in state["differences"]] == [diff_block]
    assert state["sides"]["b"]["requests"] <= 16
    # the origin really is on v1 now: a fresh lock would compare identical
    fresh, _ = run_check(a, b, scenario="match")
    assert fresh["outcome"] == OUTCOME_IDENTICAL
    assert fresh["sides"]["b"]["locked_version"] == "v1"


# ------------------------------------------------------- check: origin fault
def test_origin_fault_marks_incomplete(origins):
    a, b = origins
    state, _ = run_check(a, b, scenario="origin_fault")
    assert state["outcome"] == OUTCOME_INCOMPLETE
    assert "HTTP 503" in (state["sides"]["b"]["error"] or "")
    assert state["error"]
    assert state["differences"] == []
    # both sides still locked before the fault, so the page shows the ETags
    assert state["sides"]["a"]["locked_etag"]
    assert state["sides"]["b"]["locked_etag"]


def test_unreachable_origin_marks_incomplete(tmp_path):
    with LiveOrigin(size=1024 * 1024) as a:
        params = CheckParams.from_payload(
            {"size_bytes": 1024 * 1024, "scenario": "match"}
        )
        manager = CheckManager(a.base, f"http://127.0.0.1:{_free_port()}")

        async def go():
            await manager.start(params)
            deadline = time.time() + 20
            while manager.current.status == "running" and time.time() < deadline:
                await asyncio.sleep(0.02)
            return manager.snapshot()

        state = asyncio.run(go())
    assert state["outcome"] == OUTCOME_INCOMPLETE
    assert state["error"]


# -------------------------------------------------------------- check: budget
def test_budget_caps_are_enforced(origins):
    a, b = origins
    state, _ = run_check(
        a, b,
        scenario="match",
        size_bytes=1024 * 1024,
        max_combined_body_bytes=128,  # one probe needs 512 B
    )
    assert state["outcome"] == OUTCOME_INCOMPLETE
    assert state["budget"]["exhausted"] is True
    assert state["budget"]["combined_body_bytes"] <= 128
    assert state["sides"]["a"]["requests"] <= 2  # HEAD only


def test_128mib_single_block_diff_fits_usage_limits(origins):
    """The headline acceptance case: 128 MiB layer, one differing 64 KiB block."""
    a, b = origins
    state, _ = run_check(
        a, b, timeout=90, size_bytes=128 * 1024 * 1024,
        scenario="single_block_diff",
    )
    assert state["outcome"] == OUTCOME_DIFFERENT
    assert state["difference_count"] == 1
    diff = state["differences"][0]
    assert diff["length"] == BLOCK
    assert state["sides"]["a"]["requests"] <= 16
    assert state["sides"]["b"]["requests"] <= 16
    assert state["budget"]["combined_body_bytes"] <= 16 * 1024
    for origin in (a, b):
        stats = origin.store.stats()
        assert stats["layer_body_bytes"] == 0
