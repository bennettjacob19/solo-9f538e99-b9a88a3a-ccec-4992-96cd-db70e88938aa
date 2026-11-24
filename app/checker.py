"""Compare two image layers hosted by two local HTTP origins, metadata only.

Locking
-------
Each side is locked to the strong ETag from its very first response
(``HEAD /origin/layer``).  Every later metadata request carries
``If-Match: <locked ETag>``, so the origin answers for that exact version even
after ``POST /origin/switch``.  A check therefore always compares the two
versions that were current when it started.

How the comparison works
------------------------
The layer is cut into 64 KiB blocks (the last block is short and uses its real
length).  A probe returns one SHA-256 per requested window:

    block_digest(i)     = sha256(bytes of block i)
    window_digest(s, n) = sha256(block_digest(s) || ... || block_digest(s+n-1))

The checker starts with coarse windows covering the whole layer, then splits
every window whose digest differs into smaller sub-windows, repeating until a
window is a single block.  Differing blocks become byte intervals, and
adjacent blocks are merged.  Identical layers cost one probe; a single
differing block costs a handful of requests of a few hundred bytes each.

Budgets
-------
The defaults cap each side at 16 comparison requests (HEAD + probes) and cap
the two sides' combined probe body at 16 KiB.  Before a probe round the
checker verifies both caps; if a further round would exceed them it stops and
reports the check as incomplete instead of guessing.

Failures
--------
A timeout, non-200 metadata response or malformed probe body marks that side
and the whole check as incomplete (``outcome = incomplete``), with the reason
recorded.  Transport errors are never interpreted as "different".
"""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

BLOCK = 64 * 1024
STRONG_ETAG_RE = re.compile(r'^"[^"]*"$')
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

STATUS_IDLE = "idle"
STATUS_RUNNING = "running"
STATUS_DONE = "done"

OUTCOME_PENDING = "pending"
OUTCOME_IDENTICAL = "identical"
OUTCOME_DIFFERENT = "different"
OUTCOME_INCOMPLETE = "incomplete"

SCENARIOS = ("match", "single_block_diff", "swap_midway", "origin_fault")
SCENARIO_LABELS = {
    "match": "层一致",
    "single_block_diff": "单块差异",
    "swap_midway": "中途换版",
    "origin_fault": "源站故障",
}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + (
        f".{int(time.time() * 1000) % 1000:03d}Z"
    )


def block_count_for(size: int) -> int:
    return (size + BLOCK - 1) // BLOCK


def block_span(size: int, index: int) -> tuple[int, int]:
    start = index * BLOCK
    return start, min(size, start + BLOCK) - 1


@dataclass(frozen=True)
class CheckParams:
    size_bytes: int = 8 * 1024 * 1024
    scenario: str = "match"
    diff_block: Optional[int] = None
    max_requests_per_side: int = 16
    max_combined_body_bytes: int = 16 * 1024
    timeout_seconds: float = 15.0

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "CheckParams":
        try:
            size = int(payload.get("size_bytes", cls.size_bytes))
        except (TypeError, ValueError) as exc:
            raise ValueError("size_bytes must be an integer") from exc
        if not BLOCK <= size <= 128 * 1024 * 1024:
            raise ValueError("size_bytes must be between 65536 and 134217728")
        scenario = str(payload.get("scenario", cls.scenario))
        if scenario not in SCENARIOS:
            raise ValueError(f"scenario must be one of {', '.join(SCENARIOS)}")
        raw_block = payload.get("diff_block", None)
        diff_block: Optional[int] = None
        if raw_block not in (None, "", "auto"):
            try:
                diff_block = int(raw_block)
            except (TypeError, ValueError) as exc:
                raise ValueError("diff_block must be an integer or empty") from exc
            blocks = block_count_for(size)
            if diff_block < 0:
                diff_block = blocks + diff_block
            if not 0 <= diff_block < blocks:
                raise ValueError(f"diff_block must be within 0..{blocks - 1}")

        def as_int(name: str, default: int, low: int, high: int) -> int:
            try:
                value = int(payload.get(name, default))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be an integer") from exc
            if not low <= value <= high:
                raise ValueError(f"{name} must be between {low} and {high}")
            return value

        return cls(
            size_bytes=size,
            scenario=scenario,
            diff_block=diff_block,
            max_requests_per_side=as_int(
                "max_requests_per_side", cls.max_requests_per_side, 2, 16
            ),
            max_combined_body_bytes=as_int(
                "max_combined_body_bytes", cls.max_combined_body_bytes, 128, 16 * 1024
            ),
        )

    def public(self) -> dict[str, Any]:
        return {
            "size_bytes": self.size_bytes,
            "scenario": self.scenario,
            "scenario_label": SCENARIO_LABELS[self.scenario],
            "diff_block": self.diff_block,
            "max_requests_per_side": self.max_requests_per_side,
            "max_combined_body_bytes": self.max_combined_body_bytes,
        }


@dataclass
class SideState:
    label: str
    base: str
    locked_etag: Optional[str] = None
    locked_sha256: Optional[str] = None
    locked_version: Optional[str] = None
    size: Optional[int] = None
    blocks: Optional[int] = None
    lock_status: str = "pending"
    error: Optional[str] = None
    requests: int = 0          # comparison requests: HEAD + probes
    setup_requests: int = 0    # reset / switch / fault setup calls
    head_requests: int = 0
    probe_requests: int = 0
    body_bytes: int = 0        # probe response body bytes
    current_version: Optional[str] = None
    current_etag: Optional[str] = None
    locked_served: Optional[bool] = None
    last_status: Optional[int] = None

    def public(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "base": self.base,
            "locked_etag": self.locked_etag,
            "locked_sha256": self.locked_sha256,
            "locked_version": self.locked_version,
            "size": self.size,
            "blocks": self.blocks,
            "lock_status": self.lock_status,
            "error": self.error,
            "requests": self.requests,
            "setup_requests": self.setup_requests,
            "head_requests": self.head_requests,
            "probe_requests": self.probe_requests,
            "body_bytes": self.body_bytes,
            "current_version": self.current_version,
            "current_etag": self.current_etag,
            "locked_served": self.locked_served,
            "last_status": self.last_status,
        }


@dataclass
class CheckRun:
    params: CheckParams
    side_a: SideState
    side_b: SideState
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    status: str = STATUS_RUNNING
    outcome: str = OUTCOME_PENDING
    error: Optional[str] = None
    started_at: str = field(default_factory=_now_iso)
    finished_at: Optional[str] = None
    started_monotonic: float = field(default_factory=time.monotonic)
    finished_monotonic: Optional[float] = None
    events: list[dict[str, str]] = field(default_factory=list)
    differences: list[dict[str, Any]] = field(default_factory=list)
    blocks_compared: int = 0
    probe_rounds: int = 0
    budget_exhausted: bool = False
    reported_identical: bool = False
    swapped: list[dict[str, Any]] = field(default_factory=list)
    _lock_noted: bool = False
    _task: Optional[asyncio.Task] = None

    # -------------------------------------------------------------- reporting
    def log(self, kind: str, message: str) -> None:
        self.events.append({"at": _now_iso(), "kind": kind, "message": message})
        if len(self.events) > 300:
            del self.events[: len(self.events) - 300]

    @property
    def combined_requests(self) -> int:
        return self.side_a.requests + self.side_b.requests

    @property
    def combined_body_bytes(self) -> int:
        return self.side_a.body_bytes + self.side_b.body_bytes

    def summary(self) -> dict[str, Any]:
        max_req = self.params.max_requests_per_side
        max_body = self.params.max_combined_body_bytes
        return {
            "run_id": self.run_id,
            "status": self.status,
            "outcome": self.outcome,
            "outcome_label": {
                OUTCOME_IDENTICAL: "两锁定版本一致",
                OUTCOME_DIFFERENT: "存在差异块",
                OUTCOME_INCOMPLETE: "未核完",
                OUTCOME_PENDING: "进行中",
            }[self.outcome],
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed_ms": int(
                ((self.finished_monotonic or time.monotonic()) - self.started_monotonic)
                * 1000
            ),
            "params": self.params.public(),
            "block_size": BLOCK,
            "sides": {"a": self.side_a.public(), "b": self.side_b.public()},
            "differences": list(self.differences),
            "difference_count": len(self.differences),
            "difference_bytes": sum(d["length"] for d in self.differences),
            "blocks_compared": self.blocks_compared,
            "probe_rounds": self.probe_rounds,
            "reported_identical": self.reported_identical,
            "budget": {
                "max_requests_per_side": max_req,
                "max_combined_body_bytes": max_body,
                "combined_requests": self.combined_requests,
                "combined_body_bytes": self.combined_body_bytes,
                "requests_headroom": max(0, 2 * max_req - self.combined_requests),
                "body_headroom": max(0, max_body - self.combined_body_bytes),
                "exhausted": self.budget_exhausted,
            },
            "swapped": list(self.swapped),
            "events": list(self.events),
        }

    # --------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"check-{self.run_id}")

    async def cancel(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    # ------------------------------------------------------------- HTTP helper
    async def _request(
        self,
        side: SideState,
        method: str,
        path: str,
        *,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        comparison: bool = True,
    ) -> httpx.Response:
        # Count the attempt, not just successes: a failed request still spends
        # one of the per-side request allowances.
        if comparison:
            side.requests += 1
        else:
            side.setup_requests += 1
        timeout = httpx.Timeout(self.params.timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method, side.base + path, headers=headers, params=params
            )
        side.last_status = response.status_code
        return response

    async def _admin(self, side: SideState, path: str, params: dict[str, Any] | None = None):
        response = await self._request(
            side, "POST", path, params=params, comparison=False
        )
        response.raise_for_status()
        return response

    # -------------------------------------------------------------------- main
    async def _run(self) -> None:
        try:
            await self._prepare()
            if self.side_a.error or self.side_b.error:
                return
            await self._lock_sides()
            if self.side_a.error or self.side_b.error:
                self._finish_incomplete("源站首次应答失败")
                return
            if self.side_a.size != self.side_b.size:
                self._finish_incomplete(
                    f"两端层长度不同：{self.side_a.size} vs {self.side_b.size}"
                )
                return
            if self.params.scenario == "swap_midway":
                await self._swap_midway()
                if self.side_b.error:
                    self._finish_incomplete("换版请求失败")
                    return
            elif self.params.scenario == "origin_fault":
                await self._inject_fault_midway()
                if self.side_b.error:
                    self._finish_incomplete("故障注入失败")
                    return
            await self._compare()
        except asyncio.CancelledError:
            self.error = "check cancelled"
            self.outcome = OUTCOME_INCOMPLETE
            raise
        except Exception as exc:  # noqa: BLE001 - surfaced in the UI
            self.error = f"{type(exc).__name__}: {exc}"
            self.log("error", self.error)
            self.outcome = OUTCOME_INCOMPLETE
        finally:
            if self.outcome == OUTCOME_PENDING:
                self.outcome = OUTCOME_INCOMPLETE
            self.status = STATUS_DONE
            self.finished_at = _now_iso()
            self.finished_monotonic = time.monotonic()
            self.log(
                "done",
                f"核对结束：{self.outcome}"
                + (f"（{self.error}）" if self.error else ""),
            )

    def _finish_incomplete(self, reason: str) -> None:
        self.error = reason
        self.outcome = OUTCOME_INCOMPLETE
        self.log("error", reason)

    # ----------------------------------------------------------------- prepare
    async def _prepare(self) -> None:
        """Put both origins into the scenario's starting state."""
        scenario = self.params.scenario
        reset_params: dict[str, Any] = {"size_bytes": self.params.size_bytes}
        if self.params.diff_block is not None:
            reset_params["diff_block"] = self.params.diff_block
        for side in (self.side_a, self.side_b):
            try:
                await self._admin(side, "/origin/reset", reset_params)
            except Exception as exc:  # noqa: BLE001
                side.error = f"重置失败: {type(exc).__name__}: {exc}"
                side.lock_status = "failed"
                self.log("error", f"{side.label} 端{side.error}")
        if self.side_a.error or self.side_b.error:
            self._finish_incomplete("源站重置失败")
            return
        self.log(
            "prepare",
            f"两端重置为等长 v1（{self.params.size_bytes} 字节，"
            f"{block_count_for(self.params.size_bytes)} 个 64 KiB 块）",
        )
        if scenario == "single_block_diff":
            await self._switch(self.side_b, "v2", source="场景初始化")
        elif scenario == "swap_midway":
            await self._switch(self.side_b, "v2", source="场景初始化")

    async def _switch(self, side: SideState, version: str, *, source: str) -> None:
        try:
            await self._admin(side, "/origin/switch", {"version": version})
            self.log("switch", f"{side.label} 端源站切换到 {version}（{source}）")
        except Exception as exc:  # noqa: BLE001
            side.error = f"换版失败: {exc}"
            self.log("error", f"{side.label} 端{side.error}")

    async def _swap_midway(self) -> None:
        """Both sides are locked; now flip B.  The check must stay on locked v2."""
        await self._switch(self.side_b, "v1", source="锁定后换版")
        self.swapped.append(
            {
                "side": "b",
                "from": "v2",
                "to": "v1",
                "at": _now_iso(),
                "note": "B 端首次应答已锁定 v2；后续核对仍向源站索取 v2 的元数据",
            }
        )

    async def _inject_fault_midway(self) -> None:
        """Fail B's metadata after both locks, proving the check reports it."""
        try:
            await self._admin(self.side_b, "/origin/fault", {"count": 1000})
            self.log(
                "fault",
                "B 端已在锁定后注入源站故障：后续元数据请求返回 503，核对将标为未核完",
            )
        except Exception as exc:  # noqa: BLE001
            self.side_b.error = f"故障注入失败: {exc}"
            self.log("error", self.side_b.error)

    # -------------------------------------------------------------------- lock
    async def _lock_side(self, side: SideState) -> None:
        try:
            response = await self._request(side, "HEAD", "/origin/layer")
        except Exception as exc:  # noqa: BLE001
            side.error = f"首次应答失败: {type(exc).__name__}: {exc}"
            side.lock_status = "failed"
            self.log("error", f"{side.label} 端{side.error}")
            return
        side.head_requests += 1
        if response.status_code != 200:
            side.error = f"首次应答 HTTP {response.status_code}"
            side.lock_status = "failed"
            self.log("error", f"{side.label} 端{side.error}")
            return
        etag = response.headers.get("etag")
        size_raw = response.headers.get("x-layer-size")
        blocks_raw = response.headers.get("x-layer-blocks")
        sha = response.headers.get("x-layer-sha256")
        version = response.headers.get("x-origin-version")
        if not etag or not STRONG_ETAG_RE.match(etag):
            side.error = f"ETag 不是强校验器：{etag!r}"
            side.lock_status = "failed"
            self.log("error", f"{side.label} 端{side.error}")
            return
        if not size_raw or not blocks_raw or not sha:
            side.error = "首次应答缺少长度/分块数/摘要"
            side.lock_status = "failed"
            self.log("error", f"{side.label} 端{side.error}")
            return
        try:
            side.size = int(size_raw)
            side.blocks = int(blocks_raw)
        except ValueError:
            side.error = "首次应答的长度/分块数无法解析"
            side.lock_status = "failed"
            self.log("error", f"{side.label} 端{side.error}")
            return
        side.locked_etag = etag
        side.locked_sha256 = sha
        side.locked_version = version
        side.current_version = version
        side.current_etag = etag
        side.lock_status = "locked"
        side.error = None
        self.log(
            "lock",
            f"{side.label} 端锁定强 ETag {etag}（{side.size} 字节 / "
            f"{side.blocks} 块 / {version}）",
        )

    async def _lock_sides(self) -> None:
        await asyncio.gather(
            self._lock_side(self.side_a), self._lock_side(self.side_b)
        )

    # ------------------------------------------------------------------ probe
    def _budget_allows(self, window_count: int) -> bool:
        for side in (self.side_a, self.side_b):
            if side.requests + 1 > self.params.max_requests_per_side:
                return False
        prospective = self.combined_body_bytes + 2 * 64 * window_count
        return prospective <= self.params.max_combined_body_bytes

    async def _probe_side(
        self, side: SideState, windows: list[tuple[int, int]]
    ) -> Optional[list[str]]:
        if side.error:
            return None
        raw = ",".join(f"{start}:{count}" for start, count in windows)
        headers = {"If-Match": side.locked_etag or ""}
        try:
            response = await self._request(
                side, "GET", "/origin/probe",
                headers=headers, params={"windows": raw},
            )
        except Exception as exc:  # noqa: BLE001
            side.error = f"核对请求失败: {type(exc).__name__}: {exc}"
            self.log("error", f"{side.label} 端{side.error}")
            return None
        side.probe_requests += 1
        body = response.content
        side.body_bytes += len(body)
        if response.status_code != 200:
            side.error = f"核对应答 HTTP {response.status_code}"
            self.log("error", f"{side.label} 端{side.error}")
            return None
        served_version = response.headers.get("x-origin-version")
        served_etag = response.headers.get("etag")
        current = response.headers.get("x-origin-current-version")
        if served_etag:
            side.current_etag = served_etag
        if current:
            side.current_version = current
        elif served_version:
            side.current_version = served_version
        locked_flag = response.headers.get("x-version-locked") == "true"
        side.locked_served = locked_flag
        if locked_flag and not self._lock_noted:
            self._lock_noted = True
            self.log(
                "lock",
                f"{side.label} 端源站已换版（{served_version} → 当前 "
                f"{side.current_version}），仍按锁定 ETag 返回锁定版元数据",
            )
        expected = 64 * len(windows)
        if len(body) != expected:
            side.error = f"核对正文长度异常：{len(body)} != {expected}"
            self.log("error", f"{side.label} 端{side.error}")
            return None
        try:
            text = body.decode("ascii")
        except UnicodeDecodeError:
            side.error = "核对正文不是十六进制摘要"
            self.log("error", f"{side.label} 端{side.error}")
            return None
        digests = [text[i : i + 64] for i in range(0, len(text), 64)]
        if len(digests) != len(windows) or not all(
            HEX64_RE.match(d) for d in digests
        ):
            side.error = "核对正文摘要格式不符"
            self.log("error", f"{side.label} 端{side.error}")
            return None
        return digests

    async def _probe_both(
        self, windows: list[tuple[int, int]]
    ) -> Optional[tuple[list[str], list[str]]]:
        if not self._budget_allows(len(windows)):
            self.budget_exhausted = True
            self.log(
                "budget",
                f"请求或正文额度不足以复核 {len(windows)} 个窗口，停止核对",
            )
            return None
        a, b = await asyncio.gather(
            self._probe_side(self.side_a, windows),
            self._probe_side(self.side_b, windows),
        )
        if a is None or b is None:
            return None
        self.probe_rounds += 1
        return a, b

    # ---------------------------------------------------------------- compare
    COARSE_WINDOWS = 8
    SPLIT_FANOUT = 8

    @classmethod
    def _initial_windows(cls, blocks: int) -> list[tuple[int, int]]:
        window = max(1, -(-blocks // cls.COARSE_WINDOWS))
        return [
            (start, min(window, blocks - start))
            for start in range(0, blocks, window)
        ]

    @classmethod
    def _split(cls, window: tuple[int, int]) -> list[tuple[int, int]]:
        """Cut a window into at most SPLIT_FANOUT sub-windows."""
        start, count = window
        if count <= 1:
            return [window]
        sub = max(1, -(-count // cls.SPLIT_FANOUT))
        return [
            (s, min(sub, start + count - s))
            for s in range(start, start + count, sub)
        ]

    def _record_differences(self, indices: list[int]) -> None:
        size = self.side_a.size or 0
        existing = {d["block"] for d in self.differences}
        fresh = [i for i in indices if i not in existing]
        for index in fresh:
            start, end = block_span(size, index)
            self.differences.append(
                {
                    "block": index,
                    "start": start,
                    "end": end,
                    "length": end - start + 1,
                }
            )
        if not fresh:
            return
        self.differences.sort(key=lambda d: d["start"])
        merged: list[dict[str, Any]] = []
        for item in self.differences:
            if merged and item["start"] == merged[-1]["end"] + 1:
                merged[-1]["end"] = item["end"]
                merged[-1]["length"] = merged[-1]["end"] - merged[-1]["start"] + 1
                merged[-1]["blocks"] = merged[-1].get("blocks", 1) + 1
            else:
                merged.append({**item, "blocks": 1})
        self.differences = merged
        self.log(
            "diff",
            "确认差异块："
            + ", ".join(str(i) for i in sorted(fresh))
            + " → 区间 "
            + ", ".join(
                f"{d['start']}-{d['end']}({d['length']}B)" for d in self.differences
            ),
        )

    async def _compare(self) -> None:
        blocks = self.side_a.blocks or 0
        if blocks <= 0:
            self._finish_incomplete("没有可比对的分块")
            return
        windows = self._initial_windows(blocks)
        result = await self._probe_both(windows)
        if result is None:
            self._finish_incomplete(self.error or "首次元数据探针失败")
            return
        a, b = result
        self.log(
            "probe",
            f"第 1 轮：{len(windows)} 个粗窗口，每窗口 {windows[0][1]} 块",
        )

        pending = [
            window for window, da, db in zip(windows, a, b) if da != db
        ]
        while pending:
            singles = [w for w in pending if w[1] == 1]
            if singles:
                self._record_differences([start for start, _ in singles])
            candidates: list[tuple[int, int]] = []
            for window in pending:
                if window[1] > 1:
                    candidates.extend(self._split(window))
            if not candidates:
                break
            result = await self._probe_both(candidates)
            if result is None:
                if self.budget_exhausted:
                    self._finish_incomplete("请求或正文额度不足以核完所有窗口")
                else:
                    self._finish_incomplete(self.error or "分块探针失败")
                return
            a, b = result
            self.log(
                "probe",
                f"第 {self.probe_rounds} 轮：复核 {len(candidates)} 个子窗口",
            )
            pending = [
                window
                for window, da, db in zip(candidates, a, b)
                if da != db
            ]

        self.blocks_compared = blocks
        if self.differences:
            self.outcome = OUTCOME_DIFFERENT
            self.log(
                "result",
                f"按锁定版本比对：{len(self.differences)} 个差异区间，共 "
                f"{sum(d['length'] for d in self.differences)} 字节",
            )
        else:
            self.outcome = OUTCOME_IDENTICAL
            self.reported_identical = True
            self.log("result", "两锁定版本的 64 KiB 分块摘要完全一致")


class CheckManager:
    def __init__(self, origin_a: str, origin_b: str) -> None:
        self.origin_a = origin_a.rstrip("/")
        self.origin_b = origin_b.rstrip("/")
        self.current: Optional[CheckRun] = None
        self._lock = asyncio.Lock()

    async def start(self, params: CheckParams) -> CheckRun:
        async with self._lock:
            if self.current is not None and self.current.status == STATUS_RUNNING:
                await self.current.cancel()
            run = CheckRun(
                params=params,
                side_a=SideState(label="A", base=self.origin_a),
                side_b=SideState(label="B", base=self.origin_b),
            )
            self.current = run
            run.start()
            return run

    async def cancel(self) -> None:
        async with self._lock:
            if self.current is not None and self.current.status == STATUS_RUNNING:
                await self.current.cancel()
            self.current = None

    def snapshot(self) -> dict[str, Any]:
        if self.current is None:
            empty = CheckParams()
            return {
                "status": STATUS_IDLE,
                "outcome": OUTCOME_PENDING,
                "outcome_label": "待运行",
                "params": None,
                "block_size": BLOCK,
                "sides": {
                    "a": SideState(label="A", base=self.origin_a).public(),
                    "b": SideState(label="B", base=self.origin_b).public(),
                },
                "differences": [],
                "difference_count": 0,
                "difference_bytes": 0,
                "blocks_compared": 0,
                "probe_rounds": 0,
                "reported_identical": False,
                "swapped": [],
                "budget": {
                    "max_requests_per_side": empty.max_requests_per_side,
                    "max_combined_body_bytes": empty.max_combined_body_bytes,
                    "combined_requests": 0,
                    "combined_body_bytes": 0,
                    "requests_headroom": 2 * empty.max_requests_per_side,
                    "body_headroom": empty.max_combined_body_bytes,
                    "exhausted": False,
                },
                "events": [],
            }
        return self.current.summary()
