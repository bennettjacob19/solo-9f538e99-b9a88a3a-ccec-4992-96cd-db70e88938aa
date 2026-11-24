"""Two local HTTP origins serving equal-length image layers.

Layer bytes are *generated*, never stored: block ``i`` of a version is a
deterministic function of the layer seed and ``i``, so an origin can serve
128 MiB without allocating 128 MiB.

Each origin has two versions of exactly the same length but different bytes:

* ``v1`` baseline: a deterministic layer for the seed (both origins use the
  same seed, so their baselines are byte-identical);
* ``v2`` variant: the same layer with exactly one 64 KiB block replaced.

Checking only reads metadata.  ``HEAD /origin/layer`` returns the strong ETag
and length; ``GET /origin/probe?windows=s:n,...`` returns short SHA-256
digests for requested block windows (64 hex chars per window).  A probe may
carry ``If-Match: <ETag>`` so it always answers for the locked version, even
after ``POST /origin/switch``.  A single range ``GET`` with ``If-Range`` stays
available so tests can fetch the differing block and verify its bytes; the
checker itself never downloads layer body.

Digests used by probes (all SHA-256):

    block_digest(i)     = sha256(bytes of block i)   # short last block uses its real length
    window_digest(s, n) = sha256(block_digest(s) || ... || block_digest(s+n-1))

The store snapshots the version when a request starts; switching to ``v2``
while a response is streaming keeps that response on ``v1``.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

BLOCK = 64 * 1024
GENERATOR_REV = "g1"
# The block replaced in v2 is a deterministic interior block.  The last block
# is deliberately avoided so the "short last block" path stays independent;
# callers may override it (``diff_block=-1`` selects the last block).
DIFF_BLOCK_SLOT = 5  # 5/8 of the way through the layer


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _shake(*parts: object) -> bytes:
    material = ":".join([GENERATOR_REV, *(str(p) for p in parts)])
    return hashlib.shake_256(material.encode("ascii")).digest(BLOCK)


def default_diff_block(size: int) -> int:
    blocks = (size + BLOCK - 1) // BLOCK
    if blocks <= 1:
        return 0
    return min(blocks - 2, blocks * DIFF_BLOCK_SLOT // 8)


@dataclass(frozen=True)
class OriginVersion:
    name: str
    size: int
    seed: int
    variant: bool
    diff_block: Optional[int]
    sha256: str
    etag: str
    # Cached SHA-256 of every block; excluded from repr/compare to stay small.
    block_digests: tuple[bytes, ...] = field(
        default=(), repr=False, compare=False
    )

    @property
    def block_count(self) -> int:
        return (self.size + BLOCK - 1) // BLOCK

    def block_length(self, index: int) -> int:
        return min(BLOCK, self.size - index * BLOCK)

    def block_bytes(self, index: int) -> bytes:
        if self.variant and index == self.diff_block:
            data = _shake(self.seed, index, "variant")
        else:
            data = _shake(self.seed, index)
        return data[: self.block_length(index)]

    def block_digest(self, index: int) -> bytes:
        return self.block_digests[index]

    def window_digest(self, start: int, count: int) -> bytes:
        material = b"".join(self.block_digests[start : start + count])
        return hashlib.sha256(material).digest()


def build_version(
    name: str,
    size: int,
    seed: int,
    *,
    variant: bool = False,
    diff_block: Optional[int] = None,
) -> OriginVersion:
    if size <= 0:
        raise ValueError("layer size must be positive")
    blocks = (size + BLOCK - 1) // BLOCK
    resolved_diff: Optional[int] = None
    if variant:
        if diff_block is None:
            resolved_diff = default_diff_block(size)
        else:
            resolved_diff = diff_block if diff_block >= 0 else blocks + diff_block
        if not 0 <= resolved_diff < blocks:
            raise ValueError(f"diff_block out of range for {blocks} blocks")

    draft = OriginVersion(
        name=name, size=size, seed=seed, variant=variant,
        diff_block=resolved_diff, sha256="", etag="",
    )
    full = hashlib.sha256()
    digests: list[bytes] = []
    for index in range(blocks):
        data = draft.block_bytes(index)
        full.update(data)
        digests.append(hashlib.sha256(data).digest())
    sha = full.hexdigest()
    tag = (
        f"{GENERATOR_REV}.{seed}.d{resolved_diff}"
        if variant
        else f"{GENERATOR_REV}.{seed}.base"
    )
    return OriginVersion(
        name=name, size=size, seed=seed, variant=variant,
        diff_block=resolved_diff, sha256=sha,
        etag=f'"{tag}-{sha[:32]}"', block_digests=tuple(digests),
    )


@dataclass
class RequestRecord:
    id: int
    kind: str
    method: str
    range_header: Optional[str]
    if_range: Optional[str]
    version: str
    status: int
    started_at: str
    body_bytes: int = 0
    finished_at: Optional[str] = None
    outcome: str = "in_flight"


class OriginStore:
    """Two equal-length versions plus request and byte accounting."""

    def __init__(
        self,
        size: int,
        chunk_size: int = 64 * 1024,
        seed: int = 20261124,
        diff_block: Optional[int] = None,
    ):
        if size <= 0:
            raise ValueError("layer size must be positive")
        self.chunk_size = max(1, chunk_size)
        self.seed = seed
        self._lock = threading.Lock()
        self._variant_block = diff_block
        self._build(size)
        self._current_name = "v1"
        self._fault_remaining = 0
        self._reset_stats()

    def _build(self, size: int) -> None:
        self.size = size
        self._versions = {
            "v1": build_version("v1", size, self.seed),
            "v2": build_version(
                "v2", size, self.seed, variant=True, diff_block=self._variant_block
            ),
        }

    # ------------------------------------------------------------------ state
    def _reset_stats(self) -> None:
        self._seq = 0
        self._records: list[RequestRecord] = []
        self._request_count = 0
        self._by_kind = {"head": 0, "probe": 0, "range": 0, "get": 0}
        self._layer_body_bytes = 0
        self._metadata_body_bytes = 0

    def reset(
        self, size: Optional[int] = None, diff_block: Optional[int] = None
    ) -> None:
        with self._lock:
            rebuild = False
            if size is not None and size != self.size:
                self.size = size
                rebuild = True
            # ``None`` means "auto": the scenario default interior block.
            if diff_block != self._variant_block:
                self._variant_block = diff_block
                rebuild = True
            if rebuild:
                self._build(self.size)
            self._current_name = "v1"
            self._fault_remaining = 0
            self._reset_stats()

    def reset_stats(self) -> None:
        with self._lock:
            self._reset_stats()

    def set_fault(self, remaining: int) -> None:
        with self._lock:
            self._fault_remaining = max(0, remaining)

    def take_fault(self) -> bool:
        """Consume one injected failure, if any."""
        with self._lock:
            if self._fault_remaining > 0:
                self._fault_remaining -= 1
                return True
            return False

    def current(self) -> OriginVersion:
        with self._lock:
            return self._versions[self._current_name]

    def current_name(self) -> str:
        with self._lock:
            return self._current_name

    def version_names(self) -> list[str]:
        return list(self._versions)

    def get(self, name: str) -> OriginVersion:
        return self._versions[name]

    def by_etag(self, etag: str) -> Optional[OriginVersion]:
        for version in self._versions.values():
            if version.etag == etag:
                return version
        return None

    def switch(self, name: str = "v2") -> dict:
        with self._lock:
            if name not in self._versions:
                raise KeyError(f"unknown origin version {name!r}")
            previous = self._current_name
            self._current_name = name
            return {"from": previous, "to": name, "changed": previous != name}

    # --------------------------------------------------------------- metrics
    def begin(
        self, *, kind: str, method: str, range_header: Optional[str],
        if_range: Optional[str], version: OriginVersion, status: int,
    ) -> RequestRecord:
        with self._lock:
            self._seq += 1
            record = RequestRecord(
                id=self._seq, kind=kind, method=method,
                range_header=range_header, if_range=if_range,
                version=version.name, status=status, started_at=utc_now(),
            )
            self._records.append(record)
            if len(self._records) > 60:
                del self._records[: len(self._records) - 60]
            self._request_count += 1
            self._by_kind[kind] = self._by_kind.get(kind, 0) + 1
            return record

    def record_body(self, record: RequestRecord, amount: int) -> None:
        if amount <= 0:
            return
        with self._lock:
            record.body_bytes += amount
            if record.kind == "probe":
                self._metadata_body_bytes += amount
            else:
                self._layer_body_bytes += amount

    def finish(self, record: RequestRecord, outcome: str = "ok") -> None:
        with self._lock:
            record.finished_at = utc_now()
            record.outcome = outcome

    def stats(self) -> dict:
        with self._lock:
            current = self._versions[self._current_name]
            return {
                "current_version": self._current_name,
                "current_etag": current.etag,
                "current_sha256": current.sha256,
                "size": current.size,
                "diff_block": self._versions["v2"].diff_block,
                "requests": self._request_count,
                **{f"{kind}_requests": count for kind, count in self._by_kind.items()},
                "layer_body_bytes": self._layer_body_bytes,
                "metadata_body_bytes": self._metadata_body_bytes,
                "fault_remaining": self._fault_remaining,
                "recent": [
                    {
                        "id": r.id, "kind": r.kind, "method": r.method,
                        "range": r.range_header, "if_range": r.if_range,
                        "version": r.version, "status": r.status,
                        "body_bytes": r.body_bytes, "outcome": r.outcome,
                        "started_at": r.started_at, "finished_at": r.finished_at,
                    }
                    for r in reversed(self._records)
                ],
            }

    def describe_versions(self) -> list[dict]:
        return [
            {
                "name": v.name, "size": v.size, "sha256": v.sha256,
                "etag": v.etag, "block_size": BLOCK, "blocks": v.block_count,
                "diff_block": v.diff_block,
            }
            for v in self._versions.values()
        ]

    # -------------------------------------------------------------- streaming
    def stream(self, version: OriginVersion, start: int, end: int, record):
        """Return an async iterator of body pieces for ``[start, end]``."""

        async def generator():
            try:
                pos = start
                while pos <= end:
                    block = version.block_bytes(pos // BLOCK)
                    offset = pos % BLOCK
                    take = min(end - pos + 1, BLOCK - offset, self.chunk_size)
                    piece = block[offset : offset + take]
                    pos += len(piece)
                    self.record_body(record, len(piece))
                    yield piece
                    await asyncio.sleep(0)
                self.finish(record, "ok")
            except (asyncio.CancelledError, GeneratorExit):
                self.finish(record, "aborted")
                raise

        return generator()


def _parse_range(header: str, size: int) -> Optional[tuple[int, int]]:
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", header.strip())
    if not match:
        return None
    raw_start, raw_end = match.groups()
    if raw_start == "" and raw_end == "":
        return None
    if raw_start == "":
        suffix = int(raw_end)
        if suffix <= 0:
            return None
        return max(0, size - suffix), size - 1
    start = int(raw_start)
    end = int(raw_end) if raw_end else size - 1
    if start >= size or start > end:
        return None
    return start, min(end, size - 1)


def _meta_headers(
    version: OriginVersion, *, locked: bool, current_name: str
) -> dict[str, str]:
    return {
        "ETag": version.etag,
        "X-Layer-Size": str(version.size),
        "X-Layer-SHA256": version.sha256,
        "X-Layer-Block-Size": str(BLOCK),
        "X-Layer-Blocks": str(version.block_count),
        # Version whose bytes this response describes (may be a locked one).
        "X-Origin-Version": version.name,
        # Version the origin is serving right now.
        "X-Origin-Current-Version": current_name,
        "X-Version-Locked": "true" if locked else "false",
        "Cache-Control": "no-store",
    }


def _resolve_locked(
    store: OriginStore, request: Request
) -> tuple[Optional[OriginVersion], bool, Optional[str]]:
    """Return ``(version, served_locked, requested_etag_when_unknown)``."""
    if_match = request.headers.get("if-match")
    current = store.current()
    if not if_match:
        return current, False, None
    version = store.by_etag(if_match)
    if version is None:
        return None, False, if_match
    return version, version.name != current.name, None


def _parse_windows(raw: Optional[str], blocks: int) -> list[tuple[int, int]]:
    if not raw:
        return [(0, blocks)]
    windows: list[tuple[int, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            start_s, count_s = item.split(":", 1)
            start, count = int(start_s), int(count_s)
        except ValueError as exc:
            raise ValueError(f"invalid window {item!r}") from exc
        if start < 0 or count < 1 or start + count > blocks:
            raise ValueError(f"window {item!r} out of range")
        windows.append((start, count))
    if not windows:
        raise ValueError("no windows")
    if len(windows) > 64:
        raise ValueError("at most 64 windows per probe")
    if sum(count for _, count in windows) > blocks:
        raise ValueError("windows overlap the layer more than once")
    return windows


FAULT_BODY = b'{"detail":"origin fault injected"}'


def make_origin_router(store: OriginStore, prefix: str = "/origin") -> APIRouter:
    router = APIRouter(prefix=prefix, tags=["origin"])

    def fault_response(kind: str, method: str, request: Request) -> Optional[Response]:
        if not store.take_fault():
            return None
        record = store.begin(
            kind=kind, method=method,
            range_header=request.headers.get("range"),
            if_range=request.headers.get("if-range"),
            version=store.current(), status=503,
        )
        store.record_body(record, len(FAULT_BODY))
        store.finish(record, "fault_injected")
        return Response(
            status_code=503,
            content=FAULT_BODY,
            media_type="application/json",
            headers={"Retry-After": "1", "X-Origin-Fault": "true"},
        )

    @router.head("/layer")
    async def head_layer(request: Request):
        fault = fault_response("head", "HEAD", request)
        if fault is not None:
            return fault
        version, locked, unknown = _resolve_locked(store, request)
        if version is None:
            record = store.begin(
                kind="head", method="HEAD", range_header=None,
                if_range=request.headers.get("if-range"),
                version=store.current(), status=412,
            )
            store.finish(record, "lock_mismatch")
            return Response(
                status_code=412,
                headers={
                    "ETag": store.current().etag,
                    "X-Layer-Size": str(store.current().size),
                    "X-Lock-Requested": unknown or "",
                },
            )
        record = store.begin(
            kind="head", method="HEAD", range_header=None,
            if_range=request.headers.get("if-range"),
            version=version, status=200,
        )
        store.finish(record)
        return Response(
            status_code=200,
            headers={
                **_meta_headers(version, locked=locked, current_name=store.current_name()),
                "Accept-Ranges": "bytes",
            },
        )

    @router.get("/probe")
    async def probe(request: Request, windows: Optional[str] = Query(default=None)):
        """Window digests as hex text, 64 chars per window, in request order."""
        fault = fault_response("probe", "GET", request)
        if fault is not None:
            return fault
        version, locked, unknown = _resolve_locked(store, request)
        if version is None:
            record = store.begin(
                kind="probe", method="GET", range_header=None,
                if_range=None, version=store.current(), status=412,
            )
            store.finish(record, "lock_mismatch")
            return Response(
                status_code=412,
                headers={
                    "ETag": store.current().etag,
                    "X-Lock-Requested": unknown or "",
                },
            )
        try:
            requested = _parse_windows(windows, version.block_count)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        record = store.begin(
            kind="probe", method="GET", range_header=None,
            if_range=None, version=version, status=200,
        )
        payload = "".join(
            version.window_digest(start, count).hex()
            for start, count in requested
        ).encode("ascii")
        store.record_body(record, len(payload))
        store.finish(record)
        return Response(
            content=payload,
            media_type="text/plain",
            headers={
                **_meta_headers(version, locked=locked, current_name=store.current_name()),
                "X-Probe-Windows": str(len(requested)),
                "X-Probe-Blocks": str(sum(count for _, count in requested)),
            },
        )

    @router.get("/layer")
    async def get_layer(request: Request):
        fault = fault_response("get", "GET", request)
        if fault is not None:
            return fault
        version, locked, unknown = _resolve_locked(store, request)
        if version is None:
            record = store.begin(
                kind="range", method="GET",
                range_header=request.headers.get("range"),
                if_range=request.headers.get("if-range"),
                version=store.current(), status=412,
            )
            store.finish(record, "lock_mismatch")
            return Response(
                status_code=412,
                headers={
                    "ETag": store.current().etag,
                    "X-Lock-Requested": unknown or "",
                },
            )
        range_header = request.headers.get("range")
        if_range = request.headers.get("if-range")

        if range_header and if_range is not None and if_range != version.etag:
            # RFC 9110: failed If-Range -> ignore Range, send full body.
            record = store.begin(
                kind="range", method="GET", range_header=range_header,
                if_range=if_range, version=version, status=200,
            )
            headers = {
                **_meta_headers(version, locked=locked, current_name=store.current_name()),
                "Content-Length": str(version.size),
                "Accept-Ranges": "bytes",
                "X-If-Range-Matched": "false",
            }
            return StreamingResponse(
                store.stream(version, 0, version.size - 1, record),
                status_code=200, headers=headers,
                media_type="application/octet-stream",
            )

        if range_header:
            parsed = _parse_range(range_header, version.size)
            if parsed is None:
                record = store.begin(
                    kind="range", method="GET", range_header=range_header,
                    if_range=if_range, version=version, status=416,
                )
                store.finish(record, "invalid_range")
                return Response(
                    status_code=416,
                    headers={
                        "Content-Range": f"bytes */{version.size}",
                        "ETag": version.etag,
                        "X-Origin-Version": version.name,
                    },
                )
            start, end = parsed
            record = store.begin(
                kind="range", method="GET", range_header=range_header,
                if_range=if_range, version=version, status=206,
            )
            headers = {
                **_meta_headers(version, locked=locked, current_name=store.current_name()),
                "Content-Length": str(end - start + 1),
                "Content-Range": f"bytes {start}-{end}/{version.size}",
                "Accept-Ranges": "bytes",
                "X-If-Range-Matched": "true" if if_range else "not_sent",
            }
            return StreamingResponse(
                store.stream(version, start, end, record),
                status_code=206, headers=headers,
                media_type="application/octet-stream",
            )

        record = store.begin(
            kind="get", method="GET", range_header=None,
            if_range=None, version=version, status=200,
        )
        headers = {
            **_meta_headers(version, locked=locked, current_name=store.current_name()),
            "Content-Length": str(version.size),
            "Accept-Ranges": "bytes",
        }
        return StreamingResponse(
            store.stream(version, 0, version.size - 1, record),
            status_code=200, headers=headers,
            media_type="application/octet-stream",
        )

    @router.post("/switch")
    async def switch_version(version: str = "v2"):
        try:
            result = store.switch(version)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, **result, "at": utc_now(), "state": store.stats()}

    @router.post("/fault")
    async def inject_fault(count: int = 100):
        if count < 1 or count > 10000:
            raise HTTPException(status_code=400, detail="bad fault count")
        store.set_fault(count)
        return {"ok": True, "fault_remaining": count}

    @router.post("/reset")
    async def reset_origin(
        size_bytes: Optional[int] = None, diff_block: Optional[int] = None
    ):
        if size_bytes is not None and not (
            BLOCK <= size_bytes <= 128 * 1024 * 1024
        ):
            raise HTTPException(
                status_code=400,
                detail="size_bytes must be between 65536 and 134217728",
            )
        try:
            store.reset(size_bytes, diff_block)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True, "state": store.stats(),
            "versions": store.describe_versions(),
        }

    @router.post("/reset-stats")
    async def reset_stats():
        store.reset_stats()
        return {"ok": True, "state": store.stats()}

    @router.get("/state")
    async def origin_state():
        return {"state": store.stats(), "versions": store.describe_versions()}

    return router
