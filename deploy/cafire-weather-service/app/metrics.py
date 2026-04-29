from __future__ import annotations

import math
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


WINDOWS_SEC = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
}
MAX_EVENTS = 75_000


@dataclass(frozen=True)
class RequestEvent:
    at: float
    method: str
    path: str
    route: str
    status_code: int
    total_ms: int


@dataclass(frozen=True)
class MeteogramEvent:
    at: float
    endpoint: str
    status_code: int
    ok: bool
    sample_path: str | None
    total_ms: int | None
    sample_total_ms: int | None
    render_total_ms: int | None
    cache_hit: bool | None
    fast_store_hit: bool | None
    forecast_hour_count: int | None
    fetch_count: int | None
    blocker_count: int | None
    date_yyyymmdd: str | None
    cycle_utc: int | None
    error: str | None


class RollingMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: deque[RequestEvent] = deque(maxlen=MAX_EVENTS)
        self._meteograms: deque[MeteogramEvent] = deque(maxlen=MAX_EVENTS)
        self._started_at = time.time()

    def observe_request(
        self,
        *,
        method: str,
        path: str,
        route: str,
        status_code: int,
        total_ms: int,
    ) -> None:
        event = RequestEvent(
            at=time.time(),
            method=method,
            path=path,
            route=route,
            status_code=status_code,
            total_ms=max(0, int(total_ms)),
        )
        with self._lock:
            self._requests.append(event)
            self._prune_locked()

    def observe_meteogram(self, payload: dict[str, Any]) -> None:
        event = MeteogramEvent(
            at=time.time(),
            endpoint=str(payload.get("endpoint") or ""),
            status_code=int(payload.get("status_code") or 0),
            ok=bool(payload.get("ok")),
            sample_path=payload.get("sample_path"),
            total_ms=_optional_int(payload.get("total_ms")),
            sample_total_ms=_optional_int(payload.get("sample_total_ms")),
            render_total_ms=_optional_int(payload.get("render_total_ms")),
            cache_hit=_optional_bool(payload.get("cache_hit")),
            fast_store_hit=_optional_bool(payload.get("fast_store_hit")),
            forecast_hour_count=_optional_int(payload.get("forecast_hour_count")),
            fetch_count=_optional_int(payload.get("fetch_count")),
            blocker_count=_optional_int(payload.get("blocker_count")),
            date_yyyymmdd=_optional_str(payload.get("date_yyyymmdd")),
            cycle_utc=_optional_int(payload.get("cycle_utc")),
            error=_optional_str(payload.get("error")),
        )
        with self._lock:
            self._meteograms.append(event)
            self._prune_locked()

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            self._prune_locked(now)
            requests = list(self._requests)
            meteograms = list(self._meteograms)
            started_at = self._started_at

        return {
            "ok": True,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "process_started_at_utc": datetime.fromtimestamp(started_at, UTC).isoformat(),
            "uptime_sec": int(now - started_at),
            "windows": {
                label: {
                    "requests": _request_window(
                        [event for event in requests if event.at >= now - seconds],
                        seconds,
                    ),
                    "meteograms": _meteogram_window(
                        [event for event in meteograms if event.at >= now - seconds],
                        seconds,
                    ),
                }
                for label, seconds in WINDOWS_SEC.items()
            },
            "retained": {
                "request_events": len(requests),
                "meteogram_events": len(meteograms),
                "retention_sec": max(WINDOWS_SEC.values()),
            },
        }

    def _prune_locked(self, now: float | None = None) -> None:
        cutoff = (now or time.time()) - max(WINDOWS_SEC.values())
        while self._requests and self._requests[0].at < cutoff:
            self._requests.popleft()
        while self._meteograms and self._meteograms[0].at < cutoff:
            self._meteograms.popleft()


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _percentile(values: list[int], q: float) -> int | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    index = (len(values) - 1) * q
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return values[low]
    return int(round(values[low] * (high - index) + values[high] * (index - low)))


def _latency(values: list[int | None]) -> dict[str, int | None]:
    present = [int(value) for value in values if value is not None]
    return {
        "p50": _percentile(present, 0.50),
        "p90": _percentile(present, 0.90),
        "p95": _percentile(present, 0.95),
        "p99": _percentile(present, 0.99),
        "max": max(present) if present else None,
    }


def _status_group(status_code: int) -> str:
    if status_code <= 0:
        return "unknown"
    return f"{status_code // 100}xx"


def _rate(count: int, seconds: int) -> float:
    return round(count / max(seconds, 1), 4)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(100.0 * numerator / denominator, 2)


def _request_window(events: list[RequestEvent], seconds: int) -> dict[str, Any]:
    count = len(events)
    status_groups = Counter(_status_group(event.status_code) for event in events)
    routes: dict[str, list[RequestEvent]] = {}
    for event in events:
        routes.setdefault(event.route, []).append(event)

    route_rows = []
    for route, route_events in routes.items():
        error_count = sum(1 for event in route_events if event.status_code >= 500)
        route_rows.append(
            {
                "route": route,
                "count": len(route_events),
                "rps": _rate(len(route_events), seconds),
                "error_count": error_count,
                "error_rate_pct": _pct(error_count, len(route_events)),
                "latency_ms": _latency([event.total_ms for event in route_events]),
            }
        )

    slowest = sorted(events, key=lambda event: event.total_ms, reverse=True)[:10]
    return {
        "count": count,
        "rps": _rate(count, seconds),
        "error_count": sum(1 for event in events if event.status_code >= 500),
        "error_rate_pct": _pct(sum(1 for event in events if event.status_code >= 500), count),
        "status_groups": dict(sorted(status_groups.items())),
        "latency_ms": _latency([event.total_ms for event in events]),
        "routes": sorted(route_rows, key=lambda row: (row["count"], row["latency_ms"]["p95"] or 0), reverse=True)[:20],
        "slowest": [
            {
                "at_utc": datetime.fromtimestamp(event.at, UTC).isoformat(),
                "route": event.route,
                "status_code": event.status_code,
                "total_ms": event.total_ms,
            }
            for event in slowest
        ],
    }


def _meteogram_window(events: list[MeteogramEvent], seconds: int) -> dict[str, Any]:
    count = len(events)
    ok_count = sum(1 for event in events if event.ok)
    cache_known = [event for event in events if event.cache_hit is not None]
    fast_known = [event for event in events if event.fast_store_hit is not None]
    paths = Counter(event.sample_path or "unknown" for event in events)
    by_endpoint = Counter(event.endpoint for event in events)
    slowest = sorted(events, key=lambda event: event.total_ms or -1, reverse=True)[:10]
    return {
        "count": count,
        "rps": _rate(count, seconds),
        "ok_count": ok_count,
        "error_count": count - ok_count,
        "error_rate_pct": _pct(count - ok_count, count),
        "cache_hit_rate_pct": _pct(sum(1 for event in cache_known if event.cache_hit), len(cache_known)),
        "fast_store_hit_rate_pct": _pct(sum(1 for event in fast_known if event.fast_store_hit), len(fast_known)),
        "sample_paths": dict(paths.most_common()),
        "by_endpoint": dict(by_endpoint.most_common()),
        "total_ms": _latency([event.total_ms for event in events]),
        "sample_total_ms": _latency([event.sample_total_ms for event in events]),
        "render_total_ms": _latency([event.render_total_ms for event in events]),
        "fetch_count": _latency([event.fetch_count for event in events]),
        "blocker_count": _latency([event.blocker_count for event in events]),
        "slowest": [
            {
                "at_utc": datetime.fromtimestamp(event.at, UTC).isoformat(),
                "endpoint": event.endpoint,
                "status_code": event.status_code,
                "sample_path": event.sample_path,
                "total_ms": event.total_ms,
                "sample_total_ms": event.sample_total_ms,
                "render_total_ms": event.render_total_ms,
                "cache_hit": event.cache_hit,
                "fast_store_hit": event.fast_store_hit,
                "forecast_hour_count": event.forecast_hour_count,
                "fetch_count": event.fetch_count,
                "blocker_count": event.blocker_count,
                "date_yyyymmdd": event.date_yyyymmdd,
                "cycle_utc": event.cycle_utc,
                "error": event.error,
            }
            for event in slowest
        ],
    }


metrics = RollingMetrics()
