"""
Foxglove Coverage Foreign Data Wrapper
Read-only FDW for `GET /v1/data/coverage`.

API docs: https://docs.foxglove.dev/api#tag/Coverage

Exposed columns:
  device_id       text          (from device.id or deviceId)
  device_name     text          (from device.name)
  start_time      timestamptz   (API field: start)
  end_time        timestamptz   (API field: end)
  status          text          (deprecated API field `status` – may be null)
  import_status   text          (API field: importStatus; only present when edge recordings included)
  tolerance       integer       (echo of the request parameter used)

Push-down filters:
  - device_id (=)        -> deviceId
  - device_name (=)      -> deviceName
  - start_time (> >= = < <=) mapped to API `start` / `end` bounds
  - end_time   (> >= = < <=) mapped similarly (start/end inclusive)
  - tolerance (=) sets request tolerance (seconds). Defaults to 30 when unspecified.

API requirements:
  - Both `start` and `end` query parameters are required. If the user only
    supplies one bound we synthesize the other (epoch or now) so the API call
    succeeds, matching the semantics used in other FDWs here.
  - We always set `includeEdgeRecordings=true` so that `importStatus` is
    available in the response.

Sorting:
  - The coverage endpoint does not document server-side sorting parameters, so
    we only support local (Postgres) ORDER BY. We still allow a single ORDER BY
    on start_time or end_time to be performed locally if the planner asks.

Pseudo columns:
  - tolerance (int) is surfaced so a query can restrict or project it.

Example usage:
  SELECT * FROM foxglove_coverage
   WHERE device_id = 'dev_123'
     AND start_time >= now() - interval '6 hours'
     AND end_time   <= now();

"""

from __future__ import annotations
from multicorn import ForeignDataWrapper, SortKey
from multicorn.utils import log_to_postgres, WARNING
from typing import Dict, Any, List, Optional
import requests, datetime as dt
from .utils import to_iso8601, parse_dt


class FoxgloveCoverageFDW(ForeignDataWrapper):
    # Only provide local sort support hints (no push-down since API lacks sort params)
    SUPPORTED_SORT_FIELDS = {"start_time", "end_time"}

    def __init__(self, options: Dict[str, str], columns: Dict[str, Any]) -> None:
        super().__init__(options, columns)
        self.columns = columns
        self.base_url = options.get("base_url", "https://api.foxglove.dev/v1")
        self.api_key = options.get("api_key")
        if not self.api_key:
            log_to_postgres(
                "foxglove_fdw: `api_key` option (or USER MAPPING) is required",
                level=WARNING,
            )

    # ---------- planner --------------------------------------------------
    def get_rel_size(self, quals, columns):  # type: ignore[override]
        # Coverage responses are generally modest; use heuristic similar to others
        return 2000, len(columns) * 64

    def can_sort(self, sortkeys):
        handled = []
        for sk in sortkeys:
            # We only claim sort capability so planner may reduce work; we still do it locally.
            if sk.attname in self.SUPPORTED_SORT_FIELDS:
                handled.append(sk)
            else:
                break
        return handled

    # ---------- executor -------------------------------------------------
    def execute(  # type: ignore[override]
        self, quals: List, columns: List, sortkeys: List[SortKey] | None = None
    ):
        params: Dict[str, Any] = {"includeEdgeRecordings": "true"}
        # Time range accumulation
        start_lower: Optional[str] = None
        end_upper: Optional[str] = None
        tolerance: Optional[int] = None

        for q in quals:
            fn = q.field_name
            op = getattr(q, "operator", "=")

            # Time bounds mapping similar to recordings/events
            if fn == "start_time" and op in (">", ">=", "="):
                iso = to_iso8601(q.value)
                if start_lower is None or iso > start_lower:
                    start_lower = iso
                if op == "=":
                    if end_upper is None or iso < end_upper:
                        end_upper = iso
                continue
            if fn == "start_time" and op in ("<", "<="):
                iso = to_iso8601(q.value)
                if end_upper is None or iso < end_upper:
                    end_upper = iso
                continue
            if fn == "end_time" and op in ("<", "<=", "="):
                iso = to_iso8601(q.value)
                if end_upper is None or iso < end_upper:
                    end_upper = iso
                if op == "=":
                    if start_lower is None or iso > start_lower:
                        start_lower = iso
                continue
            if fn == "end_time" and op in (">", ">="):
                iso = to_iso8601(q.value)
                if start_lower is None or iso > start_lower:
                    start_lower = iso
                continue

            # Equality push-downs
            if op == "=":
                if fn == "device_id":
                    params["deviceId"] = q.value
                elif fn == "device_name":
                    params["deviceName"] = q.value
                elif fn == "tolerance":
                    try:
                        tolerance = int(q.value)
                    except Exception:
                        pass

        # Provide default tolerance if not overridden
        if tolerance is None:
            tolerance = 30
        params["tolerance"] = tolerance

        # API requires both start and end. Synthesize missing bound.
        if start_lower and not end_upper:
            end_upper = to_iso8601(dt.datetime.now(dt.timezone.utc))
        if end_upper and not start_lower:
            start_lower = "1970-01-01T00:00:00Z"
        if not start_lower or not end_upper:
            # If user gave no time quals at all, we fail fast with guidance instead
            raise RuntimeError(
                "foxglove_coverage FDW: you must supply a time bound (start_time and/or end_time) to form the required start/end query params"
            )
        params["start"] = start_lower
        params["end"] = end_upper

        # ---- HTTP call --------------------------------------------------
        try:
            r = requests.get(
                f"{self.base_url}/data/coverage",
                headers={"Authorization": f"Bearer {self.api_key}"},
                params=params,
                timeout=60,
            )
            r.raise_for_status()
        except requests.HTTPError as e:
            body = None
            try:
                body = r.text  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(
                f"foxglove_coverage FDW upstream error {e.response.status_code if e.response else ''}: {body} (params={params})"
            )
        cov_ranges: list[dict] = r.json()

        # Local sort if requested (API has no sortBy)
        if sortkeys:
            sk = sortkeys[0]
            if sk.attname in self.SUPPORTED_SORT_FIELDS:
                api_key = {"start_time": "start", "end_time": "end"}.get(sk.attname, sk.attname)
                cov_ranges.sort(key=lambda d: d.get(api_key) or "", reverse=sk.is_reversed)  # type: ignore

        for rec in cov_ranges:
            dev = rec.get("device") or {}
            row = {
                "device_id": dev.get("id") or rec.get("deviceId"),
                "device_name": dev.get("name"),
                "start_time": rec.get("start"),
                "end_time": rec.get("end"),
                "status": rec.get("status"),  # deprecated
                "import_status": rec.get("importStatus"),
                "tolerance": tolerance,
            }
            if not self._row_matches_quals(row, quals):
                continue
            yield {c: row.get(c) for c in columns}

    # ---------- helpers --------------------------------------------------
    @staticmethod
    def _row_matches_quals(row: Dict[str, Any], quals: List) -> bool:
        for q in quals:
            fn = q.field_name
            op = getattr(q, "operator", "=")
            if fn not in row:
                continue
            if op == "=":
                if str(row[fn]) != str(q.value):
                    return False
            elif fn in ("start_time", "end_time") and op in (">", ">=", "<", "<="):
                lhs = parse_dt(row.get(fn))
                rhs = parse_dt(q.value)
                if lhs is None or rhs is None:
                    continue
                if op == ">" and not (lhs > rhs):
                    return False
                if op == ">=" and not (lhs >= rhs):
                    return False
                if op == "<" and not (lhs < rhs):
                    return False
                if op == "<=" and not (lhs <= rhs):
                    return False
        return True
