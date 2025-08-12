"""
Foxglove Recordings Foreign Data Wrapper
Exposes GET /v1/recordings with columns start_time / end_time instead of start / end.
"""

from __future__ import annotations
from multicorn import ForeignDataWrapper, SortKey
from multicorn.utils import log_to_postgres, WARNING
from typing import Dict, List, Any, Optional
import requests, json, datetime as dt
from .utils import to_iso8601, parse_dt


class FoxgloveRecordingsFDW(ForeignDataWrapper):
    # sortBy values accepted by the API; we map start_time → start, etc.
    SUPPORTED_SORT_FIELDS = {
        "deviceName",
        "device.name",
        "createdAt",
        "start_time",
        "end_time",
        "duration",
        "path",
        "importedAt",
    }

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

    # ---------- planner ------------------------------------------------------
    def get_rel_size(self, quals, columns):  # type: ignore[override]
        return 2000, len(columns) * 80

    def can_sort(self, sortkeys):
        handled = []
        for sk in sortkeys:
            if sk.attname in self.SUPPORTED_SORT_FIELDS:
                handled.append(sk)
            else:
                break
        return handled

    # ---------- executor -----------------------------------------------------
    def execute(  # type: ignore[override]
        self,
        quals: List,
        columns: List,
        sortkeys: List[SortKey] | None = None,
    ):
        params: Dict[str, Any] = {}
        limit_ = None

        # ---- push‑down filters ---------------------------------------------
        # Track time bounds we can push to the API. The Foxglove API accepts
        #   start: lower bound on start time (>=)
        #   end:   upper bound on end time (<=)
        start_lower: Optional[str] = None
        end_upper: Optional[str] = None

        for q in quals:
            fn = q.field_name
            op = getattr(q, "operator", "=")

            # direct string/enum equalities we can pass through
            eq_map = {
                "deviceId": "deviceId",
                "device_id": "deviceId",
                "deviceName": "deviceName",
                "device_name": "deviceName",
                "path": "path",
                "projectId": "projectId",
                "project_id": "projectId",
                "importStatus": "importStatus",
                "import_status": "importStatus",
            }
            if op == "=" and fn in eq_map:
                params[eq_map[fn]] = q.value
                continue

            # Push start_time lower bounds and tighten equality with an upper bound
            if fn == "start_time" and op in (">", ">=", "="):
                iso = to_iso8601(q.value)
                # keep the most restrictive (max) lower bound if multiple quals
                if start_lower is None or iso > start_lower:
                    start_lower = iso
                # If it's equality, also tighten the upper bound to the same instant
                if op == "=":
                    if end_upper is None or iso < end_upper:
                        end_upper = iso
                continue

            # Push start_time upper bounds (<, <=) by mapping to API 'end'
            if fn == "start_time" and op in ("<", "<="):
                iso = to_iso8601(q.value)
                if end_upper is None or iso < end_upper:
                    end_upper = iso
                continue

            # Push end_time upper bounds
            if fn == "end_time" and op in ("<", "<=", "="):
                iso = to_iso8601(q.value)
                # keep the most restrictive (min) upper bound if multiple quals
                if end_upper is None or iso < end_upper:
                    end_upper = iso
                continue

            # Push end_time lower bounds (end_time > X) by using start of the window
            if fn == "end_time" and op in (">", ">="):
                iso = to_iso8601(q.value)
                if start_lower is None or iso > start_lower:
                    start_lower = iso
                continue

            # capture a LIMIT if a pseudo column was used (rare)
            if op == "=" and fn == "limit":
                try:
                    limit_ = int(q.value)
                except Exception:
                    limit_ = None

        # If either bound is present, supply both to satisfy API requirements
        if start_lower and not end_upper:
            end_upper = to_iso8601(dt.datetime.now(dt.timezone.utc))
        if end_upper and not start_lower:
            start_lower = "1970-01-01T00:00:00Z"
        if start_lower:
            params["start"] = start_lower
        if end_upper:
            params["end"] = end_upper

        if limit_:
            params["limit"] = limit_

        # ---- push‑down ORDER BY --------------------------------------------
        if sortkeys:
            sk = sortkeys[0]
            fld = sk.attname
            if fld in self.SUPPORTED_SORT_FIELDS:
                # map column names → API field names
                api_sort_field = {"start_time": "start", "end_time": "end"}.get(fld, fld)
                params["sortBy"] = api_sort_field
                params["sortOrder"] = "desc" if sk.is_reversed else "asc"

        # Final guard: API requires both start and end if either is specified
        if ("start" in params) and ("end" not in params):
            params["end"] = to_iso8601(dt.datetime.now(dt.timezone.utc))
        if ("end" in params) and ("start" not in params):
            params["start"] = "1970-01-01T00:00:00Z"

        # ---- HTTP call ------------------------------------------------------
        try:
            r = requests.get(
                f"{self.base_url}/recordings",
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
                f"foxglove_recordings FDW upstream error {e.response.status_code if e.response else ''}: {body} (params={params})"
            )
        recs: list[dict] = r.json()

        # ---- local sort fallback -------------------------------------------
        if sortkeys and "sortBy" not in params:
            sk = sortkeys[0]
            api_key = {"start_time": "start", "end_time": "end"}.get(sk.attname, sk.attname)
            recs.sort(key=lambda d: d.get(api_key), reverse=sk.is_reversed)  # type: ignore

        # ---- yield rows -----------------------------------------------------
        for r_ in recs:
            duration_s = None
            if "duration" in columns:
                try:
                    duration_s = (
                        dt.datetime.fromisoformat(r_["end"])
                        - dt.datetime.fromisoformat(r_["start"])
                    ).total_seconds()
                except Exception:
                    duration_s = None

            row = {
                "id": r_.get("id"),
                "project_id": r_.get("projectId"),
                "path": r_.get("path"),
                "size_bytes": r_.get("size"),
                "created_at": r_.get("createdAt"),
                "imported_at": r_.get("importedAt"),
                "start_time": r_.get("start"),
                "end_time": r_.get("end"),
                "duration": duration_s,
                "import_status": r_.get("importStatus"),
                "site_id": (r_.get("site") or {}).get("id"),
                "site_name": (r_.get("site") or {}).get("name"),
                "edge_site_id": (r_.get("edgeSite") or {}).get("id"),
                "edge_site_name": (r_.get("edgeSite") or {}).get("name"),
                "device_id": (r_.get("device") or {}).get("id"),
                "device_name": (r_.get("device") or {}).get("name"),
                "key": r_.get("key"),
                "metadata": json.dumps(r_.get("metadata", [])),
            }
            if not self._row_matches_quals(row, quals):
                continue
            yield {c: row.get(c) for c in columns}

    # ---------- helpers ------------------------------------------------------
    @staticmethod
    def _row_matches_quals(row: Dict[str, Any], quals: List) -> bool:
        """Lightweight local filter to reduce rows before handing back to Postgres.
        Supports = on all fields and range ops on start_time/end_time.
        Postgres will still enforce quals, this is just a best-effort prefilter.
        """

        for q in quals:
            fn = q.field_name
            op = getattr(q, "operator", "=")
            if fn not in row:
                continue

            # Equality on any field
            if op == "=":
                if str(row[fn]) != str(q.value):
                    return False
                continue

            # Range ops on timestamptz fields
            if fn in ("start_time", "end_time") and op in (">", ">=", "<", "<="):
                lhs = parse_dt(row[fn])
                rhs = parse_dt(q.value)
                if lhs is None or rhs is None:
                    # If we can't parse, don't prefilter; let Postgres handle it
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
