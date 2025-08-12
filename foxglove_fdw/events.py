"""
Foxglove Events Foreign Data Wrapper
Read-only FDW for `GET /v1/events`.

API docs: https://docs.foxglove.dev/api#tag/Events

Exposed columns:
  id           text
  device_id    text            (from device.id or deprecated deviceId)
  device_name  text            (from device.name)
  start_time   timestamptz     (API field: start)
  end_time     timestamptz     (API field: end)
  metadata     jsonb           (API object -> JSON string)
  created_at   timestamptz     (createdAt)
  updated_at   timestamptz     (updatedAt)
  project_id   text            (not always present; filled if returned)

Push-down filters (equality): device_id, device_name, project_id, id (post filter),
                                                            start_time, end_time, created_at, updated_at.
Time range pushdown:
    - start_time and end_time: >, >=, <, <=, = are pushed (mapped to API start/end;
        equality sets both bounds). If only one bound is present, the other is supplied
        to satisfy API requirements.
    - created_at and updated_at: only lower bounds (>, >=, =) are pushed via
        createdAfter/updatedAfter (upper bounds are not pushed).

ORDER BY push-down: id, device_id, device_name, start_time, created_at, updated_at.
Sorting is mapped to the API's sortBy list; device_name maps to deviceName.
"""

from __future__ import annotations
from multicorn import ForeignDataWrapper, SortKey
from multicorn.utils import log_to_postgres, WARNING
from typing import Dict, Any, List
import requests, json, datetime as dt
from .utils import to_iso8601, parse_dt


class FoxgloveEventsFDW(ForeignDataWrapper):
    SUPPORTED_SORT_FIELDS = {
        "id",
        "device_id",  # maps to deviceId
        "device_name",  # maps to deviceName
        "start_time",  # maps to start
        "created_at",  # maps to createdAt
        "updated_at",  # maps to updatedAt
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

    # ---------- planner --------------------------------------------------
    def get_rel_size(self, quals, columns):  # type: ignore[override]
        return 2000, len(columns) * 72

    def can_sort(self, sortkeys):
        handled = []
        for sk in sortkeys:
            if sk.attname in self.SUPPORTED_SORT_FIELDS:
                handled.append(sk)
            else:
                break
        return handled

    # ---------- executor -------------------------------------------------
    def execute(  # type: ignore[override]
        self, quals: List, columns: List, sortkeys: List[SortKey] | None = None
    ):
        params: Dict[str, Any] = {}
        limit_ = None
        metadata_query_parts: list[str] = []
        # Time pushdown accumulators
        start_lower: str | None = None  # API param 'start' (lower bound on start)
        end_upper: str | None = None  # API param 'end'   (upper bound on end)
        created_after_candidates: list[str] = []  # API supports createdAfter
        updated_after_candidates: list[str] = []  # API supports updatedAfter

        for q in quals:
            fn = q.field_name
            op = q.operator

            # Advanced metadata filters
            # Support: metadata @> '{"key":"val", "k2":["v1","v2"], "k3":"*"}'
            if fn == "metadata" and op == "@>":
                raw = q.value
                md: dict[str, Any] = {}
                if isinstance(raw, dict):
                    md = raw
                else:
                    try:
                        md = json.loads(raw)
                    except Exception:
                        md = {}
                for k, v in md.items():
                    if v is None:
                        continue
                    if isinstance(v, (list, tuple, set)):
                        metadata_query_parts.append(f"{k}:{','.join(str(x) for x in v)}")
                    elif v == "*":
                        metadata_query_parts.append(f"{k}:*")
                    else:
                        metadata_query_parts.append(f"{k}:{v}")
                continue

            # Pseudo-column filters: metadata_<key> = value
            if fn.startswith("metadata_") and op == "=":
                key = fn.removeprefix("metadata_")
                val = q.value
                if isinstance(val, (list, tuple, set)):
                    metadata_query_parts.append(f"{key}:{','.join(str(x) for x in val)}")
                else:
                    metadata_query_parts.append(f"{key}:{val}")
                continue

            # Generic string search in metadata: metadata = 'foo'
            if fn == "metadata" and op == "=" and isinstance(q.value, str):
                # unstructured token search
                metadata_query_parts.append(q.value)
                continue

            # Timestamp pushdown for start/end
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
                continue
            if fn == "end_time" and op in (">", ">="):
                iso = to_iso8601(q.value)
                if start_lower is None or iso > start_lower:
                    start_lower = iso
                continue

            # created_at/updated_at: push only lower bounds (API supports *After)
            if fn == "created_at" and op in (">", ">=", "="):
                created_after_candidates.append(to_iso8601(q.value))
                continue
            if fn == "updated_at" and op in (">", ">=", "="):
                updated_after_candidates.append(to_iso8601(q.value))
                continue

            # Standard equality filters
            if op == "=":
                if fn == "device_id":
                    params["deviceId"] = q.value
                elif fn == "device_name":
                    params["deviceName"] = q.value
                elif fn == "project_id":
                    params["projectId"] = q.value
                elif fn == "limit":
                    limit_ = int(q.value)
                elif fn == "id":
                    # post-filter later
                    pass

        # Finalize timestamp params
        if start_lower and not end_upper:
            end_upper = to_iso8601(dt.datetime.now(dt.timezone.utc))
        if end_upper and not start_lower:
            start_lower = "1970-01-01T00:00:00Z"
        if start_lower:
            params["start"] = start_lower
        if end_upper:
            params["end"] = end_upper

        if created_after_candidates:
            params["createdAfter"] = max(created_after_candidates)
        if updated_after_candidates:
            params["updatedAfter"] = max(updated_after_candidates)

        if limit_:
            params["limit"] = limit_
        if metadata_query_parts:
            params["query"] = " ".join(metadata_query_parts)

        if sortkeys:
            sk = sortkeys[0]
            fld = sk.attname
            if fld in self.SUPPORTED_SORT_FIELDS:
                api_field = {
                    "device_id": "deviceId",
                    "device_name": "deviceName",
                    "start_time": "start",
                    "created_at": "createdAt",
                    "updated_at": "updatedAt",
                }.get(fld, fld)
                params["sortBy"] = api_field
                params["sortOrder"] = "desc" if sk.is_reversed else "asc"

        try:
            r = requests.get(
                f"{self.base_url}/events",
                headers={"Authorization": f"Bearer {self.api_key}"},
                params=params,
                timeout=60,
            )
            r.raise_for_status()
        except requests.HTTPError as http_err:
            body = None
            try:
                body = r.text  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(
                f"foxglove_events FDW upstream error {http_err.response.status_code if http_err.response else ''}: {body} (params={params})"
            )
        events: list[dict] = r.json()

        if sortkeys and "sortBy" not in params:
            sk = sortkeys[0]
            api_key = {
                "device_id": "deviceId",
                "device_name": "deviceName",
                "start_time": "start",
                "created_at": "createdAt",
                "updated_at": "updatedAt",
            }.get(sk.attname, sk.attname)
            events.sort(key=lambda d, k=api_key: str(d.get(k) or ""), reverse=sk.is_reversed)  # type: ignore

        for e in events:
            dev = e.get("device") or {}
            row = {
                "id": e.get("id"),
                "device_id": dev.get("id") or e.get("deviceId"),
                "device_name": dev.get("name"),
                "start_time": e.get("start"),
                "end_time": e.get("end"),
                "metadata": json.dumps(e.get("metadata", {})),
                "created_at": e.get("createdAt"),
                "updated_at": e.get("updatedAt"),
                "project_id": e.get("projectId"),
            }
            if not self._row_matches_quals(row, quals):
                continue
            yield {c: row.get(c) for c in columns}

    # ---------- helpers --------------------------------------------------
    @staticmethod
    def _row_matches_quals(row: Dict[str, Any], quals: List) -> bool:

        for q in quals:
            fn, op = q.field_name, q.operator
            if op == "=" and fn in row:
                if str(row[fn]) != str(q.value):
                    return False
            elif fn == "metadata" and op == "@>":
                # local containment check for consistency
                try:
                    want = q.value if isinstance(q.value, dict) else json.loads(q.value)
                except Exception:
                    continue
                if isinstance(want, dict):
                    try:
                        current = row.get("metadata")
                        cur_obj = current if isinstance(current, dict) else json.loads(current)  # type: ignore
                    except Exception:
                        return False
                    for k, v in want.items():
                        if k not in cur_obj:
                            return False
                        cv = cur_obj[k]
                        if isinstance(v, (list, tuple, set)):
                            if cv not in v:
                                return False
                        elif v == "*":
                            continue
                        else:
                            if str(cv) != str(v):
                                return False
            elif fn in ("start_time", "end_time", "created_at", "updated_at") and op in (
                ">",
                ">=",
                "<",
                "<=",
            ):
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
