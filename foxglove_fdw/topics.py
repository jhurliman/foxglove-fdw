"""
Foxglove Topics Foreign Data Wrapper
Read-only FDW for `GET /v1/data/topics`.

API docs: https://docs.foxglove.dev/api#tag/Topics

Returned fields (we intentionally exclude the large `schema` field for performance):
  topic            text
  version          text
  encoding         text
  schema_name      text    (API: schemaName)
  schema_encoding  text    (API: schemaEncoding)

Push-down filters supported as query params:
    device_id      (=)         -> deviceId
    device_name    (=)         -> deviceName
    recording_id   (=)         -> recordingId
    recording_key  (=)         -> recordingKey
    start_time     (=, >=, >)  -> start (RFC3339 timestamp) (we take the max of provided values)
    end_time       (=, <=, <)  -> end   (RFC3339 timestamp) (we take the min of provided values)
    project_id     (=)         -> projectId
    limit          (=)         -> limit (numeric)

API constraints (enforced preflight):
    - You must provide either (recording_id OR recording_key), OR (device_id/device_name AND both start_time AND end_time).
    - Providing neither combination yields a user-friendly error instead of a 400 from the API.

Other quals (e.g. topic='foo') are post-filtered locally since API does not
expose direct filtering by topic/version besides time/device/recording constraints.

ORDER BY push-down: topic, version.
"""

from __future__ import annotations
from multicorn import ForeignDataWrapper, SortKey
from multicorn.utils import log_to_postgres, WARNING
from typing import Dict, Any, List, Optional
import requests
from .utils import to_iso8601


class FoxgloveTopicsFDW(ForeignDataWrapper):
    SUPPORTED_SORT_FIELDS = {"topic", "version"}

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
        # Topics list also capped at default 2000 unless limit provided
        return 2000, len(columns) * 64

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
        params: Dict[str, Any] = {"includeSchemas": "false"}  # never fetch schemas
        limit_: Optional[int] = None
        recording_id = None
        recording_key = None
        device_id = None
        device_name = None
        start_candidates: List[str] = []
        end_candidates: List[str] = []

        for q in quals:
            fn, op = q.field_name, q.operator
            # Handle supported operators for temporal push-down
            if fn == "start_time" and op in ("=", ">=", ">"):
                start_candidates.append(to_iso8601(q.value))
                continue
            if fn == "end_time" and op in ("=", "<=", "<"):
                end_candidates.append(to_iso8601(q.value))
                continue
            if op != "=":  # other operators not push-down (except temporal handled above)
                continue
            if fn == "device_id":
                device_id = q.value
            elif fn == "device_name":
                device_name = q.value
            elif fn == "recording_id":
                recording_id = q.value
            elif fn == "recording_key":
                recording_key = q.value
            elif fn == "project_id":
                params["projectId"] = q.value
            elif fn == "limit":
                try:
                    limit_ = int(q.value)
                except Exception:
                    pass
            # topic/version equality not push‑down; post‑filter

        # Apply start/end selection logic (choose max for start, min for end)
        if start_candidates:
            params["start"] = max(start_candidates)
        if end_candidates:
            params["end"] = min(end_candidates)

        # Preflight validation to avoid opaque API 400s
        if not recording_id and not recording_key:
            # Need device + start + end combination
            if not (device_id or device_name):
                raise RuntimeError(
                    "foxglove_topics FDW: provide either recording_id/recording_key OR (device_id/device_name plus start_time and end_time) for topics query"
                )
            if "start" not in params or "end" not in params:
                raise RuntimeError(
                    "foxglove_topics FDW: when querying by device you must also supply both start_time and end_time"
                )
        # Assign device/recording params after validation
        if device_id:
            params["deviceId"] = device_id
        if device_name:
            params["deviceName"] = device_name
        if recording_id:
            params["recordingId"] = recording_id
        if recording_key:
            params["recordingKey"] = recording_key

        if limit_:
            params["limit"] = limit_

        if sortkeys:
            sk = sortkeys[0]
            fld = sk.attname
            if fld in self.SUPPORTED_SORT_FIELDS:
                params["sortBy"] = fld
                params["sortOrder"] = "desc" if sk.is_reversed else "asc"

        debug = False  # set True via manual edit if needed; could be optionized later
        try:
            r = requests.get(
                f"{self.base_url}/data/topics",
                headers={"Authorization": f"Bearer {self.api_key}"},
                params=params,
                timeout=60,
            )
            if debug:
                log_to_postgres(f"foxglove_topics request params: {params}")
            r.raise_for_status()
        except requests.HTTPError as e:
            # Surface clearer diagnostics including body
            body = None
            try:
                body = r.text  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(
                f"foxglove_topics FDW upstream error {e.response.status_code if e.response else ''}: {body} (params={params})"
            )
        topics: list[dict] = r.json()

        if sortkeys and "sortBy" not in params:
            sk = sortkeys[0]

            def _sort_key(rec: Dict[str, Any], field: str = sk.attname) -> str:
                val = rec.get(field)
                return "" if val is None else str(val)

            topics.sort(key=_sort_key, reverse=sk.is_reversed)

        for t in topics:
            row = {
                "topic": t.get("topic"),
                "version": t.get("version"),
                "encoding": t.get("encoding"),
                "schema_name": t.get("schemaName"),
                "schema_encoding": t.get("schemaEncoding"),
                # populate pseudo columns with the request filter values (if present)
                "device_id": params.get("deviceId"),
                "device_name": params.get("deviceName"),
                "recording_id": params.get("recordingId"),
                "recording_key": params.get("recordingKey"),
                "start_time": params.get("start"),
                "end_time": params.get("end"),
                "project_id": params.get("projectId"),
            }
            if not self._row_matches_quals(row, quals):
                continue
            yield {c: row.get(c) for c in columns}

    # ---------- helpers --------------------------------------------------
    @staticmethod
    def _row_matches_quals(row: Dict[str, Any], quals: List) -> bool:
        for q in quals:
            if q.operator == "=" and q.field_name in row:
                if str(row[q.field_name]) != str(q.value):
                    return False
        return True
