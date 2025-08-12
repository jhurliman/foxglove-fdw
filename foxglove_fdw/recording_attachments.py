"""
Foxglove Recording Attachments Foreign Data Wrapper
Implements a read‑only FDW for `GET /v1/recording-attachments`.

API docs: https://docs.foxglove.dev/api#tag/Recording-Attachments

Columns exposed (snake_case):
  id                text
  recording_id      text
  site_id           text
  name              text
  media_type        text
  log_time          timestamptz  (API: logTime)
  create_time       timestamptz  (API: createTime)
  crc               bigint       (fits into 64 bits; API returns number)
  size_bytes        bigint       (API: size)
  fingerprint       text
  lake_path         text

Filter push-down (equality): recording_id, site_id, device_id, device_name,
                              project_id
Order by push-down: log_time (maps to logTime)

LIMIT and OFFSET are partially supported: LIMIT is forwarded; OFFSET is ignored
because the endpoint supports `offset` but Multicorn does not expose it in a
portable way. (Could be added later if needed.)
"""

from __future__ import annotations
from multicorn import ForeignDataWrapper, SortKey
from multicorn.utils import log_to_postgres, WARNING
from typing import Dict, Any, List
import requests


class FoxgloveRecordingAttachmentsFDW(ForeignDataWrapper):
    SUPPORTED_SORT_FIELDS = {"log_time", "logTime"}  # we expose log_time

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
        # default API limit is 2000
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
        params: Dict[str, Any] = {}
        limit_ = None

        # filter push‑down
        for q in quals:
            if q.operator != "=":
                continue
            fn = q.field_name
            if fn == "recording_id":
                params["recordingId"] = q.value
            elif fn == "site_id":
                params["siteId"] = q.value
            elif fn == "device_id":
                params["deviceId"] = q.value
            elif fn == "device_name":
                params["deviceName"] = q.value
            elif fn == "project_id":
                params["projectId"] = q.value
            elif fn == "limit":
                limit_ = int(q.value)

        if limit_:
            params["limit"] = limit_

        # ORDER BY push‑down (only first key, log_time)
        if sortkeys:
            sk = sortkeys[0]
            fld = sk.attname
            if fld in self.SUPPORTED_SORT_FIELDS:
                api_field = "logTime" if fld in ("log_time", "logTime") else fld
                params["sortBy"] = api_field
                params["sortOrder"] = "desc" if sk.is_reversed else "asc"

        # HTTP request
        try:
            r = requests.get(
                f"{self.base_url}/recording-attachments",
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
                f"foxglove_recording_attachments FDW upstream error {e.response.status_code if e.response else ''}: {body} (params={params})"
            )
        attachments: list[dict] = r.json()

        # local sort fallback if not pushed
        if sortkeys and "sortBy" not in params:
            sk = sortkeys[0]
            key = "logTime" if sk.attname in ("log_time", "logTime") else sk.attname
            # Use empty string fallback to satisfy ordering even if value missing
            attachments.sort(key=lambda d, k=key: str(d.get(k) or ""), reverse=sk.is_reversed)  # type: ignore

        for a in attachments:
            row = {
                "id": a.get("id"),
                "recording_id": a.get("recordingId"),
                "site_id": a.get("siteId"),
                "name": a.get("name"),
                "media_type": a.get("mediaType"),
                "log_time": a.get("logTime"),
                "create_time": a.get("createTime"),
                "crc": a.get("crc"),
                "size_bytes": a.get("size"),
                "fingerprint": a.get("fingerprint"),
                "lake_path": a.get("lakePath"),
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
