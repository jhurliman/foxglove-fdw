"""
Foxglove Devices Foreign Data Wrapper
Implements a read-only FDW for `GET /v1/devices`
"""

from multicorn import ForeignDataWrapper, SortKey
from multicorn.utils import log_to_postgres, WARNING
from typing import List, Dict, Any
import requests
import json


class FoxgloveDevicesFDW(ForeignDataWrapper):
    SUPPORTED_SORT_FIELDS = {"id", "name"}  # plus any properties.* key

    """
    Required server options
    -----------------------
    api_key   - Foxglove API key with `devices.list` capability.
                (For security, create a USER MAPPING instead of hard-coding
                the key in server options.)

    Optional server options
    -----------------------
    base_url  - defaults to https://api.foxglove.dev/v1
    """

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

    # ---------- Planner helpers ------------------------------------------------
    def get_rel_size(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, quals, columns
    ) -> tuple[int, int]:
        """Return (row_count, avg_row_width)."""
        return 2000, len(columns) * 64  # default row limit in Foxglove API is 2000

    # ---------- Executor -------------------------------------------------------
    # foxglove_fdw/devices.py  (only the changed / new bits shown)

    ...

    # -----------------------------------------------------------------
    # tell the planner which ORDER BY clauses we can enforce remotely
    # -----------------------------------------------------------------
    def can_sort(self, sortkeys):
        """
        Return the longest prefix of sortkeys we can handle.
        PostgreSQL will then omit its own sort node for those keys.
        """
        handled = []
        for sk in sortkeys:
            field = sk.attname
            if field in self.SUPPORTED_SORT_FIELDS or field.startswith("properties."):
                handled.append(sk)
            else:
                break  # stop at first unsupported key
        return handled

    # -----------------------------------------------------------------
    # executor
    # -----------------------------------------------------------------
    def execute(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, quals: list, columns: list, sortkeys: list[SortKey] | None = None
    ):
        params, limit_ = {}, None

        for q in quals:  # simple equality push‑down
            if q.operator == "=":
                if q.field_name == "project_id":
                    params["projectId"] = q.value
                elif q.field_name == "name":
                    params["query"] = q.value
                elif q.field_name == "id":
                    # no native filter; we will post‑filter below
                    pass
            elif q.field_name == "limit" and q.operator in ("<", "<="):
                limit_ = q.value

        if limit_:
            params["limit"] = limit_

        # ---   push‑down ORDER BY if possible   -------------------------
        if sortkeys:
            primary = sortkeys[0]  # Multicorn guarantees φ‑prefix order
            field = primary.attname
            if field in self.SUPPORTED_SORT_FIELDS or field.startswith("properties."):
                params["sortBy"] = field
                params["sortOrder"] = "desc" if primary.is_reversed else "asc"

        # ---   REST call   ----------------------------------------------
        try:
            r = requests.get(
                f"{self.base_url}/devices",
                headers={"Authorization": f"Bearer {self.api_key}"},
                params=params,
                timeout=30,
            )
            r.raise_for_status()
        except requests.HTTPError as e:
            body = None
            try:
                body = r.text  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(
                f"foxglove_devices FDW upstream error {e.response.status_code if e.response else ''}: {body} (params={params})"
            )
        devices = r.json()

        # ---   optional local sort fall‑back   --------------------------
        if sortkeys and "sortBy" not in params:
            # PostgreSQL will sort anyway, but doing it here keeps deterministic
            sk = sortkeys[0]
            devices.sort(key=lambda d: d.get(sk.attname), reverse=sk.is_reversed)

        # ---   yield rows   ---------------------------------------------
        for d in devices:
            row = {
                "id": d.get("id"),
                "name": d.get("name"),
                "org_id": d.get("orgId"),
                "project_id": d.get("projectId"),
                "created_at": d.get("createdAt"),
                "updated_at": d.get("updatedAt"),
                "retain_recordings_seconds": d.get("retainRecordingsSeconds"),
                "properties": json.dumps(d.get("properties", {})),
            }
            if not self._row_matches_quals(row, quals):
                continue
            yield {c: row.get(c) for c in columns}

    # ---------- helpers --------------------------------------------------------
    @staticmethod
    def _row_matches_quals(row: Dict[str, Any], quals: List) -> bool:
        for q in quals:
            if q.operator == "=" and q.field_name in row:
                if str(row[q.field_name]) != str(q.value):
                    return False
        return True
