"""
Shared utilities for foxglove_fdw.

Currently exposes:
  - to_iso8601(val): normalize various datetime inputs to RFC3339 UTC (no micros), e.g. 2025-08-09T20:20:12Z
"""

from __future__ import annotations
from typing import Any, Optional
import datetime as dt


def to_iso8601(val: Any) -> str:
    """Return RFC3339 timestamp in UTC without microseconds, e.g. 2025-08-09T20:20:12Z.

    Accepts:
      - datetime with or without tzinfo
      - typical timestamptz strings like 'YYYY-MM-DD HH:MM:SS.mmmmmm-07'
      - ISO 8601 strings with 'T' and 'Z'

    Raises ValueError if the value cannot be parsed into a datetime.
    """
    if isinstance(val, dt.datetime):
        d = val if val.tzinfo else val.replace(tzinfo=dt.timezone.utc)
    else:
        s = str(val).strip()
        # Replace space with 'T'
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        # If no TZ specified, assume UTC
        if not any(z in s for z in ("Z", "+", "-")):
            s += "Z"
        # Normalize trailing Z
        if s.endswith("Z"):
            try:
                d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception as e:
                raise ValueError(f"to_iso8601: could not parse timestamp {val!r}") from e
        else:
            try:
                d = dt.datetime.fromisoformat(s)
            except Exception as e:
                raise ValueError(f"to_iso8601: could not parse timestamp {val!r}") from e
    d = d.astimezone(dt.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(val: Any) -> Optional[dt.datetime]:
    """Parse various datetime inputs into a timezone-aware datetime.

    - Accepts datetime (naive treated as UTC), or strings in ISO 8601-ish forms.
    - Returns an aware datetime (UTC if originally naive), or None if unparseable.
    """
    if val is None:
        return None
    if isinstance(val, dt.datetime):
        return val if val.tzinfo else val.replace(tzinfo=dt.timezone.utc)
    s = str(val).strip()
    # Normalize trailing Z to +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        d = dt.datetime.fromisoformat(s)
    except Exception:
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
