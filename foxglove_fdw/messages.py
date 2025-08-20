"""Foxglove Messages Foreign Data Wrapper
Read-only FDW for `POST /v1/data/stream` (MCAP outputFormat) which streams MCAP
data and yields one row per MCAP message with a decoded payload when supported.

Supported schema encodings:
    - protobuf*  (decoded to JSON via mcap_protobuf)
    - json       (UTF-8 JSON blob parsed to JSON)

Unsupported encodings yield NULL for the `message` column (instead of the
previous fallback object containing `_raw_b64`).

API docs: https://docs.foxglove.dev/api#tag/Stream-data

Exposed columns:
    device_id      text            (pushed as deviceId if provided)
    device_name    text            (pushed as deviceName if provided)
    recording_id   text            (pushed as recordingId)
    recording_key  text            (pushed as recordingKey)
    timestamp      timestamptz     (derived from MCAP message.log_time)
    topic          text            (MCAP channel.topic; filterable equality)
    schema_name    text            (MCAP schema.name when present)
    channel_id     int             (MCAP channel.id)
    schema_id      int             (MCAP channel.schema_id)
    sequence_id    int             (MCAP message.sequence or 0 if absent)
    encoding       text            (message encoding, ex: 'protobuf', 'json')
    message        jsonb           (decoded protobuf object OR fallback JSON with _raw_b64/_encoding)

Push-down filters:
    Equality: device_id, device_name, recording_id, recording_key, topic, limit
    Timestamp range (column: timestamp):
            - `timestamp > / >= value`   => body.start (choose latest lower bound)
            - `timestamp < / <= value`   => body.end   (choose earliest upper bound)
            - `timestamp = value`        => start = end = value
        If only one bound is supplied AND no explicit recording_id/recording_key is
        given, the other bound is synthesized (start defaults to 1970-01-01T00:00:00Z,
        end defaults to now in UTC) in order to satisfy API range requirements.

Source selection rules:
    You MUST provide either:
        (A) recording_id or recording_key
                - Streams just that recording (no need for explicit timestamp bounds), OR
        (B) device_id or device_name PLUS at least one timestamp qualifier
                - A complete [start,end] range is ensured as described above.

Decoding:
    When schema.encoding starts with 'protobuf', each message is decoded via
    mcap_protobuf Decoder into a Python object and JSON-serialized. On decode
    failure a sentinel object is returned: {"_error": str}.
    When schema.encoding == 'json', the bytes are parsed as UTF-8 JSON.
    Any unsupported encoding causes `message` to be NULL.

Limit handling:
    A pushed equality qual on a pseudo column `limit` (e.g. WHERE limit = 100)
    caps emitted rows client-side after streaming (the API endpoint itself does
    not expose a message count limit for MCAP streams).

Ordering:
    No ORDER BY push-down is currently implemented; rows follow MCAP file order.

Notes / Caveats:
    - This implementation buffers the entire MCAP stream in memory before iterating
        (simple but not optimal for very large recordings). Future improvement could
        parse incrementally.
"""

from __future__ import annotations
from .utils import to_iso8601
from google.protobuf.json_format import MessageToDict
from mcap_protobuf.decoder import Decoder as ProtobufDecoder
from mcap.reader import make_reader
from mcap.records import Channel, Schema, Message
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, WARNING
from typing import Dict, Any, List, Optional
import requests, json, datetime as dt, io


class FoxgloveMessagesFDW(ForeignDataWrapper):
    def __init__(self, options: Dict[str, str], columns: Dict[str, Any]) -> None:
        super().__init__(options, columns)
        self.columns = columns
        self.base_url = options.get("base_url", "https://api.foxglove.dev/v1")
        self.api_key = options.get("api_key")
        if not self.api_key:
            log_to_postgres(
                "foxglove_fdw: `api_key` option (or USER MAPPING) is required", level=WARNING
            )

    # Conservative size estimates (messages unknown); planner just needs something.
    def get_rel_size(self, quals, columns):  # type: ignore[override]
        return 10000, 256

    def execute(self, quals: List, columns: List, sortkeys=None):  # type: ignore[override]
        body: Dict[str, Any] = {"outputFormat": "mcap"}

        device_id = device_name = recording_id = recording_key = None
        start_candidate: Optional[str] = None
        end_candidate: Optional[str] = None
        topic_filters: List[str] = []
        limit_messages: Optional[int] = None

        for q in quals:
            fn, op = q.field_name, getattr(q, "operator", "=")
            if fn == "timestamp":
                if op in (">", ">="):
                    iso = to_iso8601(q.value)
                    if start_candidate is None or iso > start_candidate:
                        start_candidate = iso
                elif op in ("<", "<="):
                    iso = to_iso8601(q.value)
                    if end_candidate is None or iso < end_candidate:
                        end_candidate = iso
                elif op == "=":
                    iso = to_iso8601(q.value)
                    start_candidate = iso
                    end_candidate = iso
                continue
            if op != "=":
                continue
            if fn == "device_id":
                device_id = q.value
            elif fn == "device_name":
                device_name = q.value
            elif fn == "recording_id":
                recording_id = q.value
            elif fn == "recording_key":
                recording_key = q.value
            elif fn == "topic":
                topic_filters.append(q.value)
            elif fn == "limit":
                try:
                    limit_messages = int(q.value)
                except Exception:
                    pass

        if start_candidate:
            body["start"] = start_candidate
        if end_candidate:
            body["end"] = end_candidate

        if not recording_id and not recording_key:
            if not (device_id or device_name):
                raise RuntimeError(
                    "foxglove_messages FDW: provide recording_id/recording_key OR (device_id/device_name plus timestamp range)"
                )
            if "start" in body and "end" not in body:
                body["end"] = to_iso8601(dt.datetime.now(dt.timezone.utc))
            if "end" in body and "start" not in body:
                body["start"] = "1970-01-01T00:00:00Z"

        if device_id:
            body["deviceId"] = device_id
        if device_name:
            body["deviceName"] = device_name
        if recording_id:
            body["recordingId"] = recording_id
        if recording_key:
            body["recordingKey"] = recording_key
        if topic_filters:
            body["topics"] = topic_filters

        link = self._obtain_stream_link(body)
        try:
            r = requests.get(link, timeout=300)
            r.raise_for_status()
            buf = io.BytesIO(r.content)
        except requests.HTTPError as e:
            raise RuntimeError(
                f"foxglove_messages FDW download error {e.response.status_code if e.response else ''}"
            )

        emitted = 0  # count rows for enforcing limit
        decoder = ProtobufDecoder()
        reader = make_reader(buf)
        for schema, channel, message in reader.iter_messages():  # type: ignore
            if not isinstance(channel, Channel) or not isinstance(message, Message):
                continue
            if topic_filters and channel.topic not in topic_filters:
                continue
            msg_obj = self._decode(schema, channel, message, decoder)
            emitted += 1
            encoding = schema.encoding if isinstance(schema, Schema) else None
            row = {
                "device_id": body.get("deviceId"),
                "device_name": body.get("deviceName"),
                "recording_id": body.get("recordingId"),
                "recording_key": body.get("recordingKey"),
                "timestamp": dt.datetime.fromtimestamp(message.log_time / 1e9, tz=dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "topic": channel.topic,
                "schema_name": schema.name if isinstance(schema, Schema) else None,
                "channel_id": channel.id,
                "schema_id": channel.schema_id,
                "sequence_id": getattr(message, "sequence", 0),
                "encoding": encoding,
                # msg_obj already a Python JSON-able object or None
                "message": json.dumps(msg_obj) if msg_obj is not None else None,
            }
            yield {c: row.get(c) for c in columns}
            if limit_messages and emitted >= limit_messages:
                break

    # ----- helpers -----------------------------------------------------------
    def _obtain_stream_link(self, body: Dict[str, Any]) -> str:
        try:
            r = requests.post(
                f"{self.base_url}/data/stream",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            link = data.get("link")
            if not link:
                raise RuntimeError("foxglove_messages FDW: stream response missing link")
            return link
        except requests.HTTPError as e:
            body_txt = None
            try:
                body_txt = r.text  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(
                f"foxglove_messages FDW upstream error {e.response.status_code if e.response else ''}: {body_txt} (body={body})"
            )

    # Protobuf decoding helper
    @staticmethod
    def _decode(schema: Schema | None, channel: Channel, message: Message, decoder: ProtobufDecoder):  # type: ignore[name-defined]
        if schema and schema.encoding == "protobuf":
            try:
                decoded = decoder.decode(schema, message)
                as_dict = MessageToDict(
                    decoded,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                return FoxgloveMessagesFDW._sanitize_json(as_dict)
            except Exception as e:  # pragma: no cover
                return {"_error": f"protobuf_decode_failed: {e}"}
        if schema and schema.encoding == "json":
            try:
                parsed = json.loads(message.data.decode("utf-8"))
                return FoxgloveMessagesFDW._sanitize_json(parsed)
            except Exception as e:  # pragma: no cover
                return {"_error": f"json_decode_failed: {e}"}
        # Unsupported encoding: return None to map to SQL NULL
        return None

    @staticmethod
    def _sanitize_json(value: Any) -> Any:
        """Recursively remove / replace characters Postgres JSONB cannot accept.

        Postgres rejects the Unicode code point U+0000 (NUL) even when escaped
        (\u0000) inside JSON input. We strip those to prevent 22P05 errors.
        """
        if isinstance(value, str):
            # If the string contains the NUL codepoint, remove it; otherwise return unchanged.
            return value.replace("\x00", "") if "\x00" in value else value
        if isinstance(value, list):
            return [FoxgloveMessagesFDW._sanitize_json(v) for v in value]
        if isinstance(value, dict):
            return {k: FoxgloveMessagesFDW._sanitize_json(v) for k, v in value.items()}
        return value
