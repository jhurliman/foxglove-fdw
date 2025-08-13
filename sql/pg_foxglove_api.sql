-- 1. enable multicorn in the database
CREATE EXTENSION IF NOT EXISTS multicorn;

-- 2. foreign server – keep sensitive values OUT of DDL when possible
-- Devices FDW server
CREATE SERVER foxglove_devices_srv
    FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'foxglove_fdw.devices.FoxgloveDevicesFDW');

-- Recordings FDW server (separate wrapper class)
CREATE SERVER foxglove_recordings_srv
    FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'foxglove_fdw.recordings.FoxgloveRecordingsFDW');

-- Recording Attachments FDW server
CREATE SERVER foxglove_recording_attachments_srv
    FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'foxglove_fdw.recording_attachments.FoxgloveRecordingAttachmentsFDW');

-- Events FDW server
CREATE SERVER foxglove_events_srv
    FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'foxglove_fdw.events.FoxgloveEventsFDW');

-- Topics FDW server
CREATE SERVER foxglove_topics_srv
    FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'foxglove_fdw.topics.FoxgloveTopicsFDW');

-- 3. per‑user credentials
CREATE USER MAPPING FOR CURRENT_USER
    SERVER foxglove_devices_srv
    OPTIONS (api_key '$FOXGLOVE_API_KEY');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER foxglove_recordings_srv
    OPTIONS (api_key '$FOXGLOVE_API_KEY');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER foxglove_recording_attachments_srv
    OPTIONS (api_key '$FOXGLOVE_API_KEY');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER foxglove_events_srv
    OPTIONS (api_key '$FOXGLOVE_API_KEY');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER foxglove_topics_srv
    OPTIONS (api_key '$FOXGLOVE_API_KEY');

-- 4. foreign tables

CREATE FOREIGN TABLE IF NOT EXISTS devices (
    id                       text,
    name                     text,
    org_id                   text,
    project_id               text,
    created_at               date,
    updated_at               timestamptz,
    retain_recordings_seconds integer,
    properties               jsonb
) SERVER foxglove_devices_srv;

CREATE FOREIGN TABLE IF NOT EXISTS recordings (
    id               text,
    project_id       text,
    path             text,
    size_bytes       bigint,
    created_at       timestamptz,
    imported_at      timestamptz,
    start_time       timestamptz,
    end_time         timestamptz,
    duration         double precision,
    import_status    text,
    site_id          text,
    site_name        text,
    edge_site_id     text,
    edge_site_name   text,
    device_id        text,
    device_name      text,
    key              text,
    metadata         jsonb
) SERVER foxglove_recordings_srv;

CREATE FOREIGN TABLE IF NOT EXISTS recording_attachments (
    id             text,
    recording_id   text,
    site_id        text,
    name           text,
    media_type     text,
    log_time       timestamptz,
    create_time    timestamptz,
    crc            bigint,
    size_bytes     bigint,
    fingerprint    text,
    lake_path      text
) SERVER foxglove_recording_attachments_srv;

CREATE FOREIGN TABLE IF NOT EXISTS events (
    id           text,
    device_id    text,
    device_name  text,
    start_time   timestamptz,
    end_time     timestamptz,
    metadata     jsonb,
    created_at   timestamptz,
    updated_at   timestamptz,
    project_id   text
) SERVER foxglove_events_srv;

CREATE FOREIGN TABLE IF NOT EXISTS topics (
    topic            text,
    version          text,
    encoding         text,
    schema_name      text,
    schema_encoding  text,
    -- pseudo filter columns (not returned by API, used for predicates)
    device_id        text,
    device_name      text,
    recording_id     text,
    recording_key    text,
    start_time       timestamptz,
    end_time         timestamptz,
    project_id       text
) SERVER foxglove_topics_srv;

CREATE SERVER foxglove_coverage_srv
    FOREIGN DATA WRAPPER multicorn
    OPTIONS (wrapper 'foxglove_fdw.coverage.FoxgloveCoverageFDW');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER foxglove_coverage_srv
    OPTIONS (api_key '$FOXGLOVE_API_KEY');

CREATE FOREIGN TABLE IF NOT EXISTS coverage (
    device_id      text,
    device_name    text,
    start_time     timestamptz,
    end_time       timestamptz,
    status         text,
    import_status  text,
    tolerance      integer
) SERVER foxglove_coverage_srv;
