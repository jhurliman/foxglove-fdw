# foxglove-fdw

PostgreSQL Foreign Data Wrapper for the [Foxglove API](https://docs.foxglove.dev/api)

# Usage

### Requirements:

- Docker
- `FOXGLOVE_API_KEY` environment variable set

Run `./start.sh` to build and start a local Docker container running Postgres with the Foxglove Foreign Data Wrapper installed. Connect to the database using a Postgres client (e.g., `psql` or a GUI client) with the following credentials:

- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres`
- Database: `postgres`

### Querying

You can query the Foxglove API using SQL commands. Here are some examples:

- List all registered devices:

```sql
SELECT * FROM devices;
```

- List all topics from a particular recording:

```sql
SELECT * FROM topics WHERE recording_id = 'rec_<your_recording_id>';
```

- Total bytes recorded per device in the last 10 minutes:

```sql
SELECT device_name, SUM(size_bytes) as total_bytes
FROM recordings
WHERE end_time > now() - interval '10 minutes'
GROUP BY device_name
ORDER BY total_bytes DESC;
```

- Total runtime of a device in a given time period:

```sql
WITH filtered AS
  (SELECT tstzrange(start_time, end_time, '[)') AS r
   FROM coverage
   WHERE tolerance = 60
     AND device_name = '<your_device_name>'
     AND start_time > '2025-01-01 06:00-08'::timestamptz
     AND end_time < now()),
agg AS
  (SELECT range_agg(r) AS mr FROM filtered)
SELECT SUM(EXTRACT(EPOCH FROM (upper(x) - lower(x)))) / 60.0 AS coverage_minutes
FROM agg, LATERAL unnest(mr) AS x;
```

- Parse all messages from a device in the last 10 minutes:

```sql
SELECT * FROM messages
WHERE device_name = '<your_device_name>'
  AND timestamp > now() - interval '10 minutes';
```

- Retrieve the most recently reported serial number for all robots that have been seen in the last week from `/robot_info.serial_number`:

```sql
WITH latest AS (
  SELECT
    DISTINCT ON (device_name) device_name,
    id AS recording_id
  FROM recordings
  WHERE
    import_status = 'complete'
    AND topic = '/robot_info'
    AND start_time > now() - interval '1 week'
  ORDER BY device_name, start_time DESC
)

SELECT
  l.device_name,
  m.message ->> 'serial_number' AS serial_number
FROM
  latest l
  CROSS JOIN LATERAL (
    SELECT message
    FROM messages
    WHERE
      recording_id = l.recording_id
      AND topic = '/robot_info'
    ORDER BY timestamp DESC
    LIMIT 1
  ) m
ORDER BY l.device_name;
```

# Limitations

- The messages table only supports `protobuf` and `json` encodings. Other encodings will return NULL for the message column.
- The Foxglove API defaults to returning a maximum of 2000 rows per request (with the exception of the coverage endpoint). Automatic pagination is not currently supported, so queries that would return more than 2000 rows will be silently truncated. This limitation does not apply to the messages table which retrieves an MCAP payload and parses individual messages.
- `FOXGLOVE_API_KEY` is currently baked into the Docker image and must be provided at build time. A future improvement would be to move this to a runtime configuration so the same container could be used with different API keys and there is no secret in the image.

# License

MIT License
