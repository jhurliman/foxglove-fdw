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
SELECT * FROM foxglove_devices_srv;
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

# Limitations

The Foxglove API defaults to returning a maximum of 2000 rows per request. Automatic pagination is not currently supported, so queries that would return more than 2000 rows will be silently truncated.

# License

MIT License
