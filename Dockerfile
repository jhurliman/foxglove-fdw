# syntax=docker/dockerfile:1

FROM postgres:16-bookworm

# 1. install build deps, git, python tooling
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  build-essential git ca-certificates \
  python3 python3-dev python3-pip python3-setuptools \
  postgresql-server-dev-all libpython3.11 && \
  rm -rf /var/lib/apt/lists/*

# 2. build & install Multicorn2 (C extension + Python wheel)
RUN git clone --depth 1 https://github.com/pgsql-io/multicorn2.git /tmp/multicorn2 && \
  cd /tmp/multicorn2 && \
  export PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config && \
  make && \
  make install && \
  pip install --no-cache-dir --break-system-packages "multicorn @ git+https://github.com/pgsql-io/multicorn2.git@v3.0" && \
  echo "=== Checking installed extension files ===" && \
  ls -la /usr/share/postgresql/16/extension/ | grep multicorn || echo "No multicorn files found" && \
  find /usr -name "*multicorn*" -type f 2>/dev/null | head -10 && \
  rm -rf /tmp/multicorn2

# 3. copy & install our FDW package
COPY pyproject.toml /tmp/foxglove-fdw/
COPY foxglove_fdw /tmp/foxglove-fdw/foxglove_fdw
RUN pip install --no-cache-dir --break-system-packages /tmp/foxglove-fdw && \
  rm -rf /tmp/foxglove-fdw

# 4. set up environment variables for Postgres
#    - these can be overridden at runtime
ENV POSTGRES_USER=postgres \
  POSTGRES_PASSWORD=postgres \
  POSTGRES_DB=postgres

# 5. place the SQL bootstrap script where the Postgres entrypoint will pick it up
COPY sql/pg_foxglove_api.sql /docker-entrypoint-initdb.d/
# 5.1 substitute the API key at build time
RUN --mount=type=secret,id=FOXGLOVE_API_KEY,env=FOXGLOVE_API_KEY \
  sed -i "s/\$FOXGLOVE_API_KEY/$FOXGLOVE_API_KEY/" /docker-entrypoint-initdb.d/pg_foxglove_api.sql

# 6. minimal clean‑up (keep Python runtime for multicorn)
RUN apt-get purge -y build-essential git && \
  apt-get clean

# 7. debug: verify Python shared library exists and test module import
RUN echo "=== Checking for libpython3.11.so.1.0 ===" && \
  find /usr -name "libpython3.11.so*" -type f 2>/dev/null && \
  ldd /usr/lib/postgresql/16/lib/multicorn.so | grep python || echo "No python library dependency found" && \
  echo "=== Testing Python module import ===" && \
  python3 -c "from foxglove_fdw.devices import FoxgloveDevicesFDW; print('Import successful')" || echo "Import failed"

EXPOSE 5432
