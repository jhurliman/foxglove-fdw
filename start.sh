#!/usr/bin/env bash

# Build the Docker image for Postgres with the multicorn extension and Foxglove
# Foreign Data Wrapper (FDW) loaded.

set -euo pipefail

# Ensure FOXGLOVE_API_KEY is set
if [ -z "${FOXGLOVE_API_KEY:-}" ]; then
  echo "Error: FOXGLOVE_API_KEY environment variable is not set"
  exit 1
fi

docker buildx build \
  --secret id=FOXGLOVE_API_KEY \
  -t foxglove-fdw \
  .

docker stop foxglove-postgres || true
docker rm foxglove-postgres || true

docker run -d \
  --name foxglove-postgres \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}" \
  foxglove-fdw
