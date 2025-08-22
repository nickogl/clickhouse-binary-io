#!/usr/bin/env sh

CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-"25.6-alpine"}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-"clickhouse/clickhouse-server:$CLICKHOUSE_VERSION"}
docker pull "$CLICKHOUSE_IMAGE"
docker run \
  --rm \
  --name clickhouse-binary-io-test \
  --ulimit nofile=262144 \
  -p 8123:8123 \
  -e CLICKHOUSE_DB=test \
  -e CLICKHOUSE_USER=test \
  -e CLICKHOUSE_PASSWORD=test \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  "$CLICKHOUSE_IMAGE"
