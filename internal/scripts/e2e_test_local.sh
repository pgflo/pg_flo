#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

setup_docker() {
  pkill -9 "pg_flo" || true
  rm -Rf /tmp/pg*
  log "Setting up Docker environment..."
  docker compose -f internal/docker-compose.yml down -v
  docker compose -f internal/docker-compose.yml up -d
  success "Docker environment is set up"
}

cleanup_data() {
  log "Cleaning up data..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "DROP SCHEMA IF EXISTS internal_pg_flo CASCADE;"
  rm -rf /tmp/pg_flo-output
  rm -f /tmp/pg_flo.log
  success "Data cleanup complete"
}

cleanup() {
  log "Cleaning up..."
  docker compose down -v
  success "Cleanup complete"
}

trap cleanup EXIT

make build

setup_docker

log "Running e2e snowflake tests..."
if CI=false ./internal/scripts/e2e_copy_and_stream.sh; then
  success "e2e snowflake tests completed successfully"
else
  error "Original e2e tests failed"
  exit 1
fi
