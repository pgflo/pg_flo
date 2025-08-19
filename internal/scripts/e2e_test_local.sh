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

log "Running e2e tests..."

# Default to stream_only test, but allow override with E2E_TEST env var
TEST_SCRIPT=${E2E_TEST:-e2e_stream_only.sh}
log "Running test: ${TEST_SCRIPT}"

if CI=false ./internal/scripts/"${TEST_SCRIPT}"; then
  success "e2e tests completed successfully"
else
  error "e2e tests failed"
  exit 1
fi
