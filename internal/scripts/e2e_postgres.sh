#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_tables() {
  log "Creating test tables in source database..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (
    id serial PRIMARY KEY,
    data text,
    nullable_column text,
    toasted_column text,
    created_at timestamp DEFAULT current_timestamp
  );"
  run_sql "DROP TABLE IF EXISTS public.toast_test;"
  run_sql "CREATE TABLE public.toast_test (id serial PRIMARY KEY, large_jsonb jsonb, small_text text);"
  success "Test tables created in source database"
}

create_config_files() {
  log "Creating config files..."

  # Create replicator config
  cat >"/tmp/pg_flo_replicator.yml" <<EOF
# Replicator PostgreSQL connection settings
host: "${PG_HOST}"
port: ${PG_PORT}
dbname: "${PG_DB}"
user: "${PG_USER}"
password: "${PG_PASSWORD}"
schema: "public"
group: "group_postgres_sink"
tables:
  - users
  - toast_test
nats-url: "nats://localhost:4222"
EOF

  # Create worker config
  cat >"/tmp/pg_flo_worker.yml" <<EOF
# Worker settings
group: "group_postgres_sink"
nats-url: "nats://localhost:4222"
batch-size: 5000

# Source connection for schema sync
source-host: "${PG_HOST}"
source-port: ${PG_PORT}
source-dbname: "${PG_DB}"
source-user: "${PG_USER}"
source-password: "${PG_PASSWORD}"

# Target PostgreSQL connection settings
target-host: "${TARGET_PG_HOST}"
target-port: ${TARGET_PG_PORT}
target-dbname: "${TARGET_PG_DB}"
target-user: "${TARGET_PG_USER}"
target-password: "${TARGET_PG_PASSWORD}"
target-sync-schema: true
EOF

  success "Config files created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replicator..."
  if [ -f "$pg_flo_LOG" ]; then
    mv "$pg_flo_LOG" "${pg_flo_LOG}.bak"
    log "Backed up previous replicator log to ${pg_flo_LOG}.bak"
  fi

  $pg_flo_BIN replicator --config "/tmp/pg_flo_replicator.yml" >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo replicator started with PID: $pg_flo_PID"
  success "pg_flo replicator started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with PostgreSQL sink..."
  if [ -f "$pg_flo_WORKER_LOG" ]; then
    mv "$pg_flo_WORKER_LOG" "${pg_flo_WORKER_LOG}.bak"
    log "Backed up previous worker log to ${pg_flo_WORKER_LOG}.bak"
  fi

  $pg_flo_BIN worker postgres --config "/tmp/pg_flo_worker.yml" >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

simulate_changes() {
  log "Simulating changes..."
  local insert_count=1000

  for i in $(seq 1 "$insert_count"); do
    run_sql "INSERT INTO public.users (data, nullable_column, toasted_column) VALUES ('Data $i', 'Nullable $i', 'Toasted $i');"
  done

  # Insert specific rows for deletion
  run_sql "INSERT INTO public.users (id, data) VALUES (10001, 'To be deleted 1');"
  run_sql "INSERT INTO public.users (id, data) VALUES (10002, 'To be deleted 2');"
  run_sql "INSERT INTO public.users (id, data) VALUES (10003, 'To be deleted 3');"
  run_sql "INSERT INTO public.users (id, data) VALUES (10004, 'To be deleted 4');"
  run_sql "INSERT INTO public.users (id, data) VALUES (10005, 'To be deleted 5');"

  # Insert a row with potentially toasted data
  run_sql "INSERT INTO public.users (id, toasted_column) VALUES (10006, repeat('Large toasted data ', 1000));"

  # Update with various scenarios
  run_sql "UPDATE public.users SET data = 'Updated data' WHERE id = 1;"
  run_sql "UPDATE public.users SET nullable_column = NULL WHERE id = 2;"
  run_sql "UPDATE public.users SET data = 'Updated data', nullable_column = NULL WHERE id = 3;"
  run_sql "UPDATE public.users SET toasted_column = repeat('A', 10000) WHERE id = 4;"
  run_sql "UPDATE public.users SET data = 'Updated data' WHERE id = 5;"

  # Generate large JSONB data (approximately 1MB)
  log "Generating 1MB JSONB data..."
  local json_data='{"data":"'
  for i in {1..100000}; do
    json_data+="AAAAAAAAAA"
  done
  json_data+='"}'

  # Insert large JSONB data
  run_sql "INSERT INTO public.toast_test (large_jsonb, small_text) VALUES ('$json_data'::jsonb, 'Initial small text');"
  log "Inserted large JSONB data, waiting for replication..."

  # Update unrelated column
  run_sql "UPDATE public.toast_test SET small_text = 'Updated small text' WHERE id = 1;"
  log "Updated unrelated column, waiting for replication..."

  # Delete operations
  run_sql "DELETE FROM public.users WHERE id = 10001;"
  run_sql "DELETE FROM public.users WHERE id IN (10002, 10003);"
  run_sql "DELETE FROM public.users WHERE id >= 10004 AND id <= 10005;"
  run_sql "DELETE FROM public.users WHERE id = 10006;"

  success "Changes simulated"
}

verify_changes() {
  log "Verifying changes in target database..."

  local updated_data=$(run_sql_target "SELECT data FROM public.users WHERE id = 1;" | xargs)
  log "Updated data for id 1: '$updated_data' (expected 'Updated data')"

  local null_column=$(run_sql_target "SELECT coalesce(nullable_column, 'NULL') FROM public.users WHERE id = 2;" | xargs)
  log "Nullable column for id 2: '$null_column' (expected 'NULL')"

  local mixed_update=$(run_sql_target "SELECT data || ' | ' || coalesce(nullable_column, 'NULL') FROM public.users WHERE id = 3;" | xargs)
  log "Mixed update for id 3: '$mixed_update' (expected 'Updated data | NULL')"

  local toast_length=$(run_sql_target "SELECT length(toasted_column) FROM public.users WHERE id = 4;" | xargs)
  log "TOAST column length for id 4: '$toast_length' (expected '10000')"

  local unrelated_column=$(run_sql_target "SELECT nullable_column FROM public.users WHERE id = 5;" | xargs)
  log "Unrelated column for id 5: '$unrelated_column' (expected 'Nullable 5')"

  local jsonb_length=$(run_sql_target "SELECT octet_length(large_jsonb::text) FROM public.toast_test LIMIT 1;" | xargs)
  log "JSONB column length: '$jsonb_length' bytes (expected > 1000000)"

  local small_text=$(run_sql_target "SELECT small_text FROM public.toast_test LIMIT 1;" | xargs)
  log "small_text content: '$small_text' (expected 'Updated small text')"

  local deleted_single=$(run_sql_target "SELECT COUNT(*) FROM public.users WHERE id = 10001;" | xargs)
  log "Count of deleted user (id 10001): '$deleted_single' (expected '0')"

  local deleted_multiple=$(run_sql_target "SELECT COUNT(*) FROM public.users WHERE id IN (10002, 10003);" | xargs)
  log "Count of deleted users (ids 10002, 10003): '$deleted_multiple' (expected '0')"

  local deleted_range=$(run_sql_target "SELECT COUNT(*) FROM public.users WHERE id >= 10004 AND id <= 10005;" | xargs)
  log "Count of deleted users (ids 10004-10005): '$deleted_range' (expected '0')"

  local deleted_toasted=$(run_sql_target "SELECT COUNT(*) FROM public.users WHERE id = 10006;" | xargs)
  log "Count of deleted user with toasted data (id 10006): '$deleted_toasted' (expected '0')"

  log "Detailed verification:"

  if [ "$updated_data" != "Updated data" ]; then
    log "updated_data: '$updated_data' != 'Updated data'"
    error "Verification failed: updated_data mismatch"
    return 1
  fi

  if [ "$null_column" != "NULL" ]; then
    log "null_column: '$null_column' != 'NULL'"
    error "Verification failed: null_column mismatch"
    return 1
  fi

  if [ "$mixed_update" != "Updated data | NULL" ]; then
    log "mixed_update: '$mixed_update' != 'Updated data | NULL'"
    error "Verification failed: mixed_update mismatch"
    return 1
  fi

  if [ "$toast_length" != "10000" ]; then
    log "toast_length: '$toast_length' != '10000'"
    error "Verification failed: toast_length mismatch"
    return 1
  fi

  if [ "$unrelated_column" != "Nullable 5" ]; then
    log "unrelated_column: '$unrelated_column' != 'Nullable 5'"
    error "Verification failed: unrelated_column mismatch"
    return 1
  fi

  if [ -z "$jsonb_length" ] || [ "$jsonb_length" -le 1000000 ]; then
    log "jsonb_length: '$jsonb_length' <= 1000000"
    error "Verification failed: jsonb_length mismatch"
    return 1
  fi

  if [ "$small_text" != "Updated small text" ]; then
    log "small_text: '$small_text' != 'Updated small text'"
    error "Verification failed: small_text mismatch"
    return 1
  fi

  if [ "$deleted_single" != "0" ]; then
    log "deleted_single: '$deleted_single' != '0'"
    error "Verification failed: deleted_single mismatch"
    return 1
  fi

  if [ "$deleted_multiple" != "0" ]; then
    log "deleted_multiple: '$deleted_multiple' != '0'"
    error "Verification failed: deleted_multiple mismatch"
    return 1
  fi

  if [ "$deleted_range" != "0" ]; then
    log "deleted_range: '$deleted_range' != '0'"
    error "Verification failed: deleted_range mismatch"
    return 1
  fi

  if [ "$deleted_toasted" != "0" ]; then
    log "deleted_toasted: '$deleted_toasted' != '0'"
    error "Verification failed: deleted_toasted mismatch"
    return 1
  fi

  success "All changes verified successfully in target database"
  return 0
}

test_pg_flo_postgres_sink() {
  setup_postgres
  create_tables
  create_config_files
  start_pg_flo_replication
  sleep 2
  start_pg_flo_worker
  simulate_changes

  log "Waiting for pg_flo to process changes..."

  stop_pg_flo_gracefully
  verify_changes || return 1
}

# Run the test
log "Starting pg_flo CDC test with PostgreSQL sink..."
if test_pg_flo_postgres_sink; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
