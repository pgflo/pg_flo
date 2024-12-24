#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_users() {
  log "Creating test table..."
  run_sql "DROP TABLE IF EXISTS public.users;"
  run_sql "CREATE TABLE public.users (
    id serial PRIMARY KEY,
    int_col integer,
    float_col float,
    text_col text,
    bool_col boolean,
    date_col date,
    timestamp_col timestamp with time zone,
    json_col jsonb,
    array_col integer[],
    bytea_col bytea
  );"
  success "Test table created"
}

populate_initial_data() {
  log "Populating initial data..."
  run_sql "INSERT INTO public.users (
    int_col, float_col, text_col, bool_col, date_col, timestamp_col, json_col, array_col, bytea_col
  ) SELECT
    generate_series(1, 500000),
    random() * 100,
    'Initial data ' || generate_series(1, 500000),
    (random() > 0.5),
    current_date + (random() * 365)::integer * interval '1 day',
    current_timestamp + (random() * 365 * 24 * 60 * 60)::integer * interval '1 second',
    json_build_object('key', 'value' || generate_series(1, 500000), 'number', generate_series(1, 500000)),
    ARRAY[generate_series(1, 3)],
    decode(lpad(to_hex(generate_series(1, 4)), 8, '0'), 'hex')
  ;"

  log "Inserting large JSON data..."
  local large_json='{"data":['
  for i in {1..10000}; do
    if [ "$i" -ne 1 ]; then
      large_json+=','
    fi
    large_json+='{"id":'$i',"name":"Item '$i'","description":"This is a long description for item '$i'. It contains a lot of text to make the JSON larger.","attributes":{"color":"red","size":"large","weight":10.5,"tags":["tag1","tag2","tag3"]}}'
  done
  large_json+=']}'

  run_sql "INSERT INTO public.users (int_col, json_col) VALUES (1000001, '$large_json'::jsonb);"

  run_sql "ANALYZE public.users;"
  success "Initial data populated"
}

start_pg_flo_copy_only() {
  log "Starting pg_flo in copy-only mode..."
  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "test_group" \
    --tables "users" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    --copy \
    --max-copy-workers-per-table 10 \
    >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo started with PID: $pg_flo_PID"
  success "pg_flo copy-only started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with PostgreSQL sink..."
  $pg_flo_BIN worker postgres \
    --group "test_group" \
    --nats-url "$NATS_URL" \
    --source-host "$PG_HOST" \
    --source-port "$PG_PORT" \
    --source-dbname "$PG_DB" \
    --source-user "$PG_USER" \
    --source-password "$PG_PASSWORD" \
    --target-host "$TARGET_PG_HOST" \
    --target-port "$TARGET_PG_PORT" \
    --target-dbname "$TARGET_PG_DB" \
    --target-user "$TARGET_PG_USER" \
    --target-password "$TARGET_PG_PASSWORD" \
    --batch-size 5000 \
    --target-sync-schema \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

compare_row_counts() {
  log "Comparing row counts..."
  SOURCE_COUNT=$(run_sql "SELECT COUNT(*) FROM public.users")
  TARGET_COUNT=$(run_sql_target "SELECT COUNT(*) FROM public.users")

  log "Source database row count: $SOURCE_COUNT"
  log "Target database row count: $TARGET_COUNT"

  EXPECTED_COUNT=500001 # 500,000 regular rows + 1 large JSON row

  if [ "$SOURCE_COUNT" -eq "$TARGET_COUNT" ] && [ "$SOURCE_COUNT" -eq "$EXPECTED_COUNT" ]; then
    success "Row counts match and total is correct ($EXPECTED_COUNT)"
    return 0
  else
    error "Row counts do not match or total is incorrect. Expected $EXPECTED_COUNT, Source: $SOURCE_COUNT, Target: $TARGET_COUNT"
    return 1
  fi
}

verify_large_json() {
  log "Verifying large JSON data..."
  local source_json_length=$(run_sql "
    SELECT jsonb_array_length(json_col->'data')
    FROM public.users
    WHERE int_col = 1000001
  ")
  local target_json_length=$(run_sql_target "
    SELECT jsonb_array_length(json_col->'data')
    FROM public.users
    WHERE int_col = 1000001
  ")

  log "Source JSON length: $source_json_length"
  log "Target JSON length: $target_json_length"

  if [ -n "$source_json_length" ] && [ -n "$target_json_length" ] &&
    [ "$source_json_length" -eq "$target_json_length" ] &&
    [ "$source_json_length" -eq 10000 ]; then
    success "Large JSON data verified successfully"
    return 0
  else
    error "Large JSON data verification failed. Expected length 10000, got Source: $source_json_length, Target: $target_json_length"
    return 1
  fi
}

verify_data_integrity() {
  log "Verifying data integrity..."

  generate_table_hash() {
    local db=$1
    local csv_file="/tmp/pg_flo_${db}_dump.csv"

    if [ "$db" = "source" ]; then
      PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -c "\COPY (SELECT * FROM public.users ORDER BY id) TO '$csv_file' WITH CSV"
    else
      PGPASSWORD=$TARGET_PG_PASSWORD psql -h "$TARGET_PG_HOST" -p "$TARGET_PG_PORT" -U "$TARGET_PG_USER" -d "$TARGET_PG_DB" \
        -c "\COPY (SELECT * FROM public.users ORDER BY id) TO '$csv_file' WITH CSV"
    fi

    if command -v md5 >/dev/null; then
      md5 -q "$csv_file"
    elif command -v md5sum >/dev/null; then
      md5sum "$csv_file" | awk '{ print $1 }'
    else
      echo "Neither md5 nor md5sum command found" >&2
      return 1
    fi
  }

  local source_hash=$(generate_table_hash "source")
  local target_hash=$(generate_table_hash "target")

  log "Source data hash: $source_hash"
  log "Target data hash: $target_hash"

  if [ "$source_hash" = "$target_hash" ]; then
    success "Data integrity verified: source and target databases match 100%"
    return 0
  else
    error "Data integrity check failed: source and target databases do not match"
    log "You can compare the dumps using: diff /tmp/pg_flo_source_dump.csv /tmp/pg_flo_target_dump.csv"
    return 1
  fi
}

test_pg_flo_copy_only() {
  setup_postgres
  create_users
  populate_initial_data

  start_pg_flo_copy_only
  start_pg_flo_worker

  log "Waiting for changes to replicate..."
  sleep 180
  stop_pg_flo_gracefully

  compare_row_counts || return 1
  verify_large_json || return 1
  verify_data_integrity || return 1
}

log "Starting pg_flo copy-only test..."
if test_pg_flo_copy_only; then
  success "All tests passed! 🎉"
  exit 0
else
  error "Some tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
