#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

BENCHMARK_ROWS=${BENCHMARK_ROWS:-10000}
BATCH_SIZE=${BATCH_SIZE:-1000}
FETCH_BATCH=${FETCH_BATCH:-10}
MAX_COPY_WORKERS=${MAX_COPY_WORKERS:-8}
TEST_MODE=${TEST_MODE:-"copy-and-stream"}

RESULTS_FILE="/tmp/pg_flo_benchmark_results.json"
START_TIME=""
END_TIME=""

start_timer() {
  START_TIME=$(date +%s.%N)
}

end_timer() {
  END_TIME=$(date +%s.%N)
}

calculate_duration() {
  echo "$END_TIME - $START_TIME" | bc -l
}

calculate_throughput() {
  local rows=$1
  local duration=$2
  echo "scale=2; $rows / $duration" | bc -l
}

log_performance() {
  local phase=$1
  local rows=$2
  local duration=$3
  local throughput=$4
  log "📊 Performance Results - $phase:"
  log "   Rows processed: $rows"
  log "   Duration: ${duration}s"
  log "   Throughput: ${throughput} rows/sec"
}

create_benchmark_table() {
  log "Creating benchmark table..."
  run_sql "DROP TABLE IF EXISTS public.benchmark_table CASCADE;"
  run_sql "CREATE TABLE public.benchmark_table (
    id bigserial PRIMARY KEY,
    user_id bigint NOT NULL,
    email varchar(255),
    data jsonb,
    balance numeric(15,2),
    created_at timestamp DEFAULT current_timestamp
  );"
  success "Benchmark table created"
}

generate_test_data() {
  local num_rows=$1
  log "Generating $num_rows test records..."
  start_timer

  run_sql "INSERT INTO public.benchmark_table (user_id, email, data, balance)
    SELECT
      generate_series(1, $num_rows),
      'user' || generate_series(1, $num_rows) || '@example.com',
      json_build_object('key', 'value' || generate_series(1, $num_rows), 'number', generate_series(1, $num_rows)),
      random() * 1000
  ;"

  run_sql "ANALYZE public.benchmark_table;"

  end_timer
  local generation_time=$(calculate_duration)
  local generation_throughput=$(calculate_throughput "$num_rows" "$generation_time")
  log_performance "Data Generation" "$num_rows" "$generation_time" "$generation_throughput"
  success "Test data generated"
}

start_replicator() {
  local mode=$1
  log "Starting pg_flo replicator in $mode mode..."

  local mode_flags=""
  case $mode in
    "stream") mode_flags="--stream" ;;
    "copy-and-stream") mode_flags="--copy-and-stream --max-copy-workers-per-table $MAX_COPY_WORKERS" ;;
    "copy") mode_flags="--copy --max-copy-workers-per-table $MAX_COPY_WORKERS" ;;
    *) error "Unknown test mode: $mode"; exit 1 ;;
  esac

  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "benchmark_group" \
    --tables "benchmark_table" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    "$mode_flags" \
    >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  success "pg_flo replicator started (PID: $pg_flo_PID)"
}

start_worker() {
  log "Starting pg_flo worker with PostgreSQL sink..."
  $pg_flo_BIN worker postgres \
    --group "benchmark_group" \
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
    --batch-size "$BATCH_SIZE" \
    --target-sync-schema \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  success "pg_flo worker started (PID: $pg_flo_WORKER_PID)"
}

simulate_concurrent_operations() {
  local num_operations=${1:-1000}
  log "Simulating $num_operations operations during replication..."
  start_timer

  for i in $(seq 1 "$num_operations"); do
    local new_id=$((BENCHMARK_ROWS + i))
    run_sql "INSERT INTO public.benchmark_table (user_id, email, data, balance)
             VALUES ($new_id, 'user$i@test.com', '{\"test\": $i}', $i * 1.5);"
    if [ $((i % 5)) -eq 0 ]; then
      run_sql "UPDATE public.benchmark_table SET balance = balance + 10 WHERE id = $i;"
    fi
  done

  end_timer
  local ops_duration=$(calculate_duration)
  local ops_throughput=$(calculate_throughput "$num_operations" "$ops_duration")
  log_performance "Operations" "$num_operations" "$ops_duration" "$ops_throughput"
  success "Operations completed"
}

measure_replication_performance() {
  local test_mode=$1
  log "📊 Starting throughput benchmark in $test_mode mode..."
  log "Configuration: ROWS=$BENCHMARK_ROWS, BATCH_SIZE=$BATCH_SIZE, WORKERS=$MAX_COPY_WORKERS"

  setup_postgres
  create_benchmark_table

  if [ "$test_mode" != "stream" ]; then
    generate_test_data "$BENCHMARK_ROWS"
  fi

  start_replicator "$test_mode"
  start_worker

  log "Waiting for replication to initialize..."
  sleep 5

  start_timer

  if [ "$test_mode" = "stream" ]; then
    generate_test_data "$BENCHMARK_ROWS"
  else
    simulate_concurrent_operations 1000
  fi

  log "Waiting for replication to complete..."
  local max_wait=120
  local wait_count=0

  while [ $wait_count -lt $max_wait ]; do
    local source_count=$(run_sql "SELECT COUNT(*) FROM public.benchmark_table")
    local target_count=$(run_sql_target "SELECT COUNT(*) FROM public.benchmark_table" 2>/dev/null || echo "0")

    if [ "$source_count" = "$target_count" ] && [ "$source_count" -gt 0 ]; then
      log "Replication completed: $source_count rows replicated"
      break
    fi

    if [ $((wait_count % 10)) -eq 0 ]; then
      log "Waiting... Source: $source_count, Target: $target_count (${wait_count}s/${max_wait}s)"
    fi

    sleep 1
    wait_count=$((wait_count + 1))
  done

  end_timer

  local total_duration=$(calculate_duration)
  local source_count=$(run_sql "SELECT COUNT(*) FROM public.benchmark_table")
  local target_count=$(run_sql_target "SELECT COUNT(*) FROM public.benchmark_table")
  local replication_throughput=$(calculate_throughput "$target_count" "$total_duration")

  stop_pg_flo_gracefully

  cat <<EOF > "$RESULTS_FILE"
{
  "test_mode": "$test_mode",
  "configuration": {
    "benchmark_rows": $BENCHMARK_ROWS,
    "batch_size": $BATCH_SIZE,
    "max_copy_workers": $MAX_COPY_WORKERS
  },
  "results": {
    "total_duration_seconds": $total_duration,
    "source_row_count": $source_count,
    "target_row_count": $target_count,
    "replication_throughput_rows_per_second": $replication_throughput,
    "data_integrity_check": $([ "$source_count" = "$target_count" ] && echo "\"PASS\"" || echo "\"FAIL\"")
  },
  "timestamp": "$(date -Iseconds)"
}
EOF

  log_performance "Total Replication ($test_mode mode)" "$target_count" "$total_duration" "$replication_throughput"

  if [ "$source_count" = "$target_count" ]; then
    success "✅ Benchmark completed successfully! Results saved to $RESULTS_FILE"
    return 0
  else
    error "❌ Data integrity check failed: Source($source_count) != Target($target_count)"
    return 1
  fi
}

show_results() {
  if [ -f "$RESULTS_FILE" ]; then
    log "📊 Benchmark Results:"
    echo "----------------------------------------"
    cat "$RESULTS_FILE" | jq -r '
      "Mode: " + .test_mode,
      "Duration: " + (.results.total_duration_seconds | tonumber | tostring) + "s",
      "Rows: " + (.results.target_row_count | tostring),
      "Throughput: " + (.results.replication_throughput_rows_per_second | tonumber | tostring) + " rows/sec",
      "Integrity: " + .results.data_integrity_check
    '
    echo "----------------------------------------"
  fi
}

log "🚀 Starting pg_flo Throughput Benchmark"
log "Mode: $TEST_MODE | Rows: $BENCHMARK_ROWS | Batch: $BATCH_SIZE"

if measure_replication_performance "$TEST_MODE"; then
  show_results
  success "🎉 Benchmark completed successfully!"
  exit 0
else
  error "❌ Benchmark failed"
  show_pg_flo_logs
  show_results
  exit 1
fi
