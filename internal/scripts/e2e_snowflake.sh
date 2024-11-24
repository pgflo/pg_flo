#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_test_table() {
  log "Creating test table in PostgreSQL..."
  run_sql "DROP TABLE IF EXISTS public.data_type_test;"
  run_sql "CREATE TABLE public.data_type_test (
    id serial PRIMARY KEY NOT NULL,
    text_data text NOT NULL,
    binary_data bytea NOT NULL,
    timestamp_data timestamp with time zone NOT NULL,
    float_data double precision NOT NULL,
    integer_data integer NOT NULL,
    smallint_data smallint NOT NULL,
    bigint_data bigint NOT NULL,
    boolean_data boolean NOT NULL,
    json_data json NOT NULL,
    jsonb_data jsonb NOT NULL,
    array_text_data text[] NOT NULL,
    array_int_data integer[] NOT NULL,
    array_bytea_data bytea[] NOT NULL,
    numeric_data numeric(10, 2) NOT NULL,
    uuid_data uuid NOT NULL,
    inet_data inet NOT NULL,
    cidr_data cidr NOT NULL,
    macaddr_data macaddr NOT NULL,
    interval_data interval NOT NULL
  );"
  success "Test table created"
}

start_pg_flo_replication() {
  log "Starting pg_flo replicator..."
  if [ -f "$pg_flo_LOG" ]; then
    mv "$pg_flo_LOG" "${pg_flo_LOG}.bak"
    log "Backed up previous replicator log to ${pg_flo_LOG}.bak"
  fi

  cat >"/tmp/pg_flo_replicator.yml" <<EOF
# Replicator PostgreSQL connection settings
host: "${PG_HOST}"
port: ${PG_PORT}
dbname: "${PG_DB}"
user: "${PG_USER}"
password: "${PG_PASSWORD}"
schema: "public"
group: "test_group_snowflake"
copy-and-stream: true
tables:
  - data_type_test
nats-url: "nats://localhost:4222"
EOF

  $pg_flo_BIN replicator --config "/tmp/pg_flo_replicator.yml" >"$pg_flo_LOG" 2>&1 &
  pg_flo_PID=$!
  log "pg_flo replicator started with PID: $pg_flo_PID"
  success "pg_flo replicator started"
}

start_pg_flo_worker() {
  log "Starting pg_flo worker with Snowflake sink..."
  if [ -f "$pg_flo_WORKER_LOG" ]; then
    mv "$pg_flo_WORKER_LOG" "${pg_flo_WORKER_LOG}.bak"
    log "Backed up previous worker log to ${pg_flo_WORKER_LOG}.bak"
  fi

  # Create worker config file
  cat >"/tmp/pg_flo_worker.yml" <<EOF
# Worker settings
group: "test_group_snowflake"
nats-url: "nats://localhost:4222"
batch-size: 100

# Source connection for schema sync
source-host: "${PG_HOST}"
source-port: ${PG_PORT}
source-dbname: "${PG_DB}"
source-user: "${PG_USER}"
source-password: "${PG_PASSWORD}"

# Snowflake connection settings
snowflake-account: "${SNOWFLAKE_ACCOUNT}"
snowflake-user: "${SNOWFLAKE_USER}"
snowflake-password: "${SNOWFLAKE_PASSWORD}"
snowflake-role: "${SNOWFLAKE_ROLE}"
snowflake-warehouse: "${SNOWFLAKE_WAREHOUSE}"
snowflake-database: "${SNOWFLAKE_DATABASE}"
snowflake-schema: "${SNOWFLAKE_SCHEMA}"
EOF

  $pg_flo_BIN worker snowflake --config "/tmp/pg_flo_worker.yml" >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

populate_initial_data() {
  log "Populating initial data..."
  run_sql "INSERT INTO public.data_type_test (
    text_data,
    binary_data,
    timestamp_data,
    float_data,
    integer_data,
    smallint_data,
    bigint_data,
    boolean_data,
    json_data,
    jsonb_data,
    array_text_data,
    array_int_data,
    array_bytea_data,
    numeric_data,
    uuid_data,
    inet_data,
    cidr_data,
    macaddr_data,
    interval_data
  ) SELECT
    'TEXT' || generate_series,
    decode('deadbeef', 'hex'),
    current_timestamp + (generate_series || ' minutes')::interval,
    random() * 100,
    generate_series,
    generate_series % 32767,
    generate_series,
    (random() > 0.5),
    json_build_object('key', 'value' || generate_series, 'number', generate_series),
    jsonb_build_object('key', 'value' || generate_series, 'number', generate_series),
    ARRAY['a'||generate_series, 'b'||generate_series, 'c'||generate_series],
    ARRAY[generate_series, generate_series+1, generate_series+2],
    ARRAY[decode('deadbeef', 'hex'), decode('beefdead', 'hex')],
    random() * 1000,
    gen_random_uuid(),
    ('192.168.1.' || (generate_series % 255))::inet,
    '192.168.1.0/24'::cidr,
    '08:00:2b:01:02:03'::macaddr,
    (generate_series || ' days')::interval
  FROM generate_series(1, 10000);"

  run_sql "ANALYZE public.data_type_test;"
  success "Initial data populated"
}

simulate_concurrent_changes() {
  log "Simulating concurrent changes..."
  for i in {1..100}; do
    run_sql "INSERT INTO public.data_type_test (
      text_data,
      binary_data,
      timestamp_data,
      float_data,
      integer_data,
      smallint_data,
      bigint_data,
      boolean_data,
      json_data,
      jsonb_data,
      array_text_data,
      array_int_data,
      array_bytea_data,
      numeric_data,
      uuid_data,
      inet_data,
      cidr_data,
      macaddr_data,
      interval_data
    ) VALUES (
      'Concurrent text $i',
      decode('deadbeef', 'hex'),
      current_timestamp,
      random() * 100,
      $i,
      $i,
      $i,
      true,
      '{\"key\": \"concurrent_$i\"}',
      '{\"key\": \"concurrent_$i\"}',
      ARRAY['a'||$i, 'b'||$i, 'c'||$i],
      ARRAY[$i, $i+1, $i+2],
      ARRAY[decode('deadbeef', 'hex'), decode('beefdead', 'hex')],
      random() * 1000,
      gen_random_uuid(),
      ('192.168.1.' || ($i % 255))::inet,
      '192.168.1.0/24'::cidr,
      '08:00:2b:01:02:03'::macaddr,
      ($i || ' days')::interval
    );"
  done

  run_sql "UPDATE public.data_type_test SET
    binary_data = decode('deadc0de', 'hex'),
    json_data = '{\"key\": \"updated_5000\", \"number\": 5000}',
    jsonb_data = '{\"array\": [5000, 5001, 5002], \"nested\": {\"key\": \"updated_5000\"}}',
    timestamp_data = CURRENT_TIMESTAMP
    WHERE id = 5000;"

  run_sql "DELETE FROM public.data_type_test WHERE id % 1000 = 0;"
  success "Concurrent changes simulated"
}

test_pg_flo_snowflake() {
  setup_postgres
  create_test_table
  populate_initial_data
  start_pg_flo_replication
  sleep 5
  start_pg_flo_worker
  sleep 5
  simulate_concurrent_changes
  sleep 45000
  stop_pg_flo_gracefully
}

log "Starting data setup for Snowflake test..."
test_pg_flo_snowflake
log "Data setup completed"

# -- Basic row count verification
# SELECT COUNT(*) FROM data_type_test;

# -- Verify the specific record with binary and JSON updates (ID 5000)
# SELECT
#     id,
#     binary_data,
#     json_data,
#     jsonb_data,
#     timestamp_data
# FROM data_type_test
# WHERE id = 5000;

# -- Verify deleted records (should return no rows)
# SELECT id
# FROM data_type_test
# WHERE id IN (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000);

# -- Verify concurrent inserts (should show 100 records)
# SELECT
#     id,
#     text_data,
#     binary_data,
#     json_data,
#     array_int_data,
#     timestamp_data
# FROM data_type_test
# WHERE id > 10000
# ORDER BY id;

# -- Verify data type conversions for a sample record
# SELECT
#     id,
#     text_data,
#     binary_data,
#     timestamp_data,
#     float_data,
#     integer_data,
#     smallint_data,
#     bigint_data,
#     boolean_data,
#     json_data,
#     jsonb_data,
#     array_text_data,
#     array_int_data,
#     array_bytea_data,
#     numeric_data,
#     uuid_data,
#     inet_data,
#     cidr_data,
#     macaddr_data,
#     interval_data
# FROM data_type_test
# WHERE id = 42;  -- arbitrary row for type checking
