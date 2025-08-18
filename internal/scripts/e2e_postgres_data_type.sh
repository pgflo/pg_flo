#!/bin/bash
set -euo pipefail

source "$(dirname "$0")/e2e_common.sh"

create_test_table() {
  log "Creating test table with various data types including comprehensive array testing..."

  # Create required extensions
  run_sql "CREATE EXTENSION IF NOT EXISTS hstore;"

  # Define custom types (enum and composite types)
  run_sql "DROP TYPE IF EXISTS mood CASCADE;"
  run_sql "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');"
  run_sql "DROP TYPE IF EXISTS name_type CASCADE;"
  run_sql "CREATE TYPE name_type AS (first_name text, last_name text);"

  # Create the test table with various data types
  run_sql "DROP TABLE IF EXISTS public.data_type_test;"
  run_sql "CREATE TABLE public.data_type_test (
    id serial PRIMARY KEY NOT NULL,
    text_data text NOT NULL,
    binary_data bytea NOT NULL,
    blob_data oid NOT NULL,
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
    array_text_with_commas text[] NOT NULL,
    array_text_with_quotes text[] NOT NULL,
    array_text_empty text[] NOT NULL,
    array_varchar varchar[] NOT NULL,
    array_char char(1)[] NOT NULL,
    array_bool boolean[] NOT NULL,
    array_nested_int integer[][] NOT NULL,
    array_with_nulls text[],
    array_mixed_special text[] NOT NULL,
    numeric_data numeric(10, 2) NOT NULL,
    uuid_data uuid NOT NULL,
    inet_data inet NOT NULL,
    cidr_data cidr NOT NULL,
    macaddr_data macaddr NOT NULL,
    macaddr8_data macaddr8 NOT NULL,
    point_data point NOT NULL,
    line_data line NOT NULL,
    lseg_data lseg NOT NULL,
    box_data box NOT NULL,
    path_data path NOT NULL,
    polygon_data polygon NOT NULL,
    circle_data circle NOT NULL,
    interval_data interval NOT NULL,
    hstore_data hstore NOT NULL,
    tsrange_data tsrange NOT NULL,
    tstzrange_data tstzrange NOT NULL,
    daterange_data daterange NOT NULL,
    int4range_data int4range NOT NULL,
    int8range_data int8range NOT NULL,
    numrange_data numrange NOT NULL,
    tsvector_data tsvector NOT NULL,
    tsquery_data tsquery NOT NULL,
    xml_data xml NOT NULL,
    enum_data mood NOT NULL,
    composite_data name_type NOT NULL,
    text_data_empty text NOT NULL default ''
  );"
  success "Test table created in source database"
}

insert_test_data() {
  log "Inserting test data..."

  # Insert test data into the table
  run_sql "INSERT INTO public.data_type_test (
    text_data,
    binary_data,
    blob_data,
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
    array_text_with_commas,
    array_text_with_quotes,
    array_text_empty,
    array_varchar,
    array_char,
    array_bool,
    array_nested_int,
    array_with_nulls,
    array_mixed_special,
    numeric_data,
    uuid_data,
    inet_data,
    cidr_data,
    macaddr_data,
    macaddr8_data,
    point_data,
    line_data,
    lseg_data,
    box_data,
    path_data,
    polygon_data,
    circle_data,
    interval_data,
    hstore_data,
    tsrange_data,
    tstzrange_data,
    daterange_data,
    int4range_data,
    int8range_data,
    numrange_data,
    tsvector_data,
    tsquery_data,
    xml_data,
    enum_data,
    composite_data
  ) VALUES (
    'null', -- text_data with literal 'null' string
    decode('deadbeef', 'hex'), -- binary_data
    lo_create(0), -- blob_data
    '2023-04-15 12:00:00+00', -- timestamp_data
    3.14159265359, -- float_data
    42, -- integer_data
    32767, -- smallint_data
    9223372036854775807, -- bigint_data
    true, -- boolean_data
    '{
        \"nested\": {
            \"string\": \"value\",
            \"number\": 42,
            \"bool\": true,
            \"null_value\": null,
            \"array\": [1, 2, 3],
            \"nested_object\": {
                \"key\": \"value\",
                \"numbers\": [1.1, 2.2, 3.3],
                \"mixed_array\": [\"text\", 1, true, null, {\"key\": \"value\"}]
            }
        },
        \"arrays\": {
            \"empty\": [],
            \"strings\": [\"a\", \"b\", \"c\"],
            \"objects\": [
                {\"id\": 1, \"name\": \"first\"},
                {\"id\": 2, \"name\": \"second\"}
            ]
        },
        \"special_chars\": \"Hello \\\"quoted\\\" and \\n newline\"
    }',
    '{
        \"metadata\": {
            \"version\": 1,
            \"tags\": [\"test\", \"json\"],
            \"config\": {
                \"enabled\": true,
                \"limits\": {
                    \"max\": 100,
                    \"min\": 0
                },
                \"options\": [
                    {\"name\": \"opt1\", \"value\": 1},
                    {\"name\": \"opt2\", \"value\": 2}
                ]
            }
        },
        \"data\": {
            \"nulls\": [null, \"null\", {\"key\": null}],
            \"unicode\": \"Hello 世界 👋\",
            \"deep_nesting\": {
                \"level1\": {
                    \"level2\": {
                        \"level3\": {
                            \"value\": \"deep\"
                        }
                    }
                }
            }
        }
    }',
    ARRAY['null', 'two', 'three'], -- array_text_data
    ARRAY[1, 2, 3], -- array_int_data
    ARRAY[
      E'\\\\x010203'::bytea,
      E'\\\\x040506'::bytea
    ], -- array_bytea_data
    ARRAY['hello world', 'test,value', 'another,item'], -- array_text_with_commas (commas in values)
    ARRAY['hello\"world', 'test\\\\value'], -- array_text_with_quotes (quotes and backslashes)
    ARRAY[]::text[], -- array_text_empty (empty array)
    ARRAY['varchar1', 'varchar2']::varchar[], -- array_varchar
    ARRAY['a', 'b', 'c']::char(1)[], -- array_char (char array - different OID)
    ARRAY[true, false, true], -- array_bool (boolean array)
    ARRAY[[1,2],[3,4]], -- array_nested_int (multidimensional - critical test)
    ARRAY['value1', NULL, 'value3'], -- array_with_nulls (array with NULLs)
    ARRAY['tab\tchar', 'newline\nchar', 'quote''test'], -- array_mixed_special (special characters)
    123.45, -- numeric_data
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', -- uuid_data
    '192.168.0.1', -- inet_data
    '192.168.100.128/25', -- cidr_data
    '08:00:2b:01:02:03', -- macaddr_data
    '08:00:2b:ff:fe:01:02:03', -- macaddr8_data
    '(1.5,2.5)', -- point_data
    '{1.5,2.5,3.5}', -- line_data
    '[(1,2),(3,4)]', -- lseg_data
    '( (1,2), (3,4) )', -- box_data
    '[(1,2),(3,4),(5,6)]', -- path_data
    '((1,2),(3,4),(5,6),(1,2))', -- polygon_data
    '<(1,2),3>', -- circle_data
    '1 year 2 months 3 days', -- interval_data
    '\"key\" => \"value\"', -- hstore_data (requires hstore extension)
    '[\"2021-01-01 14:30\",\"2021-01-01 15:30\"]'::tsrange, -- tsrange_data
    '[\"2021-01-01 14:30+00\",\"2021-01-01 15:30+00\"]'::tstzrange, -- tstzrange_data
    '[2021-01-01,2021-12-31]'::daterange, -- daterange_data
    '[1,10]'::int4range, -- int4range_data
    '[10000000000,20000000000]'::int8range, -- int8range_data
    '[1.5,3.5]'::numrange, -- numrange_data
    to_tsvector('The quick brown fox jumps over the lazy dog'), -- tsvector_data
    to_tsquery('fox & dog'), -- tsquery_data
    '<note><to>User</to><message>Hello, XML!</message></note>', -- xml_data
    'happy', -- enum_data
    ROW('John', 'Doe') -- composite_data
  );"
  success "Test data inserted"
}

alter_target_database() {
  log "Defining custom types and extensions in target database..."
  run_sql_target "CREATE EXTENSION IF NOT EXISTS hstore;"
  run_sql_target "DROP TYPE IF EXISTS mood CASCADE;"
  run_sql_target "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');"
  run_sql_target "DROP TYPE IF EXISTS name_type CASCADE;"
  run_sql_target "CREATE TYPE name_type AS (first_name text, last_name text);"
  success "Custom types and extensions defined in target database"
}

start_pg_flo_replication() {
  log "Starting pg_flo replicator..."
  if [ -f "$pg_flo_LOG" ]; then
    mv "$pg_flo_LOG" "${pg_flo_LOG}.bak"
    log "Backed up previous replicator log to ${pg_flo_LOG}.bak"
  fi
  $pg_flo_BIN replicator \
    --host "$PG_HOST" \
    --port "$PG_PORT" \
    --dbname "$PG_DB" \
    --user "$PG_USER" \
    --password "$PG_PASSWORD" \
    --group "group_postgres_data_type_sink" \
    --tables "data_type_test" \
    --schema "public" \
    --nats-url "$NATS_URL" \
    >"$pg_flo_LOG" 2>&1 &
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
  $pg_flo_BIN worker postgres \
    --group "group_postgres_data_type_sink" \
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
    --target-sync-schema \
    >"$pg_flo_WORKER_LOG" 2>&1 &
  pg_flo_WORKER_PID=$!
  log "pg_flo worker started with PID: $pg_flo_WORKER_PID"
  success "pg_flo worker started"
}

verify_data() {
  log "Verifying data in target database..."

  # Check the number of rows in both source and target databases
  local source_row_count
  local target_row_count

  source_row_count=$(run_sql "SELECT COUNT(*) FROM public.data_type_test;")
  target_row_count=$(run_sql_target "SELECT COUNT(*) FROM public.data_type_test;")

  # Trim whitespace
  source_row_count=$(echo "$source_row_count" | xargs)
  target_row_count=$(echo "$target_row_count" | xargs)

  if [ "$source_row_count" -ne "$target_row_count" ]; then
    error "Row count mismatch between source and target databases."
    log "🔹 Source row count: $source_row_count"
    log "🔹 Target row count: $target_row_count"
    return 1
  fi

  success "Row counts match: $source_row_count rows."

  # Retrieve checksums from source and target databases in CSV format
  local source_checksums
  local target_checksums

  source_checksums=$(run_sql "COPY (SELECT id, md5(row_to_json(t.*)::text) AS checksum FROM public.data_type_test t ORDER BY id) TO STDOUT WITH CSV;")
  target_checksums=$(run_sql_target "COPY (SELECT id, md5(row_to_json(t.*)::text) AS checksum FROM public.data_type_test t ORDER BY id) TO STDOUT WITH CSV;")

  local mismatched=0

  # Read source checksums into arrays
  local src_ids=()
  local src_checksums=()
  IFS=$'\n'
  for line in $source_checksums; do
    IFS=',' read -r id checksum <<<"$line"
    id=$(echo "$id" | xargs)
    checksum=$(echo "$checksum" | xargs)
    src_ids+=("$id")
    src_checksums+=("$checksum")
  done

  # Read target checksums into arrays
  local tgt_ids=()
  local tgt_checksums=()
  IFS=$'\n'
  for line in $target_checksums; do
    IFS=',' read -r id checksum <<<"$line"
    id=$(echo "$id" | xargs)
    checksum=$(echo "$checksum" | xargs)
    tgt_ids+=("$id")
    tgt_checksums+=("$checksum")
  done

  # Compare the source and target checksums
  local num_rows=${#src_ids[@]}
  local i
  for ((i = 0; i < num_rows; i++)); do
    local src_id="${src_ids[$i]}"
    local src_checksum="${src_checksums[$i]}"
    local tgt_id="${tgt_ids[$i]}"
    local tgt_checksum="${tgt_checksums[$i]}"

    if [ "$src_id" != "$tgt_id" ]; then
      error "Mismatch in row IDs: Source ID $src_id vs Target ID $tgt_id"
      mismatched=1
      continue
    fi

    if [ "$src_checksum" != "$tgt_checksum" ]; then
      error "Data mismatch for ID $src_id:"
      log "🔹 Source checksum: $src_checksum"
      log "🔹 Target checksum: $tgt_checksum"

      local src_data
      local tgt_data
      src_data=$(run_sql "SELECT row_to_json(t.*) FROM public.data_type_test t WHERE id = $src_id;")
      tgt_data=$(run_sql_target "SELECT row_to_json(t.*) FROM public.data_type_test t WHERE id = $src_id;")

      log "🔹 Source data: $src_data"
      log "🔹 Target data: $tgt_data"

      mismatched=1
    fi
  done

  if [ $mismatched -eq 0 ]; then
    success "Data in target database matches source database"
    return 0
  else
    error "Data mismatch detected."
    return 1
  fi
}

test_pg_flo_data_types() {
  setup_postgres
  create_test_table
  alter_target_database
  start_pg_flo_replication
  sleep 2
  start_pg_flo_worker
  sleep 2
  insert_test_data
  sleep 2
  stop_pg_flo_gracefully
  verify_data || return 1
}

# Run the test
log "Starting pg_flo CDC test for PostgreSQL data types..."
if test_pg_flo_data_types; then
  success "All data type tests passed! 🎉"
  exit 0
else
  error "Some data type tests failed. Please check the logs."
  show_pg_flo_logs
  exit 1
fi
