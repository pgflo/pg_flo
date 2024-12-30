#!/usr/bin/env ruby

require 'pg'
require 'logger'
require 'securerandom'
require 'json'
require 'digest'

class PostgresUniquenessTest
  # Database configuration
  PG_HOST = 'localhost'
  PG_PORT = 5433
  PG_USER = 'myuser'
  PG_PASSWORD = 'mypassword!@#%1234'
  PG_DB = 'mydb'

  TARGET_PG_HOST = 'localhost'
  TARGET_PG_PORT = 5434
  TARGET_PG_USER = 'targetuser'
  TARGET_PG_PASSWORD = 'targetpassword!@#1234'
  TARGET_PG_DB = 'targetdb'

  # NATS configuration
  NATS_URL = 'nats://localhost:4222'

  # Paths
  PG_FLO_BIN = './bin/pg_flo'
  PG_FLO_LOG = '/tmp/pg_flo.log'
  PG_FLO_WORKER_LOG = '/tmp/pg_flo_worker.log'

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc { |_, _, _, msg| "#{msg}\n" }
    sleep 5
    connect_to_databases
  end

  def connect_to_databases
    @source_db = PG.connect(
      host: PG_HOST,
      port: PG_PORT,
      dbname: PG_DB,
      user: PG_USER,
      password: PG_PASSWORD
    )

    @target_db = PG.connect(
      host: TARGET_PG_HOST,
      port: TARGET_PG_PORT,
      dbname: TARGET_PG_DB,
      user: TARGET_PG_USER,
      password: TARGET_PG_PASSWORD
    )
  end

  def create_test_tables
    @logger.info "Creating test tables..."

    # Create extensions and types
    @source_db.exec(<<-SQL)
      CREATE EXTENSION IF NOT EXISTS hstore;
      DROP TYPE IF EXISTS mood CASCADE;
      CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    SQL

    # Drop existing tables
    @source_db.exec(<<-SQL)
      DROP TABLE IF EXISTS public.pk_test CASCADE;
      DROP TABLE IF EXISTS public.unique_test CASCADE;
      DROP TABLE IF EXISTS public.composite_pk_test CASCADE;
      DROP TABLE IF EXISTS public.multi_unique_test CASCADE;
      DROP TABLE IF EXISTS public.mixed_constraints_test CASCADE;
    SQL

    # Create test tables
    @source_db.exec(<<-SQL)
      CREATE TABLE public.pk_test (
        id serial PRIMARY KEY,
        text_data text,
        json_data jsonb,
        array_data text[],
        numeric_data numeric(10,2),
        timestamp_data timestamp with time zone,
        enum_data mood,
        nullable_column text
      );

      CREATE TABLE public.unique_test (
        id serial,
        unique_col uuid UNIQUE NOT NULL,
        binary_data bytea,
        interval_data interval,
        data jsonb
      );

      CREATE TABLE public.composite_pk_test (
        id1 integer,
        id2 uuid,
        json_data jsonb,
        array_int_data integer[],
        data text,
        PRIMARY KEY (id1, id2)
      );

      CREATE TABLE public.multi_unique_test (
        id serial,
        unique_col1 timestamp with time zone NOT NULL,
        unique_col2 uuid NOT NULL,
        array_data text[],
        hstore_data hstore,
        data jsonb,
        CONSTRAINT multi_unique_test_unique_cols UNIQUE (unique_col1, unique_col2)
      );

      CREATE TABLE public.mixed_constraints_test (
        id serial PRIMARY KEY,
        unique_col uuid UNIQUE,
        non_unique_col jsonb,
        array_data integer[],
        data text
      );

      ALTER TABLE public.unique_test REPLICA IDENTITY USING INDEX unique_test_unique_col_key;
      ALTER TABLE public.multi_unique_test REPLICA IDENTITY USING INDEX multi_unique_test_unique_cols;
      ALTER TABLE public.mixed_constraints_test REPLICA IDENTITY FULL;
    SQL

    @logger.info "Test tables created successfully"
  end

  def start_pg_flo
    @logger.info "Starting pg_flo..."

    create_config_files

    replicator_cmd = "#{PG_FLO_BIN} replicator --config /tmp/pg_flo_replicator.yml"
    worker_cmd = "#{PG_FLO_BIN} worker postgres --config /tmp/pg_flo_worker.yml"

    @replicator_pid = spawn(replicator_cmd, out: PG_FLO_LOG, err: PG_FLO_LOG)
    sleep 2
    @worker_pid = spawn(worker_cmd, out: PG_FLO_WORKER_LOG, err: PG_FLO_WORKER_LOG)
    sleep 2
  end

  def create_config_files
    File.write('/tmp/pg_flo_replicator.yml', <<-YAML)
host: #{PG_HOST}
port: #{PG_PORT}
dbname: #{PG_DB}
user: #{PG_USER}
password: #{PG_PASSWORD}
group: group_unique
tables:
  - pk_test
  - unique_test
  - composite_pk_test
  - multi_unique_test
  - mixed_constraints_test
schema: public
nats-url: #{NATS_URL}
    YAML

    File.write('/tmp/pg_flo_worker.yml', <<-YAML)
group: group_unique
nats-url: #{NATS_URL}
source-host: #{PG_HOST}
source-port: #{PG_PORT}
source-dbname: #{PG_DB}
source-user: #{PG_USER}
source-password: #{PG_PASSWORD}
target-host: #{TARGET_PG_HOST}
target-port: #{TARGET_PG_PORT}
target-dbname: #{TARGET_PG_DB}
target-user: #{TARGET_PG_USER}
target-password: #{TARGET_PG_PASSWORD}
target-sync-schema: true
    YAML
  end

  def test_pk_operations
    @logger.info "Testing operations on pk_test..."

    # Test INSERT
    @source_db.exec_params(
      "INSERT INTO public.pk_test (text_data, json_data, array_data, numeric_data, timestamp_data, enum_data, nullable_column)
       VALUES ($1, $2, $3, $4, $5, $6, $7)",
      ['test1', '{"key": "value"}', '{a,b,c}', 123.45, '2024-03-20 10:00:00+00', 'happy', 'value1']
    )

    sleep 1
    success = verify_table_data("pk_test", "id = 1",
      "1 | test1 | {\"key\": \"value\"} | a,b,c | 123.45 | 2024-03-20 10:00:00+00 | happy | value1")

    @source_db.exec("DELETE FROM public.pk_test WHERE id = 1")
    sleep 1

    success
  end

  def test_unique_operations
    @logger.info "Testing operations on unique_test..."
    uuid = SecureRandom.uuid

    logo_path = "internal/pg_flo_logo.png"
    binary_data = File.binread(logo_path).force_encoding('BINARY')

    @source_db.exec_params(
      "INSERT INTO public.unique_test (unique_col, binary_data, interval_data, data)
       VALUES ($1, $2::bytea, $3, $4)",
      [uuid, { value: binary_data, format: 1 }, '1 year 2 months 3 days', '{"value": "test"}']
    )

    sleep 1
    verify_table_data("unique_test", "unique_col = '#{uuid}'",
      "1 | #{uuid} | #{Digest::MD5.hexdigest(binary_data)} | 1 year 2 mons 3 days | {\"value\": \"test\"}")

    # Test UPDATE
    @source_db.exec_params(
      "UPDATE public.unique_test SET data = $1 WHERE unique_col = $2",
      ['{"value": "updated_data"}', uuid]
    )

    sleep 1
    verify_table_data("unique_test", "unique_col = '#{uuid}'",
      "1 | #{uuid} | #{Digest::MD5.hexdigest(binary_data)} | 1 year 2 mons 3 days | {\"value\": \"updated_data\"}")

    # Test DELETE
    @source_db.exec_params("DELETE FROM public.unique_test WHERE unique_col = $1", [uuid])
  end

  def test_composite_pk_operations
    @logger.info "Testing operations on composite_pk_test..."
    uuid = SecureRandom.uuid

    # Test INSERT
    @source_db.exec_params(
      "INSERT INTO public.composite_pk_test (id1, id2, json_data, array_int_data, data)
       VALUES ($1, $2, $3, $4, $5)",
      [1, uuid, '{"key": "value"}', '{1,2,3}', 'test_composite']
    )

    sleep 1
    verify_table_data("composite_pk_test", "id1 = 1 AND id2 = '#{uuid}'",
      "1 | #{uuid} | {\"key\": \"value\"} | 1,2,3 | test_composite")

    # Test UPDATE
    @source_db.exec_params(
      "UPDATE public.composite_pk_test SET data = $1 WHERE id1 = $2 AND id2 = $3",
      ['updated_composite', 1, uuid]
    )

    sleep 1
    verify_table_data("composite_pk_test", "id1 = 1 AND id2 = '#{uuid}'",
      "1 | #{uuid} | {\"key\": \"value\"} | 1,2,3 | updated_composite")

    # Test DELETE
    @source_db.exec_params(
      "DELETE FROM public.composite_pk_test WHERE id1 = $1 AND id2 = $2",
      [1, uuid]
    )
  end

  def test_multi_unique_operations
    @logger.info "Testing operations on multi_unique_test..."
    uuid = SecureRandom.uuid
    timestamp = '2024-03-20 10:00:00+00'

    # Test INSERT
    @source_db.exec_params(
      "INSERT INTO public.multi_unique_test (unique_col1, unique_col2, array_data, hstore_data, data)
       VALUES ($1, $2, $3, $4, $5)",
      [timestamp, uuid, '{test1,test2}', 'key1=>value1,key2=>value2', '{"test": "multi"}']
    )

    sleep 1
    verify_table_data("multi_unique_test",
      "unique_col1 = '#{timestamp}' AND unique_col2 = '#{uuid}'",
      "1 | #{timestamp} | #{uuid} | test1,test2 | \"key1\"=>\"value1\", \"key2\"=>\"value2\" | {\"test\": \"multi\"}")

    # Test UPDATE
    @source_db.exec_params(
      "UPDATE public.multi_unique_test SET data = $1 WHERE unique_col1 = $2 AND unique_col2 = $3",
      ['{"test": "updated_multi"}', timestamp, uuid]
    )

    sleep 1
    verify_table_data("multi_unique_test",
      "unique_col1 = '#{timestamp}' AND unique_col2 = '#{uuid}'",
      "1 | #{timestamp} | #{uuid} | test1,test2 | \"key1\"=>\"value1\", \"key2\"=>\"value2\" | {\"test\": \"updated_multi\"}")

    # Test DELETE
    @source_db.exec_params(
      "DELETE FROM public.multi_unique_test WHERE unique_col1 = $1 AND unique_col2 = $2",
      [timestamp, uuid]
    )
  end

  def test_mixed_constraints_operations
    @logger.info "Testing operations on mixed_constraints_test..."
    uuid = SecureRandom.uuid

    # Test INSERT
    @source_db.exec_params(
      "INSERT INTO public.mixed_constraints_test (unique_col, non_unique_col, array_data, data)
       VALUES ($1, $2, $3, $4)",
      [uuid, '{"key": "non_unique1"}', '{1,2,3}', 'test_mixed']
    )

    sleep 1
    verify_table_data("mixed_constraints_test", "id = 1",
      "1 | #{uuid} | {\"key\": \"non_unique1\"} | 1,2,3 | test_mixed")

    # Test UPDATE
    @source_db.exec_params(
      "UPDATE public.mixed_constraints_test SET data = $1 WHERE id = $2",
      ['updated_by_pk', 1]
    )

    sleep 1
    verify_table_data("mixed_constraints_test", "id = 1",
      "1 | #{uuid} | {\"key\": \"non_unique1\"} | 1,2,3 | updated_by_pk")

    # Test DELETE
    @source_db.exec_params("DELETE FROM public.mixed_constraints_test WHERE id = $1", [1])
  end

  def build_verification_query(table, condition)
    case table
    when "pk_test"
      <<~SQL
        SELECT (
          id::text || ' | ' ||
          text_data || ' | ' ||
          json_data::text || ' | ' ||
          array_to_string(array_data, ',') || ' | ' ||
          numeric_data::text || ' | ' ||
          timestamp_data::text || ' | ' ||
          enum_data::text || ' | ' ||
          COALESCE(nullable_column, '<NULL>')
        ) AS row_data
        FROM public.#{table}
        WHERE #{condition}
        ORDER BY id
      SQL
    when "unique_test"
      <<~SQL
        SELECT (
          id::text || ' | ' ||
          unique_col::text || ' | ' ||
          MD5(binary_data) || ' | ' ||
          interval_data::text || ' | ' ||
          data::text
        ) AS row_data
        FROM public.#{table}
        WHERE #{condition}
        ORDER BY id
      SQL
    when "composite_pk_test"
      <<~SQL
        SELECT (
          id1::text || ' | ' ||
          id2::text || ' | ' ||
          json_data::text || ' | ' ||
          array_to_string(array_int_data, ',') || ' | ' ||
          data
        ) AS row_data
        FROM public.#{table}
        WHERE #{condition}
        ORDER BY id1, id2
      SQL
    when "multi_unique_test"
      <<~SQL
        SELECT (
          id::text || ' | ' ||
          unique_col1::text || ' | ' ||
          unique_col2::text || ' | ' ||
          array_to_string(array_data, ',') || ' | ' ||
          hstore_data::text || ' | ' ||
          data::text
        ) AS row_data
        FROM public.#{table}
        WHERE #{condition}
        ORDER BY id
      SQL
    when "mixed_constraints_test"
      <<~SQL
        SELECT (
          id::text || ' | ' ||
          unique_col::text || ' | ' ||
          non_unique_col::text || ' | ' ||
          array_to_string(array_data, ',') || ' | ' ||
          data
        ) AS row_data
        FROM public.#{table}
        WHERE #{condition}
        ORDER BY id
      SQL
    end
  end

  def verify_table_data(table, condition, expected_values)
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries
      query = build_verification_query(table, condition)
      result = @target_db.exec(query)

      actual_values = result.ntuples > 0 ? result[0]['row_data'] : ""

      if actual_values == expected_values
        @logger.info "✓ Table #{table} data verified successfully"
        return true
      end

      retry_count += 1
      if retry_count < max_retries
        @logger.info "Retrying verification (attempt #{retry_count + 1}/#{max_retries})..."
        sleep 1
      end
    end

    @logger.error "✗ Table #{table} data verification failed"
    @logger.error "  Condition: [#{condition}]"
    @logger.error "  Expected: #{expected_values}"
    @logger.error "  Actual:   #{actual_values}"
    false
  end

  def verify_final_state
    @logger.info "Verifying final state..."
    sleep 2  # Give time for final DELETEs to replicate

    tables = ["pk_test", "unique_test", "composite_pk_test", "multi_unique_test", "mixed_constraints_test"]

    failed = false
    tables.each do |table|
      result = @target_db.exec("SELECT COUNT(*) FROM #{table}")
      count = result[0]['count'].to_i
      if count != 0
        @logger.error "Table #{table} verification failed: expected 0 rows, got #{count}"
        @logger.error "Current state of #{table}:"
        @target_db.exec("SELECT * FROM #{table}").each do |row|
          @logger.error "  #{row.values.join(' | ')}"
        end
        failed = true
      end
    end

    if failed
      false
    else
      @logger.info "Final state verified successfully"
      true
    end
  end

  def cleanup
    Process.kill('TERM', @replicator_pid) if @replicator_pid
    Process.kill('TERM', @worker_pid) if @worker_pid
    Process.wait(@replicator_pid) if @replicator_pid
    Process.wait(@worker_pid) if @worker_pid
  end

  def run_test
    success = true
    begin
      create_test_tables
      start_pg_flo

      sleep 2

      # Run all tests and track success
      success = success && test_pk_operations
      success = success && test_unique_operations
      success = success && test_composite_pk_operations
      success = success && test_multi_unique_operations
      success = success && test_mixed_constraints_operations

      sleep 5
      success = success && verify_final_state
    ensure
      cleanup
    end
    success
  end
end

# Simple main execution
if __FILE__ == $0
  begin
    test = PostgresUniquenessTest.new
    exit(test.run_test ? 0 : 1)
  rescue => e
    puts "Error: #{e.message}"
    puts e.backtrace
    exit 1
  end
end
