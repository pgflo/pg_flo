#!/usr/bin/env ruby

# This script tests pg_flo's resume capability by:
# 1. Starting replicators and workers for multiple groups
# 2. Concurrently inserting distinct data patterns into each table:
#    - Group 1: Numeric sequences
#    - Group 2: Alphabetic patterns
#    - Group 3: UUID-like strings
# 3. Interrupting replication midway
# 4. Resuming replication
# 5. Inserting more data
# 6. Verifying data integrity and completeness without cleaning up replication slots

require 'pg'
require 'logger'
require 'securerandom'

class ResumeTest
  TOTAL_INSERTS = 5000
  INSERTS_BEFORE_INTERRUPT = 1500
  RESUME_WAIT_TIME = 1 # seconds
  NUM_GROUPS = 4

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

  NATS_URL = 'nats://localhost:4222'

  PG_FLO_BIN = './bin/pg_flo'
  PG_FLO_LOG = '/tmp/pg_flo.log'
  PG_FLO_WORKER_LOG = '/tmp/pg_flo_worker.log'

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc { |_, _, _, msg| "#{msg}\n" }

    sleep 10
    connect_to_databases
    @replicator_pids = []
    @worker_pids = []
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

  def create_tables
    @logger.info "Creating test tables in source database..."
    NUM_GROUPS.times do |i|
      table_name = "test_table_#{i+1}"
      @source_db.exec(<<-SQL)
        DROP TABLE IF EXISTS public.#{table_name};
        CREATE TABLE public.#{table_name} (
          id serial PRIMARY KEY,
          data text,
          sequence_num integer,
          phase text,
          value_type text,
          created_at timestamp DEFAULT current_timestamp
        );
      SQL
    end
    @logger.info "Test tables created in source database"
  end

  def start_pg_flo
    NUM_GROUPS.times do |i|
      group = "resume_group_#{i+1}"
      table_name = "test_table_#{i+1}"

      replicator_cmd = "#{PG_FLO_BIN} replicator " \
        "--host #{PG_HOST} " \
        "--port #{PG_PORT} " \
        "--dbname #{PG_DB} " \
        "--user #{PG_USER} " \
        "--password #{PG_PASSWORD} " \
        "--group #{group} " \
        "--tables #{table_name} " \
        "--schema public " \
        "--nats-url #{NATS_URL}"

      worker_cmd = "#{PG_FLO_BIN} worker postgres " \
        "--group #{group} " \
        "--nats-url #{NATS_URL} " \
        "--source-host #{PG_HOST} " \
        "--source-port #{PG_PORT} " \
        "--source-dbname #{PG_DB} " \
        "--source-user #{PG_USER} " \
        "--source-password #{PG_PASSWORD} " \
        "--target-host #{TARGET_PG_HOST} " \
        "--target-port #{TARGET_PG_PORT} " \
        "--target-dbname #{TARGET_PG_DB} " \
        "--target-user #{TARGET_PG_USER} " \
        "--target-password #{TARGET_PG_PASSWORD} " \
        "--target-sync-schema"

      replicator_log = "#{PG_FLO_LOG}_#{group}"
      worker_log = "#{PG_FLO_WORKER_LOG}_#{group}"

      @logger.info "Starting pg_flo replicator for group #{group}..."
      replicator_pid = spawn(replicator_cmd, out: replicator_log, err: replicator_log)
      @replicator_pids << replicator_pid

      @logger.info "Starting pg_flo worker for group #{group}..."
      worker_pid = spawn(worker_cmd, out: worker_log, err: worker_log)
      @worker_pids << worker_pid
    end
  end

  def insert_batch_concurrent(group, table_name, start_seq, end_seq, phase, value_type)
    Thread.new do
      db = PG.connect(
        host: PG_HOST,
        port: PG_PORT,
        dbname: PG_DB,
        user: PG_USER,
        password: PG_PASSWORD
      )

      (start_seq..end_seq).each do |i|
        value = generate_value(value_type, i)
        db.exec_params(
          "INSERT INTO public.#{table_name} (data, sequence_num, phase, value_type) " \
          "VALUES ($1, $2, $3, $4)",
          [value, i, phase, value_type]
        )
        sleep(rand * 0.01) # Random small delay
      end

      db.close
    end
  end

  def generate_value(type, i)
    case type
    when 'numeric'
      (i * 1000).to_s
    when 'alphabetic'
      letter = (65 + (i % 26)).chr
      letter * 3
    when 'uuid'
      "test-#{format("%04d", i)}-#{format("%04d", i*2)}-#{format("%04d", i*3)}-#{format("%04d", i*4)}"
    else
      "value-#{i}"
    end
  end

  def verify_table_data(table_name, value_type)
    @logger.info "Verifying data for #{table_name} (type: #{value_type})"

    source_total = @source_db.exec("SELECT COUNT(*) FROM public.#{table_name}").getvalue(0,0).to_i
    target_total = @target_db.exec("SELECT COUNT(*) FROM public.#{table_name}").getvalue(0,0).to_i

    if source_total != target_total
      @logger.error "Count mismatch for #{table_name}: source=#{source_total}, target=#{target_total}"
      return false
    end

    type_mismatch = @target_db.exec(
      "SELECT COUNT(*) FROM public.#{table_name} WHERE value_type != $1", [value_type]
    ).getvalue(0,0).to_i

    if type_mismatch != 0
      @logger.error "Found #{type_mismatch} rows with incorrect value_type in #{table_name}"
      return false
    end

    ['pre', 'post'].each do |phase|
      source_phase_count = @source_db.exec(
        "SELECT COUNT(*) FROM public.#{table_name} WHERE phase = $1", [phase]
      ).getvalue(0,0).to_i
      target_phase_count = @target_db.exec(
        "SELECT COUNT(*) FROM public.#{table_name} WHERE phase = $1", [phase]
      ).getvalue(0,0).to_i

      if source_phase_count != target_phase_count
        @logger.error "Phase count mismatch for #{table_name} phase #{phase}: source=#{source_phase_count}, target=#{target_phase_count}"
        return false
      end
    end

    @logger.info "Verification passed for #{table_name}"
    true
  end

  def test_resume
    create_tables
    start_pg_flo

    sleep 2

    # Start pre-interrupt inserts
    @logger.info "Starting concurrent pre-interrupt inserts..."
    threads = []
    NUM_GROUPS.times do |i|
      group = "resume_group_#{i+1}"
      table_name = "test_table_#{i+1}"
      value_type = case i
      when 0
        'numeric'
      when 1
        'alphabetic'
      when 2
        'uuid'
      else
        'unknown'
      end
      threads << insert_batch_concurrent(group, table_name, 1, INSERTS_BEFORE_INTERRUPT, 'pre', value_type)
    end

    sleep 5

    @logger.info "Sending SIGTERM to trigger graceful shutdown..."
    @replicator_pids.each do |pid|
      Process.kill('TERM', pid)
    end

    @worker_pids.each do |pid|
      Process.kill('TERM', pid)
    end

    @logger.info "Waiting for processes to shut down..."
    @replicator_pids.each do |pid|
      Process.wait(pid)
    end

    @worker_pids.each do |pid|
      Process.wait(pid)
    end

    @logger.info "Waiting #{RESUME_WAIT_TIME} seconds before resuming..."
    sleep RESUME_WAIT_TIME

    @replicator_pids = []
    @worker_pids = []
    start_pg_flo

    sleep 2

    @logger.info "Waiting for pre-interrupt inserts to complete..."
    threads.each(&:join)

    @logger.info "Starting concurrent post-interrupt inserts..."
    threads = []
    NUM_GROUPS.times do |i|
      group = "resume_group_#{i+1}"
      table_name = "test_table_#{i+1}"
      value_type = case i
      when 0
        'numeric'
      when 1
        'alphabetic'
      when 2
        'uuid'
      else
        'unknown'
      end
      threads << insert_batch_concurrent(group, table_name, INSERTS_BEFORE_INTERRUPT + 1, TOTAL_INSERTS, 'post', value_type)
    end

    @logger.info "Waiting for all inserts to complete..."
    threads.each(&:join)

    sleep 60

    @logger.info "Sending final SIGTERM to cleanup..."
    @replicator_pids.each do |pid|
      Process.kill('TERM', pid) rescue Errno::ESRCH
    end

    @worker_pids.each do |pid|
      Process.kill('TERM', pid) rescue Errno::ESRCH
    end

    @logger.info "Waiting for processes to shut down..."
    @replicator_pids.each do |pid|
      Process.wait(pid) rescue Errno::ECHILD
    end

    @worker_pids.each do |pid|
      Process.wait(pid) rescue Errno::ECHILD
    end

    all_passed = true
    NUM_GROUPS.times do |i|
      table_name = "test_table_#{i+1}"
      value_type = case i
      when 0
        'numeric'
      when 1
        'alphabetic'
      when 2
        'uuid'
      else
        'unknown'
      end
      unless verify_table_data(table_name, value_type)
        all_passed = false
      end
    end

    if all_passed
      @logger.info "Resume test passed! 🎉"
      exit 0
    else
      @logger.error "Resume test failed. Check the logs for details."
      exit 1
    end
  end

  def run_test
    test_resume
  rescue => e
    @logger.error "Error during test: #{e.message}"
    @logger.error e.backtrace.join("\n")
    exit 1
  ensure
    (@replicator_pids + @worker_pids).each do |pid|
      begin
        Process.kill('TERM', pid)
      rescue Errno::ESRCH
      end
    end
  end
end

ResumeTest.new.run_test
