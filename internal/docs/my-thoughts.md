Implement schema wildcard discovery for 'schemas.public: "\*"' config to auto-discover tables
Implement actual PostgreSQL WAL streaming in DirectReplicator using existing replication connection patterns
Wire existing copy-and-stream logic from copy_and_stream_replicator.go into DirectReplicator
Implement proper commit lock pattern with BEGIN/COMMIT message handling and transaction consolidation
Implement PebbleDB spilling for large transactions exceeding 128MB memory limit
Research and integrate pg_parquet for type-safe PostgreSQL to Parquet conversion without encoding overhead
Implement key-based operation consolidation within transaction boundaries to reduce operations by 50-90%
Create comprehensive e2e tests covering copy+stream, transaction boundaries, large transactions, data types, and crash recovery
Fix PebbleDB double-close panic on graceful shutdown
Implement comprehensive PostgreSQL to Parquet type mapping supporting all data types including arrays, JSON, binary
Implement parallel range-based copy workers following existing copy_and_stream_replicator.go patterns
Implement worker heartbeat system with 5s updates and 1min failure detection
Implement capacity monitoring to suggest scaling when servers reach max_workers_per_server limit
Validate crash recovery for copy ranges, streaming LSN, and transaction state
Ensure all tests pass with 'make test' and 'make lint' without errors

- remove inline comments
- port direct sink to match types from pg_postgres and resumability
- horizonaly scaling by group & publication calrifiication in doc
- how are tables added and removed ? by adding removing from config and rollin deploy?
- no double encoding/decoding
  -is commit lock even working? are we spilling to disk?
- is consolidation even working?
- no need for conslidation in copy
- what about include/exclude column

Update metadata store schema to match design doc and actual code requirements

Implement worker heartbeat system with 5s updates and 1min failure detection
Wire existing copy-and-stream logic from copy_and_stream_replicator.go into DirectReplicator
Implement worker heartbeat system with 5s updates and 1min failure detection

Research proper Go parquet libraries and similar projects like pg_parquet for production-ready implementation
Fix 'jsonb || json' operator error in e2e test UPDATE statements
Fix expected row count calculation in e2e test (currently off by 1)
Investigate why initial 50 transactions keep getting re-inserted (2511 instead of 60) - this is data integrity critical
Fix missing 'warn' function in e2e test validation
Research proper type mapping for parquet instead of converting everything to strings - follow pg_parquet approach
Evaluate current parquet implementation for unnecessary encoding/decoding
Implement production-ready parquet writer using proper Arrow types like pg_parquet instead of string conversion - critical for production use
Fix whitespace trimming in consolidation test text comparison
Add parquet-tools cat validation to e2e test to inspect actual data content and verify no duplicates/missing records
Ensure comprehensive validation of parquet files against source data for completeness and accuracy

Study pg_parquet's exact approach to PostgreSQL to Arrow type mapping without precision loss
The panic is in marshal.Unmarshal - find the exact field causing the zero Value issue
Fixed by restarting containers
Study exactly how PeerDB handles decimal/numeric without any string conversion
Completed: Removed ALL string parsing from numeric, float, date, timestamp conversion functions
Test the native pgx approach with e2e test to see if parquet-tools cat works
