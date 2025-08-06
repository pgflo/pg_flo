# <img src="internal/pg_flo_logo.png" alt="pg_flo logo" width="40" align="center"> pg_flo

[![CI](https://github.com/pgflo/pg_flo/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pgflo/pg_flo/actions/workflows/ci.yml)
[![Integration](https://github.com/pgflo/pg_flo/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/pgflo/pg_flo/actions/workflows/integration.yml)
[![Release](https://img.shields.io/github/v/release/pgflo/pg_flo?style=flat&color=#959DA5&sort=semver)](https://github.com/pgflo/pg_flo/releases/latest)
[![Docker Image](https://img.shields.io/docker/v/pgflo/pg_flo?style=flat&label=docker&color=#959DA5&label=docker&sort=semver)](https://hub.docker.com/r/pgflo/pg_flo/tags)

> The easiest way to move and transform data between PostgreSQL databases using Logical Replication.

ℹ️ `pg_flo` is in active development. The design and architecture is continuously improving. PRs/Issues are very much welcome 🙏

## Key Features

- **Real-time Data Streaming** - Capture inserts, updates, deletes, and DDL changes in near real-time
- **Fast Initial Loads** - Parallel copy of existing data with automatic follow-up continuous replication
- **Powerful Transformations** - Filter and transform data on-the-fly ([see rules](pkg/rules/README.md))
- **Flexible Routing** - Route to different tables and remap columns ([see routing](pkg/routing/README.md))
- **Production Ready** - Supports resumable streaming, DDL tracking, and more

## Common Use Cases

- Real-time data replication between PostgreSQL databases
- ETL pipelines with data transformation
- Data re-routing, masking and filtering
- Database migration with zero downtime
- Event streaming from PostgreSQL

[View detailed examples →](internal/examples/README.md)

## Quick Start

### Prerequisites

- Docker
- PostgreSQL database with `wal_level=logical`

### 1. Install

```shell
docker pull pgflo/pg_flo:latest
```

### 2. Configure

Choose one:

- Environment variables
- YAML configuration file ([example](internal/pg-flo.yaml))
- CLI flags

### 3. Run

```shell
# Start NATS server
docker run -d --name pg_flo_nats \
  --network host \
  -v /path/to/nats-server.conf:/etc/nats/nats-server.conf \
  nats:latest \
  -c /etc/nats/nats-server.conf

# Start replicator (using config file)
docker run -d --name pg_flo_replicator \
  --network host \
  -v /path/to/config.yaml:/etc/pg_flo/config.yaml \
  pgflo/pg_flo:latest \
  replicator --config /etc/pg_flo/config.yaml

# Start worker
docker run -d --name pg_flo_worker \
  --network host \
  -v /path/to/config.yaml:/etc/pg_flo/config.yaml \
  pgflo/pg_flo:latest \
  worker postgres --config /etc/pg_flo/config.yaml
```

#### Example Configuration (config.yaml)

```yaml
# Replicator settings
host: "localhost"
port: 5432
dbname: "myapp"
user: "replicator"
password: "secret"
group: "users"
tables:
  - "users"

# Worker settings (postgres sink)
target-host: "dest-db"
target-dbname: "myapp"
target-user: "writer"
target-password: "secret"

# Common settings
nats-url: "nats://localhost:4222"
```

[View full configuration options →](internal/pg-flo.yaml)

## Core Concepts

### Architecture

pg_flo uses two main components:

- **Replicator**: Captures PostgreSQL changes via logical replication
- **Worker**: Processes and routes changes through NATS

[Learn how it works →](internal/how-it-works.md)

### Groups

Groups are used to:

- Identify replication processes
- Isolate replication slots and publications
- Run multiple instances on same database
- Maintain state for resumability
- Enable parallel processing

```shell
# Example: Separate groups for different tables
pg_flo replicator --group users_orders --tables users,orders

pg_flo replicator --group products --tables products
```

### Streaming Modes

1. **Stream Only** (default)
   - Real-time streaming of changes

```shell
pg_flo replicator --stream
```

2. **Copy Only**
   - One-time parallel copy of existing data

```shell
pg_flo replicator --copy --max-copy-workers-per-table 4
```

3. **Copy and Stream**
   - Initial parallel copy followed by continuous streaming

```shell
pg_flo replicator --copy-and-stream --max-copy-workers-per-table 4
```

### Destinations

- **stdout**: Console output
- **file**: File writing
- **postgres**: Database replication
- **webhook**: HTTP endpoints

[View destination details →](pkg/sinks/README.md)

## Advanced Features

### Message Routing

Routing configuration is defined in a separate YAML file:

```yaml
# routing.yaml
users:
  source_table: users
  destination_table: customers
  column_mappings:
    - source: id
      destination: customer_id
```

```shell
# Apply routing configuration
pg_flo worker postgres --routing-config /path/to/routing.yaml
```

[Learn about routing →](pkg/routing/README.md)

### Transformation Rules

Rules are defined in a separate YAML file:

```yaml
# rules.yaml
tables:
    users:
      - type: exclude_column
        columns: password
      - type: exclude_column
        columns: ssn
      - type: transform
        column: email
        parameters:
          type: mask
          mask_char: "*"
```

```shell
# Apply transformation rules
pg_flo worker file --rules-config /path/to/rules.yaml
```

[View transformation options →](pkg/rules/README.md)

### Combined Example

```shell
pg_flo worker postgres --config /etc/pg_flo/config.yaml --routing-config routing.yaml --rules-config rules.yaml
```

## Scaling Guide

Best practices:

- Run one worker per group
- Use groups to replicate different tables independently
- Scale horizontally using multiple groups

Example scaling setup:

```shell
# Group: sales
pg_flo replicator --group sales --tables sales
pg_flo worker postgres --group sales

# Group: inventory
pg_flo replicator --group inventory --tables inventory
pg_flo worker postgres --group inventory
```

## Limits and Considerations

- NATS message size: 8MB (configurable)
- One worker per group recommended
- PostgreSQL logical replication prerequisites required
- Tables must have one of the following for replication:
  - Primary key
  - Unique constraint with `NOT NULL` columns
  - `REPLICA IDENTITY FULL` set

Example table configurations:

```sql
-- Using primary key (recommended)
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email TEXT,
  name TEXT
);

-- Using unique constraint
CREATE TABLE orders (
  order_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  data JSONB,
  CONSTRAINT orders_unique UNIQUE (order_id, customer_id)
);
ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_unique;

-- Using all columns (higher overhead in terms of performance)
CREATE TABLE audit_logs (
  id SERIAL,
  action TEXT,
  data JSONB
);
ALTER TABLE audit_logs REPLICA IDENTITY FULL;
```

## Development

```shell
make build
make test
make lint

# E2E tests
./internal/scripts/e2e_local.sh
```

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## License

Apache License 2.0. [View license →](LICENSE)
