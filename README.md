# scylla_fdw - PostgreSQL Foreign Data Wrapper for ScyllaDB

A PostgreSQL extension that provides a Foreign Data Wrapper (FDW) for ScyllaDB,
allowing PostgreSQL to query ScyllaDB tables as if they were local PostgreSQL tables.

## Features

- **Full CRUD Support**: SELECT, INSERT, UPDATE, and DELETE operations
- **WHERE Clause Pushdown**: Pushes compatible WHERE conditions to ScyllaDB
- **Type Conversion**: Automatic type conversion between PostgreSQL and CQL types
- **Connection Pooling**: Efficient connection management
- **SSL Support**: Secure connections to ScyllaDB clusters
- **Import Foreign Schema**: Automatically create foreign table definitions

## Requirements

- PostgreSQL 14 or later
- ScyllaDB cpp-rs-driver (https://github.com/scylladb/cpp-rs-driver)
- C++ compiler with C++17 support

## Installation

### 1. Install ScyllaDB cpp-rs-driver

The ScyllaDB cpp-rs-driver requires the following build dependencies:
- Rust compiler (rustc and cargo)
- C/C++ compiler (gcc/g++ or clang)
- CMake
- pkg-config
- OpenSSL development libraries
- Git
- libuv

**Install dependencies on Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config libssl-dev git libuv1-dev curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

**Install dependencies on RHEL/Fedora:**
```bash
sudo dnf install -y gcc gcc-c++ cmake pkg-config openssl-devel git libuv-devel
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

Follow the instructions at https://github.com/scylladb/cpp-rs-driver to build and install
the driver. The default installation paths are:
- Headers: `/usr/local/include`
- Libraries: `/usr/local/lib`

### 2. Build the Extension

```bash
cd scylla_fdw
make USE_PGXS=1
sudo make USE_PGXS=1 install
```

If the driver is installed in a non-standard location:

```bash
make USE_PGXS=1 SCYLLA_DRIVER_INCLUDE=/path/to/include SCYLLA_DRIVER_LIB=/path/to/lib
sudo make USE_PGXS=1 install
```

### 3. Create the Extension

```sql
CREATE EXTENSION scylla_fdw;
```

## Usage

### Create a Foreign Server

```sql
CREATE SERVER scylla_server
    FOREIGN DATA WRAPPER scylla_fdw
    OPTIONS (
        host '127.0.0.1',           -- ScyllaDB contact point(s)
        port '9042',                -- CQL native protocol port
        consistency 'local_quorum'  -- Default consistency level
    );
```

### Create User Mapping

```sql
CREATE USER MAPPING FOR current_user
    SERVER scylla_server
    OPTIONS (
        username 'cassandra',
        password 'cassandra'
    );
```

### Create Foreign Tables

```sql
CREATE FOREIGN TABLE users (
    user_id uuid,
    username text,
    email text,
    created_at timestamp with time zone
)
SERVER scylla_server
OPTIONS (
    keyspace 'my_keyspace',
    table 'users',
    primary_key 'user_id'           -- Required for UPDATE/DELETE
);
```

### Import Schema Automatically

```sql
IMPORT FOREIGN SCHEMA my_keyspace
    FROM SERVER scylla_server
    INTO public;
```

## Server Options

| Option | Description | Default |
|--------|-------------|---------|
| `host` | ScyllaDB contact point(s), comma-separated | `127.0.0.1` |
| `port` | CQL native protocol port | `9042` |
| `consistency` | Default consistency level | `local_quorum` |
| `connect_timeout` | Connection timeout in milliseconds | `5000` |
| `request_timeout` | Request timeout in milliseconds | `12000` |
| `ssl` | Enable SSL/TLS | `false` |
| `ssl_cert` | Path to client certificate file | - |
| `ssl_key` | Path to client private key file | - |
| `ssl_ca` | Path to CA certificate file | - |

## User Mapping Options

| Option | Description |
|--------|-------------|
| `username` | ScyllaDB username for authentication |
| `password` | ScyllaDB password for authentication |

## Table Options

| Option | Description |
|--------|-------------|
| `keyspace` | ScyllaDB keyspace name (required) |
| `table` | ScyllaDB table name (required) |
| `primary_key` | Comma-separated list of primary key columns (required for UPDATE/DELETE) |
| `clustering_key` | Comma-separated list of clustering key columns |

## Type Mapping

| PostgreSQL Type | CQL Type |
|-----------------|----------|
| `boolean` | `boolean` |
| `smallint` | `smallint`, `tinyint` |
| `integer` | `int` |
| `bigint` | `bigint`, `counter` |
| `real` | `float` |
| `double precision` | `double` |
| `numeric` | `decimal`, `varint` |
| `text`, `varchar` | `text`, `varchar`, `ascii` |
| `bytea` | `blob` |
| `uuid` | `uuid`, `timeuuid` |
| `timestamp with time zone` | `timestamp` |
| `date` | `date` |
| `time` | `time` |
| `inet` | `inet` |

## WHERE Clause Pushdown

The FDW pushes compatible WHERE conditions to ScyllaDB for efficient querying.
Supported operators:
- Equality (`=`)
- Comparison (`<`, `>`, `<=`, `>=`)
- AND combinations

Note: ScyllaDB requires equality on the partition key for most queries.
Range queries are only efficient on clustering columns.

## Examples

### Basic Queries

```sql
-- Select all rows (caution: full table scan)
SELECT * FROM users;

-- Select by partition key (efficient)
SELECT * FROM users WHERE user_id = '550e8400-e29b-41d4-a716-446655440000';

-- Range query on clustering column
SELECT * FROM users 
WHERE user_id = '550e8400-e29b-41d4-a716-446655440000' 
  AND created_at > '2024-01-01';
```

### Data Modification

```sql
-- Insert
INSERT INTO users (user_id, username, email, created_at)
VALUES (gen_random_uuid(), 'john_doe', 'john@example.com', now());

-- Update (requires primary_key option on foreign table)
UPDATE users 
SET email = 'new@example.com' 
WHERE user_id = '550e8400-e29b-41d4-a716-446655440000';

-- Delete
DELETE FROM users 
WHERE user_id = '550e8400-e29b-41d4-a716-446655440000';
```

### Using EXPLAIN

```sql
EXPLAIN (VERBOSE) SELECT * FROM users WHERE user_id = 'some-uuid';
```

## Migrating from PostgreSQL to ScyllaDB

For a complete migration from PostgreSQL to ScyllaDB, you can use the [postgres-to-scylla-migration](https://github.com/GeoffMontee/postgres-to-scylla-migration) toolkit. This toolkit:

- **Converts PostgreSQL schemas to CQL** - Automatically translates table definitions, data types, and constraints
- **Migrates data** - Efficiently copies data from PostgreSQL to ScyllaDB
- **Handles type mapping** - Properly converts PostgreSQL types to compatible CQL types
- **Supports incremental migration** - Can migrate specific tables or schemas

### Basic Migration Workflow

1. **Install the migration toolkit** (see the [repo documentation](https://github.com/GeoffMontee/postgres-to-scylla-migration))
2. **Convert your schema** to CQL and create tables in ScyllaDB
3. **Migrate data** using the toolkit's data migration tools
4. **Set up scylla_fdw** to access ScyllaDB tables from PostgreSQL
5. **Test and validate** your migrated data

For detailed instructions, examples, and best practices, visit the [migration toolkit repository](https://github.com/GeoffMontee/postgres-to-scylla-migration).

## Limitations

1. **No JOIN Pushdown**: ScyllaDB doesn't support JOINs in CQL, so joins between
   foreign tables are performed locally in PostgreSQL.

2. **Limited WHERE Support**: Only simple conditions can be pushed down. Complex
   expressions, functions, and OR clauses are evaluated locally.

3. **No Aggregate Pushdown**: Aggregations (COUNT, SUM, etc.) are performed locally.

4. **Primary Key Required for Modifications**: UPDATE and DELETE require the
   `primary_key` option to be set.

5. **Collection Types**: Sets, lists, and maps are not yet fully supported.

## Troubleshooting

### Connection Errors

Ensure ScyllaDB is running and accessible:
```bash
cqlsh <host> <port>
```

### Permission Errors

Verify the user has appropriate permissions in ScyllaDB:
```sql
GRANT SELECT, MODIFY ON KEYSPACE my_keyspace TO my_user;
```

### Performance Issues

- Ensure queries use the partition key in WHERE clause
- Use EXPLAIN to see which conditions are pushed down
- Check ScyllaDB query tracing for slow queries

## Building from Source

```bash
git clone https://github.com/your-repo/scylla_fdw.git
cd scylla_fdw
make USE_PGXS=1
make USE_PGXS=1 install
make USE_PGXS=1 installcheck  # Run regression tests
```

## License

This extension is released under the PostgreSQL License.

## Contributing

Contributions are welcome! Please submit issues and pull requests on GitHub.

## Acknowledgments

- ScyllaDB team for the cpp-rs-driver
- PostgreSQL community for the FDW infrastructure
- Contributors to postgres_fdw for inspiration
