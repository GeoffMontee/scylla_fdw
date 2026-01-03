# Copilot Instructions for scylla_fdw

## Project Overview

This is a **PostgreSQL Foreign Data Wrapper (FDW)** for ScyllaDB, enabling PostgreSQL to query and modify ScyllaDB/Cassandra tables. It wraps the ScyllaDB cpp-rs-driver (C++ library) with a C interface for PostgreSQL's FDW API.

## Architecture

### C/C++ Bridge Pattern
- **C++ layer** ([scylla_connection.cpp](../scylla_connection.cpp)): Wraps the cpp-rs-driver using `extern "C"` to expose C-compatible functions
- **C layer**: PostgreSQL FDW callbacks in [scylla_fdw.c](../scylla_fdw.c), [scylla_fdw_modify.c](../scylla_fdw_modify.c), [scylla_deparse.c](../scylla_deparse.c), etc.
- All C++ code must use `extern "C"` blocks to be callable from C code

### Module Organization
- **scylla_fdw.c**: Main FDW handler, query planning (GetForeignRelSize, GetForeignPaths, GetForeignPlan, scan execution)
- **scylla_fdw_modify.c**: INSERT/UPDATE/DELETE operations, EXPLAIN support, IMPORT FOREIGN SCHEMA
- **scylla_deparse.c**: Converts PostgreSQL query nodes to CQL queries, WHERE clause pushdown logic
- **scylla_typemap.c**: Type conversion between PostgreSQL and CQL types
- **scylla_connection.cpp**: C++ wrapper for driver connection, execution, result handling
- **scylla_fdw_helper.c**: Utility functions for option parsing, validation

### Build System
- Uses PostgreSQL's PGXS build system (see [Makefile](../Makefile))
- Requires explicit C++17 compilation for `.cpp` files: `$(CXX) -std=c++17 -fPIC`
- Links against `-lscylla-cpp-driver -lstdc++`
- Default driver paths: `/usr/local/include` and `/usr/local/lib/aarch64-linux-gnu`
- Override with `SCYLLA_DRIVER_INCLUDE` and `SCYLLA_DRIVER_LIB` environment variables

## Critical Build Commands

```bash
# Standard build
make USE_PGXS=1
sudo make USE_PGXS=1 install

# Custom driver location
make USE_PGXS=1 SCYLLA_DRIVER_INCLUDE=/path/to/include SCYLLA_DRIVER_LIB=/path/to/lib

# Development helpers
make format          # clang-format on all source files
make check-syntax    # Syntax validation without linking
```

## ScyllaDB/Cassandra Specifics

### Query Pushdown Constraints
- **Partition key equality required**: ScyllaDB queries typically require `WHERE partition_key = value`
- **Clustering column ranges**: Can use `<`, `>`, `<=`, `>=` only on clustering columns after partition key
- See `deparseExpr()` in [scylla_deparse.c](../scylla_deparse.c#L300) for pushdown logic
- Non-pushable conditions stored in `local_conds` and evaluated by PostgreSQL

### Type Mapping (scylla_typemap.c)
- **UUID**: PostgreSQL `uuid` ↔ CQL `uuid`/`timeuuid`
- **Timestamps**: PostgreSQL `timestamp with time zone` ↔ CQL `timestamp` (millisecond precision)
- **Integers**: `tinyint` (CQL) → `smallint` (PG), `bigint` maps to `bigint` or `counter`
- **Text**: Both `text` and `varchar` map to CQL `text`/`varchar`/`ascii`
- **Blobs**: PostgreSQL `bytea` ↔ CQL `blob`

### Foreign Table Requirements
- **Options** (see [scylla_fdw.h](../scylla_fdw.h#L47)):
  - `keyspace` and `table` are required
  - `primary_key`: Required for UPDATE/DELETE (comma-separated list)
  - `clustering_key`: Optional, for metadata only
- **Example**:
  ```sql
  CREATE FOREIGN TABLE users (...)
  SERVER scylla_server
  OPTIONS (keyspace 'my_keyspace', table 'users', primary_key 'user_id');
  ```

## Development Patterns

### Adding New Type Support
1. Update `scylla_convert_pg_to_cql()` in [scylla_typemap.c](../scylla_typemap.c) for PostgreSQL → CQL conversion
2. Update `scylla_convert_cql_to_pg()` for CQL → PostgreSQL conversion
3. Add type validation in `scyllaCheckTypeMapping()` if needed
4. Test with both SELECT and INSERT/UPDATE operations

### Adding FDW Options
1. Define constant in [scylla_fdw.h](../scylla_fdw.h#L47) (e.g., `#define OPT_NEW_OPTION "new_option"`)
2. Add to `scylla_fdw_options[]` array in [scylla_fdw.c](../scylla_fdw.c#L137) with appropriate context (ForeignServerRelationId, UserMappingRelationId, or ForeignTableRelationId)
3. Extract value in `scylla_get_options()` and use in connection/query logic
4. Document in [README.md](../README.md#L76) options table

### Connection Management
- Connections are managed per-query in `scyllaBeginForeignScan()` / `scyllaBeginForeignModify()`
- Released in `scyllaEndForeignScan()` / `scyllaEndForeignModify()`
- Use `scylla_connect()` (C++ wrapper) to establish connections with host, port, auth, SSL options
- Always call `scylla_disconnect()` to free resources

## Common Pitfalls
1. **C++ code in extern blocks**: All C++ wrapper code in scylla_connection.cpp must be wrapped in `extern "C" { }` blocks
2. **Missing PGXS variable**: Always use `USE_PGXS=1` when building (not as a contrib module)
3. **Driver path issues**: On non-standard installations, export `SCYLLA_DRIVER_INCLUDE` and `SCYLLA_DRIVER_LIB` before `make`
4. **Partition key requirement**: Queries without partition key equality will cause full table scans (inefficient in ScyllaDB)
5. **Primary key for modifications**: UPDATE/DELETE fail silently or incorrectly if `primary_key` option is not set on the foreign table
6. **PostgreSQL version compatibility**: The `create_foreignscan_path()` signature changed in PG17 and PG18. Code uses `#if PG_VERSION_NUM >= 180000` for version-specific implementations

## PostgreSQL Version Compatibility

The FDW supports PostgreSQL 9.6 through 18+. Key API changes to be aware of:

- **PostgreSQL 18**: 
  - `create_foreignscan_path()` signature changed:
    - Added `disabled_nodes` parameter (5th position after `rows`)
    - Still has `total_cost` parameter (unlike what some docs suggested)
    - Final signature: `(root, rel, pathtarget, rows, disabled_nodes, startup_cost, total_cost, pathkeys, required_outer, fdw_outerpath, fdw_restrictinfo, fdw_private)` - 12 parameters
  - **EXPLAIN hooks removed**: `ExplainForeignScan` and `ExplainForeignModify` callbacks were removed from the FDW API. EXPLAIN now uses a different mechanism internally.
- **PostgreSQL 17**: 
  - Added `fdw_restrictinfo` and `fdw_private` parameters to `create_foreignscan_path()`
  - Signature: `(root, rel, pathtarget, rows, startup_cost, total_cost, pathkeys, lateral_relids, fdw_outerpath, fdw_restrictinfo, fdw_private)` - 11 parameters
- **PostgreSQL 9.6+**: Modern FDW API with path-based planning

When adding new features that interact with PostgreSQL APIs, check for version-specific changes and use `#if PG_VERSION_NUM` conditionals.

## Extension Installation

After building, users must:
1. Create the extension: `CREATE EXTENSION scylla_fdw;`
2. Create a foreign server with ScyllaDB connection details
3. Create user mapping for authentication
4. Create foreign tables or use `IMPORT FOREIGN SCHEMA`

## Current Limitations & Missing Features

### Not Yet Implemented
- **Collections**: CQL list, set, and map types are not supported (type constants defined but no conversion logic)
- **User-Defined Types (UDTs)**: No support for custom types (CASS_VALUE_TYPE_UDT defined but unused)
- **Materialized Views**: No special handling for MVs (treated as regular tables if queried)
- **Vector Search**: No support for ScyllaDB Cloud ANN vector search queries
- **Rack/Datacenter Awareness**: No options to configure local datacenter or rack awareness for query routing
- **Token-Aware Routing**: The driver supports it internally, but no FDW-level configuration exposed
- **Connection Pooling**: FDW creates per-query connections; no session reuse across queries
- **BATCH Statements**: No support for batching multiple INSERT/UPDATE/DELETE operations
- **Join Pushdown**: Stubbed in code but not functional (ScyllaDB doesn't support JOINs natively)

### Implementation Priorities (by complexity)
1. **Connection pooling** (reuse sessions across queries)
2. **Materialized views** (query system_schema to detect MVs, handle read-only semantics)
3. **Collections** (add type conversion for list/set/map to PostgreSQL arrays/JSON)
4. **Rack awareness** (add `local_dc` server option, configure load balancing policy)
5. **User-Defined Types** (map CQL UDTs to PostgreSQL composite types)
6. **Vector search** (add support for ANN queries via CQL extensions)

