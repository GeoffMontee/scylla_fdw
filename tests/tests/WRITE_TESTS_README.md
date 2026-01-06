# Write Operations Test Suite

## Overview

This directory contains tests for the INSERT/UPDATE/DELETE operations in `scylla_fdw`. These tests verify that the FDW correctly executes write operations against ScyllaDB databases.

## Test Structure

Write operation tests are numbered starting at **050** to distinguish them from read-only tests (000-049).

### ScyllaDB Setup Tests (050-051)
- **050_create_write_test_table**: Creates base table for write operation testing
- **051_create_write_datatypes_table**: Creates table with various data types

### PostgreSQL Write Tests (050-058)

#### Basic Operations
- **050_write_insert_single**: Single row INSERT operation
- **051_write_insert_multiple**: Multiple individual INSERT operations
- **052_write_update_single**: Single row UPDATE operation
- **053_write_update_multiple**: Multi-row UPDATE operation
- **054_write_delete_single**: Single row DELETE operation
- **055_write_delete_multiple**: Multi-row DELETE operation

#### Edge Cases
- **056_write_null_handling**: NULL value handling in INSERT/UPDATE
- **057_write_datatypes**: Foreign table setup for data type testing
- **058_write_datatypes_operations**: INSERT/UPDATE/DELETE with various data types

## Design Considerations

### Concurrent Execution Safety

The current test design uses schema isolation via `@SCHEMANAME` placeholder to minimize collision risk during concurrent test execution. Each test run can use a unique schema name.

**Note**: For true concurrent safety with multiple simultaneous test runs, consider:
1. Using timestamp-based table names (e.g., `write_test_20240115_120000_abc123`)
2. Implementing cleanup in test teardown
3. Using temporary tables where appropriate

### Test Isolation

- Each test assumes a clean starting state via `DROP IF EXISTS`
- Tests verify state before and after operations
- Cleanup is performed within tests where possible
- Tests use specific ID ranges (100+, 200+) to avoid conflicts with initial data

## Running Write Tests

### Prerequisites
1. ScyllaDB cluster with test keyspace
2. PostgreSQL instance with `scylla_fdw` extension installed
3. Python 3 with `scylla-driver` and `psycopg2` libraries

### Execution

```bash
# 1. Create ScyllaDB test objects
python scylla-tests.py \
    --host <scylla_host> \
    --port 9042 \
    --keyspace scylla_fdw_tests \
    --username <user> \
    --password <pass>

# 2. Run PostgreSQL FDW tests
python postgresql-tests.py \
    --postgres_server localhost \
    --postgres_port 5432 \
    --postgres_database testdb \
    --postgres_schema scylla_fdw_pg_tests \
    --postgres_username pguser \
    --postgres_password <pass> \
    --scylla_host <scylla_host> \
    --scylla_port 9042 \
    --scylla_keyspace scylla_fdw_tests \
    --scylla_username <user> \
    --scylla_password <pass>
```

## Known Limitations

Based on the write operations implementation:

1. **No RETURNING clause support**: Cannot retrieve values generated on insert
2. **Primary key requirement**: UPDATE/DELETE require primary key in WHERE clause for optimal performance
3. **Transaction handling**: Relies on PostgreSQL's transaction management
4. **Individual operations**: Each INSERT/UPDATE/DELETE is executed individually

## Test Validation

Each test includes validation queries that return boolean results:
- `*_success` columns should return `t` (true) for passing tests
- Count queries verify expected number of rows affected
- State verification queries confirm data integrity

### Example Test Output

```sql
-- Expected successful test output:
 insert_success 
----------------
 t
(1 row)

 update_success 
----------------
 t
(1 row)

 delete_success 
----------------
 t
(1 row)
```

## Adding New Write Tests

To add new write operation tests:

1. **Create ScyllaDB setup file** (if new table needed):
   - `tests/tests/scylla/0XX_create_your_table.json`
   - `tests/tests/scylla/0XX_create_your_table.cql`

2. **Create PostgreSQL test file**:
   - `tests/tests/postgresql/0XX_write_your_test.json`
   - `tests/tests/postgresql/0XX_write_your_test.sql`

3. **Follow existing patterns**:
   - DROP IF EXISTS for idempotency
   - Verify state before operations
   - Execute write operation
   - Verify state after operations
   - Include validation query with `*_success` boolean result
   - Clean up test data if appropriate

4. **Consider edge cases**:
   - NULL values
   - Empty strings
   - Maximum/minimum values for data types
   - Special characters in strings
   - Date/time boundary conditions
   - Concurrent modification scenarios (if applicable)

## Debugging Failed Tests

Use the `--debugging` flag with postgresql-tests.py:

```bash
./tests/postgresql-tests.py --debugging [other options]
```

This will:
1. Print the PostgreSQL backend PID
2. Pause before running tests (allowing gdb attachment)
3. Display detailed SQL query information on failure
4. Show PostgreSQL logs after test completion

## Future Enhancements

Potential improvements for write operation testing:

1. **Randomized table names**: Implement UUID/timestamp-based table naming for true concurrent execution safety
2. **Transaction testing**: Explicit BEGIN/COMMIT/ROLLBACK scenarios
3. **Error condition testing**: Constraint violations, type mismatches, referential integrity
4. **Performance testing**: Bulk insert performance, update performance with various WHERE conditions
5. **Concurrency testing**: Multiple simultaneous writers, read-while-write scenarios
6. **RETURNING clause support** (when implemented): Verify returned values match expected
