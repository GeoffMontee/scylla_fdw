# Write Operations Test Suite - Quick Start Guide

## What This Is

A comprehensive test suite for validating INSERT/UPDATE/DELETE operations in scylla_fdw. This suite ensures that the Foreign Data Wrapper correctly executes write operations against ScyllaDB clusters.

## Quick Start (TL;DR)

```bash
# 1. Set environment variables
export SCYLLA_PASSWORD="your_scylla_password"
export PG_PASSWORD="your_pg_password"

# 2. Navigate to tests directory
cd /path/to/scylla_fdw/tests

# 3. Run ScyllaDB setup tests
python scylla-tests.py --host scylla_host --port 9042 \
  --keyspace test_keyspace --username scylla_user \
  --password "$SCYLLA_PASSWORD"

# 4. Run PostgreSQL tests
python postgresql-tests.py --postgres_server localhost \
  --postgres_port 5432 --postgres_database testdb \
  --postgres_schema test_schema --postgres_username pguser \
  --postgres_password "$PG_PASSWORD" \
  --scylla_host scylla_host --scylla_port 9042 \
  --scylla_keyspace test_keyspace \
  --scylla_username scylla_user \
  --scylla_password "$SCYLLA_PASSWORD"
```

## What Was Created

### 📋 Test Files Summary
- **28 total files**: 14 test pairs (JSON + CQL/SQL)
- **3 ScyllaDB setup tests**: Table creation for write operations
- **11 PostgreSQL write tests**: Complete coverage of INSERT/UPDATE/DELETE
- **Coverage**: Basic operations, NULL handling, data types, error conditions, workflows

### 🎯 Test Coverage

| Operation | Tests | Description |
|---scylla-tests.py                          # ScyllaDB test runner
├── postgresql-tests.py                      # PostgreSQL test runner
└── tests/
    ├── WRITE_TESTS_README.md               # Detailed documentation
    ├── WRITE_TESTS_SUMMARY.md              # Implementation summary
    ├── scyllaing | 1 | Constraint violations, non-existent rows |

## File Structure

```
tests/
├── scylla-tests.py                          # ScyllaDB test runner
├── postgresql-tests.py                      # PostgreSQL test runner
└── tests/
    ├── WRITE_TESTS_README.md               # Detailed documentation
    ├── WRITE_TESTS_SUMMARY.md              # Implementation summary
    ├── WRITE_TESTS_QUICKSTART.md           # Quick start guide
    ├── scylla/
    │   ├── 050_create_write_test_table.*   # NEW: Basic write test table
    │   ├── 051_create_write_datatypes_table.* # NEW: Data types test table
    │   └── 052_create_concurrent_safe_table.* # NEW: Concurrent-safe pattern
    └── postgresql/
        ├── 050_write_insert_single.*        # NEW: Single INSERT
        ├── 051_write_insert_multiple.*      # NEW: Multiple INSERTs
        ├── 052_write_update_single.*        # NEW: Single UPDATE
        ├── 053_write_update_multiple.*      # NEW: Multiple UPDATEs
        ├── 054_write_delete_single.*        # NEW: Single DELETE
        ├── 055_write_delete_multiple.*      # NEW: Multiple DELETEs
        ├── 056_write_null_handling.*        # NEW: NULL value testing
        ├── 057_write_datatypes.*            # NEW: Data types setup
        ├── 058_write_datatypes_operations.* # NEW: Data types operations
        ├── 059_write_complete_workflow.*    # NEW: Full workflow test
        └── 060_write_error_handling.*       # NEW: Error scenarios
```

## Manual Test Execution

### Option 1: Using the convenience script
```bash
cd /path/to/scylla_fdw/tests

# Set passwords
export SCYLLA_PASSWORD="your_password"
export PG_PASSWORD="your_password"

# Override defaults if needed
export SCYLLA_HOST="your_scylla_host"
export SCYLLA_PORT="9042"
export PG_SERVER="localhost"

# Run tests
./run-write-tests.sh
```

### Option 2: Manual step-by-step
```bash
cd /path/to/scylla_fdw/tests

# Step 1: Create ScyllaDB test objects
python scylla-tests.py \
    --host your_scylla_host \
    --port 9042 \
    --keyspace scylla_fdw_tests \
    --username scyllauser \
    --password your_password

# Step 2: Run PostgreSQL FDW tests
python postgresql-tests.py \
    --postgres_server localhost \
    --postgres_port 5432 \
    --postgres_database testdb \
    --postgres_schema scylla_fdw_pg_tests \
    --postgres_username pguser \
    --postgres_password your_password \
    --scylla_host your_scylla_host \
    --scylla_port 9042 \
    --scylla_keyspace scylla_fdw_tests \
    --scylla_username scyllauser \
    --scylla_password your_password
```

## Expected Output

### Successful Test Run
```
[INFO] Step 1: Running ScyllaDB setup tests...
✓ Test 050_create_write_test_table passed
✓ Test 051_create_write_datatypes_table passed
✓ Test 052_create_concurrent_safe_table passed

[INFO] Step 2: Running PostgreSQL FDW write operation tests...
✓ Test 050_write_insert_single passed
✓ Test 051_write_insert_multiple passed
✓ Test 052_write_update_single passed
✓ Test 053_write_update_multiple passed
✓ Test 054_write_delete_single passed
✓ Test 055_write_delete_multiple passed
✓ Test 056_write_null_handling passed
✓ Test 057_write_datatypes passed
✓ Test 058_write_datatypes_operations passed
✓ Test 059_write_complete_workflow passed
✓ Test 060_write_error_handling passed

[INFO] All write operation tests PASSED! ✓

Total: 14 tests
Passed: 14
Failed: 0
```

### Test Validation Queries
Each test includes validation queries that return boolean results:
```sql
-- Example from a test:
SELECT (COUNT(*) = 1) AS insert_success 
FROM schema.table 
WHERE conditions;

-- Expected result:
 insert_success 
----------------
 t              -- 't' means test passed
(1 row)
```

## Concurrent Execution

### Current Approach
Tests use **schema isolation** via `@SCHEMANAME` placeholder. Each test run can specify a unique schema:

```bash
# Run 1 (Terminal 1):
export SCYLLA_KEYSPACE="scylla_fdw_tests_run1"
export PG_SCHEMA="scylla_fdw_pg_tests_run1"
./run-write-tests.sh

# Run 2 (Terminal 2):
export SCYLLA_KEYSPACE="scylla_fdw_tests_run2"
export PG_SCHEMA="scylla_fdw_pg_tests_run2"
./run-write-tests.sh
```

### Future Enhancement
See `052_create_concurrent_safe_table.sql` for timestamp-based table naming pattern that enables true concurrent execution within the same schema.

## Debugging Failed Tests

### Enable Debug Mode
```bash
./postgresql-tests.py --debugging \
    [... other options ...]
```

This will:
1. Print PostgreSQL backend PID
2. Pause before tests (for gdb attachment)
3. Show detailed SQL on failure
4. Display PostgreSQL logs after completion

### Attach GDB
```bash
# In another terminal after seeing PID:
gdb --pid=<PID>

# In gdb:
(gdb) break scyllaExecForeignInsert
(gdb) break scyllaExecForeignUpdate
(gdb) break scyllaExecForeignDelete
(gdb) cont

# Return to test terminal and press Enter to continue
```

## Test Design Philosophy

### ✅ What Makes These Tests Good

1. **Idempotent**: Can be run multiple times safely (DROP IF EXISTS)
2. **Self-Verifying**: Boolean `*_success` columns validate results
3. **Isolated**: ID ranges prevent collisions between test phases
4. **Comprehensive**: Cover normal operations, edge cases, and errors
5. **Well-Documented**: Each test has clear purpose and validation
6. **Pattern-Following**: Consistent with existing test framework

### 🎯 Test Pattern
```sql
-- 1. Verify initial state
SELECT COUNT(*) FROM table;

-- 2. Execute operation
INSERT/UPDATE/DELETE ...

-- 3. Verify result
SELECT COUNT(*) FROM table WHERE condition;

-- 4. Boolean validation
SELECT (condition) AS test_success FROM table;

-- 5. Cleanup (if needed)
DELETE FROM table WHERE test_data_condition;
```

## Known Limitations

These tests work within the current implementation constraints:

| Limitation | Impact | Test Approach |
|------------|--------|---------------|
| No RETURNING clause | Can't get auto-generated values | Use SELECT after INSERT |
| Primary key required | UPDATE/DELETE need PK in WHERE | All test tables have PKs |
| Inline values | No prepared statements | Tests use safe literal values |
| Individual operations | No batch support | Multiple individual statements |

## What's Next?

### Immediate Actions
1. ✅ Validate JSON files: `./validate-test-json --path tests/tests/scylla/*.json`
2. ✅ Run tests against your test environment
3. ✅ Review output for any failures
4. ✅ Report any issues found

### Future Enhancements
1. **Transaction tests**: BEGIN/COMMIT/ROLLBACK scenarios
2. **Performance tests**: Bulk operation benchmarks
3. **Stress tests**: High-volume concurrent writes
4. **More error conditions**: All constraint types, referential integrity
5. **RETURNING support** (when implemented in scylla_fdw)

## Questions & Answers

**Q: Are these tests safe to run on production?**  
A: **No!** These are write operations that modify data. Use a dedicated test environment.

**Q: Do I need to cleanup after running tests?**  
A: Tests are designed to be idempotent. Re-running drops and recreates objects.

**Q: Can I run these tests on ScyllaDB Cloud?**  
A: Yes! Use appropriate host and authentication parameters with scylla-tests.py

**Q: What if a test fails?**  
A: Check the `*_success` column in the output. Use `--debugging` flag for details.

**Q: Can I add my own tests?**  
A: Absolutely! Follow the pattern in existing tests. See WRITE_TESTS_README.md for guidelines.

## Success Criteria

After running tests, you should see:
- ✅ All JSON files validated
- ✅ All ScyllaDB tables created
- ✅ All PostgreSQL foreign tables created
- ✅ All INSERT operations successful
- ✅ All UPDATE operations successful
- ✅ All DELETE operations successful
- ✅ All validation queries return `t` (true)
- ✅ No unexpected errors in output

## Documentation

- **WRITE_TESTS_README.md**: Detailed technical documentation
- **WRITE_TESTS_SUMMARY.md**: Implementation summary and statistics
- **This file**: Quick start and practical guide

## Contact & Support

For issues or questions about these tests:
1. Check the documentation files
2. Review test output with `--debugging` flag
3. Check PostgreSQL logs
4. Review ScyllaDB logs
5. Check scylla_fdw GitHub issues

---

**Created**: 2024 (date of implementation)  
**Purpose**: Validate write operations in scylla_fdw  
**Status**: Production-ready test suite  
**Maintainer**: scylla_fdw project contributors
