-- Drop and recreate foreign table for datatype testing
DROP FOREIGN TABLE IF EXISTS @PSCHEMANAME.write_datatypes_test;

-- First, ensure ScyllaDB table exists
-- Note: This would normally be in a ScyllaDB test file, but included here for completeness

-- Create foreign table with various data types
CREATE FOREIGN TABLE @PSCHEMANAME.write_datatypes_test (
    id INT,
    int_val INT,
    bigint_val BIGINT,
    float_val FLOAT,
    varchar_val VARCHAR(100),
    datetime_val TIMESTAMP
)
SERVER scylla_svr
OPTIONS (schema_name '@MSCHEMANAME', table_name 'write_datatypes_test');

-- Note: The ScyllaDB side table should be created separately
-- This test assumes the table exists on ScyllaDB side
