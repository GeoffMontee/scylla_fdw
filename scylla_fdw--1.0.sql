/* scylla_fdw/scylla_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scylla_fdw" to load this file. \quit

-- Create the FDW handler function
CREATE FUNCTION scylla_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Create the FDW validator function
CREATE FUNCTION scylla_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Create the foreign data wrapper
CREATE FOREIGN DATA WRAPPER scylla_fdw
    HANDLER scylla_fdw_handler
    VALIDATOR scylla_fdw_validator;

-- Create utility function to get version
CREATE FUNCTION scylla_fdw_version()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Add comment
COMMENT ON FOREIGN DATA WRAPPER scylla_fdw IS 'Foreign data wrapper for ScyllaDB';

/*
 * Usage example:
 *
 * -- Create a server connection to ScyllaDB
 * CREATE SERVER scylla_server
 *     FOREIGN DATA WRAPPER scylla_fdw
 *     OPTIONS (
 *         host '127.0.0.1',
 *         port '9042',
 *         consistency 'local_quorum'
 *     );
 *
 * -- Create user mapping with credentials
 * CREATE USER MAPPING FOR current_user
 *     SERVER scylla_server
 *     OPTIONS (
 *         username 'cassandra',
 *         password 'cassandra'
 *     );
 *
 * -- Create a foreign table
 * CREATE FOREIGN TABLE users (
 *     user_id uuid,
 *     username text,
 *     email text,
 *     created_at timestamp with time zone
 * )
 * SERVER scylla_server
 * OPTIONS (
 *     keyspace 'my_keyspace',
 *     table 'users',
 *     primary_key 'user_id'
 * );
 *
 * -- Query the foreign table
 * SELECT * FROM users WHERE user_id = 'some-uuid';
 *
 * -- Insert into the foreign table
 * INSERT INTO users (user_id, username, email, created_at)
 * VALUES (gen_random_uuid(), 'john_doe', 'john@example.com', now());
 *
 * -- Update a row
 * UPDATE users SET email = 'new@example.com' WHERE user_id = 'some-uuid';
 *
 * -- Delete a row
 * DELETE FROM users WHERE user_id = 'some-uuid';
 *
 * -- Import schema from ScyllaDB
 * IMPORT FOREIGN SCHEMA my_keyspace
 *     FROM SERVER scylla_server
 *     INTO public;
 */
