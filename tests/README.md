# Testing scripts

Testing should follow that workflow :

  * First build a ScyllaDB cluster
    1. Create the cluster (local, container, VM or cloud)
    2. Create a testing keyspace with a proper user and access privileges
    3. Run `scylla-tests.py` against that cluster.
  * Next, build a PostgreSQL Server
    1. Create the server (on your machine, with docker or VM)
    2. Compile and install `scylla_fdw` extension
    3. Create a testing database and schema with proper user and access privilege
    4. On that database you'll have first to install `scylla_fdw`: `CREATE EXTENSION scylla_fdw;`
    5. You can run `postgresql_test.py`

# Debugging

It may be interesting to build a full setup for debugging purpose and use tests to check if anythong regresses.
For this, you can use `--debugging` parameter at `postgresql-tests.py` launch time.

The test program will stop just after connection creation and give you the backend PID used for testing. This will allow you to connect gdb in another shell session (`gdb --pid=<PID>`). Once connected with gdb, just put breakpoints where you need and run `cont`. Then you can press any key in the test shell script to start testing.

Also, in case of test failure, `--debugging` will allow to define quite precisely where the script crashed using psycopg2 `Diagnostics` class information and will give the corresponding SQL injected in PostgreSQL.

## Command-Line Options

The test framework supports the following command-line options to control debugging and diagnostic output:

### --postgres_min_messages

Controls the PostgreSQL `client_min_messages` setting for all test connections. This determines which message levels are sent to the client.

- **Supported values**: `DEBUG5`, `DEBUG4`, `DEBUG3`, `DEBUG2`, `DEBUG1`, `LOG`, `NOTICE`, `WARNING`, `ERROR`
- **Default**: `NOTICE` (standard informational messages)
- **Example**: `--postgres_min_messages DEBUG1` for highly verbose output
- **Note**: Only affects PostgreSQL test connections, not ScyllaDB connections

Lower numbered DEBUG levels provide more detailed output:
- `DEBUG5`: Maximum verbosity (includes all debug messages)
- `DEBUG3`: Very verbose (good for detailed troubleshooting)
- `DEBUG1`: Highly verbose (includes query planning details)
- `LOG`: Server operational messages
- `NOTICE`: User-facing informational messages
- `WARNING`: Warnings only
- `ERROR`: Errors only

### Usage Example

```bash
# Enable maximum debugging output
./tests/postgresql-tests.py \
  --postgres_server localhost \
  --postgres_port 5432 \
  --postgres_database testdb \
  --postgres_schema pg_tests \
  --postgres_username testuser \
  --postgres_password testpass \
  --scylla_host scylla.example.com \
  --scylla_port 9042 \
  --scylla_keyspace scylla_fdw_tests \
  --scylla_username scyllauser \
  --scylla_password scyllapass \
  --postgres_min_messages DEBUG1
```

# Adding or modifying tests

There are two folders where tests are added:

* `tests/mssql` contains the tests to interact with a MSSQL server using `mssql-tests.py`. Such tests are, normally, used to create stuff required for the PostgreSQL test themselves.
* `tests/postgresql` contains the test to interact with a PostgreSQL server using `postgresql-tests.py`. Such tests are, normally, used to test the `tds_fdw` functionalities.
scylla` contains the tests to interact with a ScyllaDB cluster using `scylla-tests.py`. Such tests are, normally, used to create stuff required for the PostgreSQL test themselves.
* `tests/postgresql` contains the test to interact with a PostgreSQL server using `postgresql-tests.py`. Such tests are, normally, used to test the `scylla

```
XXX_description
```

For example: `000_my_test.json` and `000_my_test.sql`.

**Rule1:** `XXX` is used to provide an order to the scripts.

**Rule2:** If a script creates an item, or adds a row, it must assume that such item or row exists already, and handle it (for example dropping the table before creating it)

## The JSON file

Always has the following format:

```
{
    "test_desc" : "<My description>",
    "server" : {
        "version" : {
            "min" : "<Min.Required.Ver>",
            "max" : "<Max.Supported.Ver>"
        }
    }
}
```

* `test_desc` can be any arbitrary string describing the test.
* `min` and `max` are version the for mat `X.W[.Y[.Z]]` for MSSQL and PostgreSQL respectively.
  * `min` is mandatory, as minimum `7.0.623` for MSSQL (MSSQL 7.0 RTM) and `9.2.0` for PostgreSQL.
  * `max` is also mandatory, but it can be an empty string if the test supports up to the most recent MSSQL or PostgreSQL version.

You can check the list of versions for [MSSQL](https://sqlserverbuilds.blogspot.com/) (format `X.Y.Z` and `X.Y.W.Z`), [PostgreSQL](https://www.postgresql.org/docs/release/) (formats `X.Y.Z` and `X.Y`), to adjust the `min` and `max` values as needed.

To validate the JSON file, you can use the script `validate-test-json`.

## The SQL file

It is a regular SQL file for one or more queries for either MSSQL or PostgreSQL.

There are several variables that can be used and will be placed by the testing scripts.

For the MSSQL scripts, the values come from the `mssql-tests.py` parameters:

* `@SCHEMANAME`: The MSSQL schema name.

For the PostgreSQL scripts the values come from the `postgresql-tests.py` parameters:

* `@PSCHEMANAME`: The PostgreSQL schema name
* `@PUSER`: The PostgreSQL user
* `@MSERVER`: The MSSQL Server
* `@MPORT`: The MSSQL port
* `@MUSER`: The MSSQL user
* `@MPASSWORD`: The MSSQL password
* `@MDATABASE`: The MSSQL database
* `@MSCHEMANAME`: The MSSQL schema name
* `@TDSVERSION`: The TDS version
