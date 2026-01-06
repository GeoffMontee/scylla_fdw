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
    5. You can run `postgresql-tests.py`

# Debugging

It may be interesting to build a full setup for debugging purpose and use tests to check if anything regresses.
For this, you can use `--debugging` parameter at `postgresql-tests.py` launch time.

The test program will stop just after connection creation and give you the backend PID used for testing. This will allow you to connect gdb in another shell session (`gdb --pid=<PID>`). Once connected with gdb, just put breakpoints where you need and run `cont`. Then you can press any key in the test shell script to start testing.

Also, in case of test failure, `--debugging` will allow to define quite precisely where the script crashed using psycopg2 `Diagnostics` class information and will give the corresponding SQL injected in PostgreSQL.

## Command-Line Options

### scylla-tests.py Options

- `--host`: ScyllaDB contact point (required)
- `--port`: Native transport port (default: 9042)
- `--keyspace`: Keyspace name to create/use (required)
- `--username`: Authentication username (optional)
- `--password`: Authentication password (optional)
- `--ssl`: Enable SSL/TLS connection (flag)

**Example:**
```bash
# With authentication
python scylla-tests.py \
  --host scylla.example.com \
  --port 9042 \
  --keyspace scylla_fdw_tests \
  --username cassandra \
  --password cassandra

# Without authentication (default ScyllaDB setup)
python scylla-tests.py \
  --host localhost \
  --port 9042 \
  --keyspace scylla_fdw_tests
```

### postgresql-tests.py Options

- `--postgres_server`, `--postgres_port`, `--postgres_database`: PostgreSQL connection details
- `--postgres_schema`: Schema to create for foreign tables
- `--postgres_username`, `--postgres_password`: PostgreSQL credentials
- `--scylla_host`, `--scylla_port`: ScyllaDB connection details
- `--scylla_keyspace`: Keyspace to import as foreign schema
- `--scylla_username`, `--scylla_password`: ScyllaDB credentials (optional)
- `--debugging`: Pause after connection, show queries, print logs on failure
- `--unattended_debugging`: Like debugging but no pause (for CI)
- `--postgres_min_messages`: PostgreSQL log level (DEBUG1-5, LOG, NOTICE, WARNING, ERROR)

#### --postgres_min_messages Details

Controls the PostgreSQL `client_min_messages` setting for all test connections. This determines which message levels are sent to the client.

- **Supported values**: `DEBUG5`, `DEBUG4`, `DEBUG3`, `DEBUG2`, `DEBUG1`, `LOG`, `NOTICE`, `WARNING`, `ERROR`
- **Default**: `NOTICE` (standard informational messages)
- **Note**: Only affects PostgreSQL test connections, not ScyllaDB connections

Lower numbered DEBUG levels provide more detailed output:
- `DEBUG5`: Maximum verbosity (includes all debug messages)
- `DEBUG3`: Very verbose (good for detailed troubleshooting)
- `DEBUG1`: Highly verbose (includes query planning details)
- `LOG`: Server operational messages
- `NOTICE`: User-facing informational messages
- `WARNING`: Warnings only
- `ERROR`: Errors only

**Example:**
```bash
# Full test run with maximum debugging output
python postgresql-tests.py \
  --postgres_server localhost \
  --postgres_port 5432 \
  --postgres_database testdb \
  --postgres_schema scylla_fdw_tests \
  --postgres_username pguser \
  --postgres_password pgpass \
  --scylla_host scylla.example.com \
  --scylla_port 9042 \
  --scylla_keyspace scylla_fdw_tests \
  --scylla_username cassandra \
  --scylla_password cassandra \
  --debugging \
  --postgres_min_messages DEBUG1
```

# Adding or modifying tests

There are two folders where tests are added:

* `tests/scylla` contains the tests to interact with a ScyllaDB cluster using `scylla-tests.py`. Such tests are, normally, used to create stuff required for the PostgreSQL test themselves.
* `tests/postgresql` contains the test to interact with a PostgreSQL server using `postgresql-tests.py`. Such tests are, normally, used to test the `scylla_fdw` functionalities.

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
* `min` and `max` are version formats for ScyllaDB and PostgreSQL respectively.
  * `min` is mandatory, as minimum `2025.1.0` for ScyllaDB and `14.0` for PostgreSQL.
  * `max` is also mandatory, but it can be an empty string if the test supports up to the most recent ScyllaDB or PostgreSQL version.

You can check the list of versions for [ScyllaDB](https://www.scylladb.com/download/) and [PostgreSQL](https://www.postgresql.org/docs/release/) to adjust the `min` and `max` values as needed.

To validate the JSON file, you can use the script `validate-test-json`.

## The SQL/CQL file

It is a regular CQL file for ScyllaDB or SQL file for PostgreSQL.

There are several variables that can be used and will be placed by the testing scripts.

For the ScyllaDB scripts, the values come from the `scylla-tests.py` parameters:

* `@KEYSPACE`: The ScyllaDB keyspace name.

For the PostgreSQL scripts the values come from the `postgresql-tests.py` parameters:

* `@PSCHEMANAME`: The PostgreSQL schema name
* `@PUSER`: The PostgreSQL user
* `@SHOST`: The ScyllaDB host
* `@SPORT`: The ScyllaDB port
* `@SUSER`: The ScyllaDB user
* `@SPASSWORD`: The ScyllaDB password
* `@KEYSPACE`: The ScyllaDB keyspace
