/*-------------------------------------------------------------------------
 *
 * scylla_connection.cpp
 *        C++ wrapper for ScyllaDB cpp-rs-driver
 *
 * This file provides a C-compatible interface to the ScyllaDB driver.
 * The cpp-rs-driver uses the Cassandra driver API (cassandra.h).
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_connection.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassandra.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <cstdint>

extern "C" {

/*
 * Connection management
 */

typedef struct ScyllaConnection {
    CassCluster* cluster;
    CassSession* session;
} ScyllaConnection;

void *
scylla_connect(const char *host, int port, const char *username,
               const char *password, int connect_timeout,
               bool use_ssl, const char *ssl_cert, 
               const char *ssl_key, const char *ssl_ca,
               char **error_msg)
{
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();
    CassFuture* connect_future = NULL;
    CassError rc;
    ScyllaConnection* conn;

    *error_msg = NULL;

    /* Set contact points */
    cass_cluster_set_contact_points(cluster, host);
    cass_cluster_set_port(cluster, port);

    /* Set connection timeout */
    cass_cluster_set_connect_timeout(cluster, connect_timeout);

    /* Set authentication if provided */
    if (username != NULL && password != NULL) {
        cass_cluster_set_credentials(cluster, username, password);
    }

    /* Set up SSL if enabled */
    if (use_ssl) {
        CassSsl* ssl = cass_ssl_new();
        
        /* Set verification mode */
        cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);
        
        /* Load trusted certificates */
        if (ssl_ca != NULL) {
            rc = cass_ssl_add_trusted_cert(ssl, ssl_ca);
            if (rc != CASS_OK) {
                *error_msg = strdup(cass_error_desc(rc));
                cass_ssl_free(ssl);
                cass_session_free(session);
                cass_cluster_free(cluster);
                return NULL;
            }
        }
        
        /* Load client certificate */
        if (ssl_cert != NULL) {
            rc = cass_ssl_set_cert(ssl, ssl_cert);
            if (rc != CASS_OK) {
                *error_msg = strdup(cass_error_desc(rc));
                cass_ssl_free(ssl);
                cass_session_free(session);
                cass_cluster_free(cluster);
                return NULL;
            }
        }
        
        /* Load private key */
        if (ssl_key != NULL) {
            rc = cass_ssl_set_private_key(ssl, ssl_key, NULL);
            if (rc != CASS_OK) {
                *error_msg = strdup(cass_error_desc(rc));
                cass_ssl_free(ssl);
                cass_session_free(session);
                cass_cluster_free(cluster);
                return NULL;
            }
        }
        
        cass_cluster_set_ssl(cluster, ssl);
        cass_ssl_free(ssl);  /* Cluster takes ownership */
    }

    /* Connect to the cluster */
    connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);

    rc = cass_future_error_code(connect_future);
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        *error_msg = strndup(message, message_length);
        cass_future_free(connect_future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return NULL;
    }

    cass_future_free(connect_future);

    /* Allocate and return connection struct */
    conn = (ScyllaConnection*) malloc(sizeof(ScyllaConnection));
    conn->cluster = cluster;
    conn->session = session;

    return conn;
}

void
scylla_disconnect(void *conn_ptr, void *cluster_ptr)
{
    ScyllaConnection* conn = (ScyllaConnection*) conn_ptr;
    
    if (conn == NULL)
        return;

    /* Close session */
    CassFuture* close_future = cass_session_close(conn->session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    /* Free resources */
    cass_session_free(conn->session);
    cass_cluster_free(conn->cluster);
    free(conn);
}

/*
 * Query execution
 */

void *
scylla_execute_query(void *conn_ptr, const char *query, 
                     int consistency, char **error_msg)
{
    ScyllaConnection* conn = (ScyllaConnection*) conn_ptr;
    CassStatement* statement;
    CassFuture* result_future;
    CassError rc;

    *error_msg = NULL;

    statement = cass_statement_new(query, 0);
    cass_statement_set_consistency(statement, (CassConsistency) consistency);

    result_future = cass_session_execute(conn->session, statement);
    cass_future_wait(result_future);

    rc = cass_future_error_code(result_future);
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        *error_msg = strndup(message, message_length);
        cass_future_free(result_future);
        cass_statement_free(statement);
        return NULL;
    }

    const CassResult* result = cass_future_get_result(result_future);
    cass_future_free(result_future);
    cass_statement_free(statement);

    return (void*) result;
}

void *
scylla_prepare_query(void *conn_ptr, const char *query, char **error_msg)
{
    ScyllaConnection* conn = (ScyllaConnection*) conn_ptr;
    CassFuture* prepare_future;
    CassError rc;

    *error_msg = NULL;

    prepare_future = cass_session_prepare(conn->session, query);
    cass_future_wait(prepare_future);

    rc = cass_future_error_code(prepare_future);
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(prepare_future, &message, &message_length);
        *error_msg = strndup(message, message_length);
        cass_future_free(prepare_future);
        return NULL;
    }

    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    return (void*) prepared;
}

void *
scylla_execute_prepared(void *conn_ptr, void *prepared_ptr, 
                        void **params, int num_params,
                        int consistency, char **error_msg)
{
    ScyllaConnection* conn = (ScyllaConnection*) conn_ptr;
    const CassPrepared* prepared = (const CassPrepared*) prepared_ptr;
    CassStatement* statement = (CassStatement*) params[0];  /* Statement is passed as first param */
    CassFuture* result_future;
    CassError rc;

    *error_msg = NULL;

    cass_statement_set_consistency(statement, (CassConsistency) consistency);

    result_future = cass_session_execute(conn->session, statement);
    cass_future_wait(result_future);

    rc = cass_future_error_code(result_future);
    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        *error_msg = strndup(message, message_length);
        cass_future_free(result_future);
        return NULL;
    }

    const CassResult* result = cass_future_get_result(result_future);
    cass_future_free(result_future);

    return (void*) result;
}

void
scylla_free_result(void *result_ptr)
{
    if (result_ptr != NULL)
        cass_result_free((const CassResult*) result_ptr);
}

void
scylla_free_prepared(void *prepared_ptr)
{
    if (prepared_ptr != NULL)
        cass_prepared_free((const CassPrepared*) prepared_ptr);
}

/*
 * Result iteration
 */

void *
scylla_result_iterator(void *result_ptr)
{
    const CassResult* result = (const CassResult*) result_ptr;
    return cass_iterator_from_result(result);
}

bool
scylla_iterator_next(void *iterator_ptr)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    return cass_iterator_next(iterator) == cass_true;
}

void
scylla_free_iterator(void *iterator_ptr)
{
    if (iterator_ptr != NULL)
        cass_iterator_free((CassIterator*) iterator_ptr);
}

/*
 * Value extraction
 */

bool
scylla_get_bool(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_bool_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return false;

    cass_value_get_bool(value, &result);
    return result == cass_true;
}

int32_t
scylla_get_int32(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_int32_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0;

    cass_value_get_int32(value, &result);
    return result;
}

int64_t
scylla_get_int64(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_int64_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0;

    cass_value_get_int64(value, &result);
    return result;
}

float
scylla_get_float(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_float_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0.0f;

    cass_value_get_float(value, &result);
    return result;
}

double
scylla_get_double(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_double_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0.0;

    cass_value_get_double(value, &result);
    return result;
}

const char *
scylla_get_string(void *iterator_ptr, int col, size_t *len, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    const char* result;
    size_t result_length;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null) {
        *len = 0;
        return NULL;
    }

    cass_value_get_string(value, &result, &result_length);
    *len = result_length;
    return result;
}

const char *
scylla_get_bytes(void *iterator_ptr, int col, size_t *len, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    const cass_byte_t* result;
    size_t result_size;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null) {
        *len = 0;
        return NULL;
    }

    cass_value_get_bytes(value, &result, &result_size);
    *len = result_size;
    return (const char*) result;
}

const char *
scylla_get_uuid(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    CassUuid uuid;
    static char uuid_str[CASS_UUID_STRING_LENGTH];

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return NULL;

    cass_value_get_uuid(value, &uuid);
    cass_uuid_string(uuid, uuid_str);
    return uuid_str;
}

const char *
scylla_get_inet(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    CassInet inet;
    static char inet_str[CASS_INET_STRING_LENGTH];

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return NULL;

    cass_value_get_inet(value, &inet);
    cass_inet_string(inet, inet_str);
    return inet_str;
}

int64_t
scylla_get_timestamp(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_int64_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0;

    cass_value_get_int64(value, &result);
    return result;
}

int32_t
scylla_get_date(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_uint32_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0;

    cass_value_get_uint32(value, &result);
    return (int32_t) result;
}

int64_t
scylla_get_time(void *iterator_ptr, int col, bool *is_null)
{
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    cass_int64_t result;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return 0;

    cass_value_get_int64(value, &result);
    return result;
}

const char *
scylla_get_decimal(void *iterator_ptr, int col, bool *is_null)
{
    /* Decimal type returns varint + scale - convert to string */
    CassIterator* iterator = (CassIterator*) iterator_ptr;
    const CassRow* row = cass_iterator_get_row(iterator);
    const CassValue* value = cass_row_get_column(row, col);
    const cass_byte_t* varint;
    size_t varint_size;
    cass_int32_t scale;

    *is_null = (cass_value_is_null(value) == cass_true);
    if (*is_null)
        return NULL;

    cass_value_get_decimal(value, &varint, &varint_size, &scale);
    
    /* For simplicity, return a placeholder - full implementation would
       convert varint to string with proper decimal placement */
    static char decimal_str[64];
    snprintf(decimal_str, sizeof(decimal_str), "0");  /* Placeholder */
    return decimal_str;
}

/*
 * Column metadata
 */

int
scylla_get_column_type(void *result_ptr, int col)
{
    const CassResult* result = (const CassResult*) result_ptr;
    const CassColumnMeta* meta = cass_result_column_type(result, col);
    
    /* This returns a CassValueType which maps to our type codes */
    CassValueType type = cass_result_column_type(result, col);
    return (int) type;
}

int
scylla_get_column_count(void *result_ptr)
{
    const CassResult* result = (const CassResult*) result_ptr;
    return (int) cass_result_column_count(result);
}

const char *
scylla_get_column_name(void *result_ptr, int col, size_t *len)
{
    const CassResult* result = (const CassResult*) result_ptr;
    const char* name;
    size_t name_length;

    cass_result_column_name(result, col, &name, &name_length);
    *len = name_length;
    return name;
}

/*
 * Statement binding
 */

void *
scylla_create_statement(void *prepared_ptr)
{
    const CassPrepared* prepared = (const CassPrepared*) prepared_ptr;
    return cass_prepared_bind(prepared);
}

void
scylla_bind_null(void *statement_ptr, int index)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_null(statement, index);
}

void
scylla_bind_bool(void *statement_ptr, int index, bool value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_bool(statement, index, value ? cass_true : cass_false);
}

void
scylla_bind_int32(void *statement_ptr, int index, int32_t value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_int32(statement, index, value);
}

void
scylla_bind_int64(void *statement_ptr, int index, int64_t value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_int64(statement, index, value);
}

void
scylla_bind_float(void *statement_ptr, int index, float value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_float(statement, index, value);
}

void
scylla_bind_double(void *statement_ptr, int index, double value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_double(statement, index, value);
}

void
scylla_bind_string(void *statement_ptr, int index, const char *value, size_t len)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_string_n(statement, index, value, len);
}

void
scylla_bind_bytes(void *statement_ptr, int index, const char *value, size_t len)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_bytes(statement, index, (const cass_byte_t*) value, len);
}

void
scylla_bind_uuid(void *statement_ptr, int index, const char *value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    CassUuid uuid;
    cass_uuid_from_string(value, &uuid);
    cass_statement_bind_uuid(statement, index, uuid);
}

void
scylla_bind_timestamp(void *statement_ptr, int index, int64_t value)
{
    CassStatement* statement = (CassStatement*) statement_ptr;
    cass_statement_bind_int64(statement, index, value);
}

void
scylla_free_statement(void *statement_ptr)
{
    if (statement_ptr != NULL)
        cass_statement_free((CassStatement*) statement_ptr);
}

/*
 * Utility functions
 */

const char *
scylla_consistency_to_string(int consistency)
{
    switch ((CassConsistency) consistency) {
        case CASS_CONSISTENCY_ANY:          return "any";
        case CASS_CONSISTENCY_ONE:          return "one";
        case CASS_CONSISTENCY_TWO:          return "two";
        case CASS_CONSISTENCY_THREE:        return "three";
        case CASS_CONSISTENCY_QUORUM:       return "quorum";
        case CASS_CONSISTENCY_ALL:          return "all";
        case CASS_CONSISTENCY_LOCAL_QUORUM: return "local_quorum";
        case CASS_CONSISTENCY_EACH_QUORUM:  return "each_quorum";
        case CASS_CONSISTENCY_SERIAL:       return "serial";
        case CASS_CONSISTENCY_LOCAL_SERIAL: return "local_serial";
        case CASS_CONSISTENCY_LOCAL_ONE:    return "local_one";
        default:                            return "unknown";
    }
}

int
scylla_string_to_consistency(const char *str)
{
    if (strcasecmp(str, "any") == 0)          return CASS_CONSISTENCY_ANY;
    if (strcasecmp(str, "one") == 0)          return CASS_CONSISTENCY_ONE;
    if (strcasecmp(str, "two") == 0)          return CASS_CONSISTENCY_TWO;
    if (strcasecmp(str, "three") == 0)        return CASS_CONSISTENCY_THREE;
    if (strcasecmp(str, "quorum") == 0)       return CASS_CONSISTENCY_QUORUM;
    if (strcasecmp(str, "all") == 0)          return CASS_CONSISTENCY_ALL;
    if (strcasecmp(str, "local_quorum") == 0) return CASS_CONSISTENCY_LOCAL_QUORUM;
    if (strcasecmp(str, "each_quorum") == 0)  return CASS_CONSISTENCY_EACH_QUORUM;
    if (strcasecmp(str, "serial") == 0)       return CASS_CONSISTENCY_SERIAL;
    if (strcasecmp(str, "local_serial") == 0) return CASS_CONSISTENCY_LOCAL_SERIAL;
    if (strcasecmp(str, "local_one") == 0)    return CASS_CONSISTENCY_LOCAL_ONE;
    return -1;
}

int64_t
scylla_result_row_count(void *result_ptr)
{
    const CassResult* result = (const CassResult*) result_ptr;
    return (int64_t) cass_result_row_count(result);
}

}  /* extern "C" */
