/*-------------------------------------------------------------------------
 *
 * scylla_typemap.c
 *        Type conversion between PostgreSQL and ScyllaDB
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_typemap.c
 *
 *-------------------------------------------------------------------------
 */
#include "scylla_fdw.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/float.h"
#include "utils/inet.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

/*
 * CassValueType enum values from cassandra.h
 */
#define CASS_VALUE_TYPE_UNKNOWN     0xFFFF
#define CASS_VALUE_TYPE_CUSTOM      0x0000
#define CASS_VALUE_TYPE_ASCII       0x0001
#define CASS_VALUE_TYPE_BIGINT      0x0002
#define CASS_VALUE_TYPE_BLOB        0x0003
#define CASS_VALUE_TYPE_BOOLEAN     0x0004
#define CASS_VALUE_TYPE_COUNTER     0x0005
#define CASS_VALUE_TYPE_DECIMAL     0x0006
#define CASS_VALUE_TYPE_DOUBLE      0x0007
#define CASS_VALUE_TYPE_FLOAT       0x0008
#define CASS_VALUE_TYPE_INT         0x0009
#define CASS_VALUE_TYPE_TEXT        0x000A
#define CASS_VALUE_TYPE_TIMESTAMP   0x000B
#define CASS_VALUE_TYPE_UUID        0x000C
#define CASS_VALUE_TYPE_VARCHAR     0x000D
#define CASS_VALUE_TYPE_VARINT      0x000E
#define CASS_VALUE_TYPE_TIMEUUID    0x000F
#define CASS_VALUE_TYPE_INET        0x0010
#define CASS_VALUE_TYPE_DATE        0x0011
#define CASS_VALUE_TYPE_TIME        0x0012
#define CASS_VALUE_TYPE_SMALLINT    0x0013
#define CASS_VALUE_TYPE_TINYINT     0x0014
#define CASS_VALUE_TYPE_DURATION    0x0015
#define CASS_VALUE_TYPE_LIST        0x0020
#define CASS_VALUE_TYPE_MAP         0x0021
#define CASS_VALUE_TYPE_SET         0x0022
#define CASS_VALUE_TYPE_UDT         0x0030
#define CASS_VALUE_TYPE_TUPLE       0x0031

/* PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01 */
#define POSTGRES_EPOCH_JDATE    2451545  /* == date2j(2000, 1, 1) */
#define UNIX_EPOCH_JDATE        2440588  /* == date2j(1970, 1, 1) */
#define SECS_PER_DAY            86400
#define MSECS_PER_SEC           1000LL
/* USECS_PER_SEC is already defined in PostgreSQL headers */

/*
 * scylla_convert_to_pg
 *        Convert a ScyllaDB value to a PostgreSQL Datum
 */
Datum
scylla_convert_to_pg(void *iterator, int col, Oid pg_type,
                     int32 typmod, bool *is_null)
{
    Datum       result = (Datum) 0;

    switch (pg_type)
    {
        case BOOLOID:
            {
                bool value = scylla_get_bool(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = BoolGetDatum(value);
            }
            break;

        case INT2OID:
            {
                int32 value = scylla_get_int32(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = Int16GetDatum((int16) value);
            }
            break;

        case INT4OID:
            {
                int32 value = scylla_get_int32(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = Int32GetDatum(value);
            }
            break;

        case INT8OID:
            {
                int64 value = scylla_get_int64(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = Int64GetDatum(value);
            }
            break;

        case FLOAT4OID:
            {
                float value = scylla_get_float(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = Float4GetDatum(value);
            }
            break;

        case FLOAT8OID:
            {
                double value = scylla_get_double(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = Float8GetDatum(value);
            }
            break;

        case NUMERICOID:
            {
                /* Get decimal from ScyllaDB and convert to PostgreSQL numeric */
                const char *decimal_str;
                
                decimal_str = scylla_get_decimal(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                
                result = DirectFunctionCall3(numeric_in,
                                            CStringGetDatum(decimal_str),
                                            ObjectIdGetDatum(InvalidOid),
                                            Int32GetDatum(typmod));
            }
            break;

        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            {
                size_t len;
                const char *str = scylla_get_string(iterator, col, &len, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = PointerGetDatum(cstring_to_text_with_len(str, len));
            }
            break;

        case BYTEAOID:
            {
                size_t len;
                const char *data;
                bytea *bytes;
                
                data = scylla_get_bytes(iterator, col, &len, is_null);
                if (*is_null)
                    return (Datum) 0;
                
                bytes = (bytea *) palloc(VARHDRSZ + len);
                SET_VARSIZE(bytes, VARHDRSZ + len);
                memcpy(VARDATA(bytes), data, len);
                result = PointerGetDatum(bytes);
            }
            break;

        case UUIDOID:
            {
                const char *uuid_str = scylla_get_uuid(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = DirectFunctionCall1(uuid_in, CStringGetDatum(uuid_str));
            }
            break;

        case TIMESTAMPOID:
            {
                /* ScyllaDB timestamp is milliseconds since Unix epoch */
                int64 ms;
                int64 usec;
                
                ms = scylla_get_timestamp(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                
                /* Convert to PostgreSQL timestamp (microseconds since 2000-01-01) */
                usec = ms * 1000LL;
                /* Adjust from Unix epoch to PostgreSQL epoch */
                usec -= ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC);
                result = TimestampGetDatum(usec);
            }
            break;

        case TIMESTAMPTZOID:
            {
                /* Same as TIMESTAMP but ScyllaDB stores in UTC */
                int64 ms;
                int64 usec;
                
                ms = scylla_get_timestamp(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                
                usec = ms * 1000LL;
                usec -= ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC);
                result = TimestampTzGetDatum(usec);
            }
            break;

        case DATEOID:
            {
                /* ScyllaDB date is days since 1970-01-01 with center at 2^31 */
                int32 scylla_date;
                int32 unix_days;
                int32 pg_date;
                
                scylla_date = scylla_get_date(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                
                /* Convert to PostgreSQL date (days since 2000-01-01) */
                /* ScyllaDB date: 2^31 = 1970-01-01, so actual days = value - 2^31 */
                unix_days = scylla_date - (1 << 31);
                pg_date = unix_days - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
                result = DateADTGetDatum(pg_date);
            }
            break;

        case TIMEOID:
            {
                /* ScyllaDB time is nanoseconds since midnight */
                int64 ns;
                int64 usec;
                
                ns = scylla_get_time(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                
                /* Convert to PostgreSQL time (microseconds since midnight) */
                usec = ns / 1000;
                result = TimeADTGetDatum(usec);
            }
            break;

        case INETOID:
            {
                const char *inet_str = scylla_get_inet(iterator, col, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = DirectFunctionCall1(inet_in, CStringGetDatum(inet_str));
            }
            break;

        default:
            /* Fall back to text conversion */
            {
                size_t len;
                const char *str = scylla_get_string(iterator, col, &len, is_null);
                if (*is_null)
                    return (Datum) 0;
                result = PointerGetDatum(cstring_to_text_with_len(str, len));
            }
            break;
    }

    return result;
}

/*
 * scylla_convert_from_pg
 *        Convert a PostgreSQL Datum to ScyllaDB format and bind to statement
 */
void
scylla_convert_from_pg(Datum value, Oid pg_type, void *statement,
                       int index, bool is_null)
{
    if (is_null)
    {
        scylla_bind_null(statement, index);
        return;
    }

    switch (pg_type)
    {
        case BOOLOID:
            scylla_bind_bool(statement, index, DatumGetBool(value));
            break;

        case INT2OID:
            scylla_bind_int32(statement, index, DatumGetInt16(value));
            break;

        case INT4OID:
            scylla_bind_int32(statement, index, DatumGetInt32(value));
            break;

        case INT8OID:
            scylla_bind_int64(statement, index, DatumGetInt64(value));
            break;

        case FLOAT4OID:
            scylla_bind_float(statement, index, DatumGetFloat4(value));
            break;

        case FLOAT8OID:
            scylla_bind_double(statement, index, DatumGetFloat8(value));
            break;

        case NUMERICOID:
            {
                /* Convert numeric to string for ScyllaDB decimal type */
                char *str = DatumGetCString(DirectFunctionCall1(numeric_out, value));
                scylla_bind_decimal(statement, index, str);
                pfree(str);
            }
            break;

        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            {
                text *txt = DatumGetTextPP(value);
                scylla_bind_string(statement, index,
                                   VARDATA_ANY(txt),
                                   VARSIZE_ANY_EXHDR(txt));
            }
            break;

        case BYTEAOID:
            {
                bytea *bytes = DatumGetByteaPP(value);
                scylla_bind_bytes(statement, index,
                                  VARDATA_ANY(bytes),
                                  VARSIZE_ANY_EXHDR(bytes));
            }
            break;

        case UUIDOID:
            {
                char *uuid_str = DatumGetCString(DirectFunctionCall1(uuid_out, value));
                scylla_bind_uuid(statement, index, uuid_str);
                pfree(uuid_str);
            }
            break;

        case TIMESTAMPOID:
            {
                /* Convert PostgreSQL timestamp to ScyllaDB milliseconds since Unix epoch */
                Timestamp ts = DatumGetTimestamp(value);
                /* Add PostgreSQL to Unix epoch offset */
                int64 usec = ts + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC);
                int64 ms = usec / 1000;
                scylla_bind_timestamp(statement, index, ms);
            }
            break;

        case TIMESTAMPTZOID:
            {
                TimestampTz ts = DatumGetTimestampTz(value);
                int64 usec = ts + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC);
                int64 ms = usec / 1000;
                scylla_bind_timestamp(statement, index, ms);
            }
            break;

        case DATEOID:
            {
                DateADT pg_date = DatumGetDateADT(value);
                /* Convert to ScyllaDB date format (uint32 with 2^31 as epoch) */
                int32 unix_days = pg_date + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
                uint32_t scylla_date = (uint32_t)(unix_days + (1 << 31));
                scylla_bind_uint32(statement, index, scylla_date);
            }
            break;

        case TIMEOID:
            {
                TimeADT pg_time = DatumGetTimeADT(value);
                /* Convert microseconds to nanoseconds */
                int64 ns = pg_time * 1000;
                scylla_bind_int64(statement, index, ns);
            }
            break;

        case INETOID:
            {
                char *inet_str = DatumGetCString(DirectFunctionCall1(inet_out, value));
                scylla_bind_string(statement, index, inet_str, strlen(inet_str));
                pfree(inet_str);
            }
            break;

        default:
            /* Fall back to text conversion */
            {
                Oid         typoutput;
                bool        typIsVarlena;
                char       *str;

                getTypeOutputInfo(pg_type, &typoutput, &typIsVarlena);
                str = OidOutputFunctionCall(typoutput, value);
                scylla_bind_string(statement, index, str, strlen(str));
                pfree(str);
            }
            break;
    }
}

/*
 * scylla_report_error
 *        Report an error from the ScyllaDB driver
 */
void
scylla_report_error(int elevel, const char *msg)
{
    ereport(elevel,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("ScyllaDB error: %s", msg ? msg : "unknown error")));
}
