/*-------------------------------------------------------------------------
 *
 * scylla_fdw.h
 *        Foreign Data Wrapper for ScyllaDB
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCYLLA_FDW_H
#define SCYLLA_FDW_H

#include "postgres.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "access/reloptions.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/plancat.h"
#include "optimizer/appendinfo.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "commands/copy.h"

/* Version */
#define SCYLLA_FDW_VERSION "1.0.0"

/*
 * Options that can be set on the server, user mapping, or foreign table
 */

/* Server options */
#define OPT_HOST                "host"
#define OPT_PORT                "port"
#define OPT_PROTOCOL_VERSION    "protocol_version"
#define OPT_SSL                 "ssl"
#define OPT_SSL_CERT            "ssl_cert"
#define OPT_SSL_KEY             "ssl_key"
#define OPT_SSL_CA              "ssl_ca"
#define OPT_CONNECT_TIMEOUT     "connect_timeout"
#define OPT_REQUEST_TIMEOUT     "request_timeout"
#define OPT_CONSISTENCY         "consistency"

/* User mapping options */
#define OPT_USERNAME            "username"
#define OPT_PASSWORD            "password"

/* Table options */
#define OPT_KEYSPACE            "keyspace"
#define OPT_TABLE               "table"
#define OPT_PRIMARY_KEY         "primary_key"
#define OPT_CLUSTERING_KEY      "clustering_key"

/* Default values */
#define DEFAULT_HOST            "127.0.0.1"
#define DEFAULT_PORT            9042
#define DEFAULT_CONSISTENCY     "local_quorum"
#define DEFAULT_CONNECT_TIMEOUT 5000
#define DEFAULT_REQUEST_TIMEOUT 12000

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private
 */
typedef struct ScyllaFdwRelationInfo
{
    /* baserestrictinfo clauses, partitioned into safe and unsafe subsets */
    List       *remote_conds;
    List       *local_conds;

    /* Bitmap of attr numbers we need to fetch from the remote server */
    Bitmapset  *attrs_used;

    /* Cost and selectivity of local_conds */
    QualCost    local_conds_cost;
    Selectivity local_conds_sel;

    /* Estimated size and cost for a scan with baserestrictinfo quals */
    double      rows;
    int         width;
    Cost        startup_cost;
    Cost        total_cost;

    /* Options extracted from catalogs */
    char       *keyspace;
    char       *table;
    char       *primary_key;
    char       *clustering_key;
    char       *host;
    int         port;
    char       *username;
    char       *password;
    char       *consistency;

    /* Join pushdown info */
    bool        use_remote_estimate;
    Cost        fdw_startup_cost;
    Cost        fdw_tuple_cost;

    /* Cached relation info */
    Relation    rel;
    
    /* For join pushdown */
    RelOptInfo *outerrel;
    RelOptInfo *innerrel;
    JoinType    jointype;
    List       *joinclauses;
} ScyllaFdwRelationInfo;

/*
 * Execution state of a foreign scan
 */
typedef struct ScyllaFdwScanState
{
    /* Connection state */
    void       *conn;           /* CassSession* */
    void       *cluster;        /* CassCluster* */
    
    /* Query execution state */
    void       *result;         /* CassResult* */
    void       *iterator;       /* CassIterator* */
    void       *prepared;       /* CassPrepared* */
    
    /* Query string */
    char       *query;
    
    /* Relation info */
    Relation    rel;
    AttInMetadata *attinmeta;
    
    /* Parameters for parameterized scans */
    List       *param_exprs;
    FmgrInfo   *param_flinfo;
    List       *param_types;
    
    /* For rescans */
    int         fetch_ct;
    bool        eof_reached;
    
    /* Tuple descriptor */
    TupleDesc   tupdesc;
    
    /* Column mapping */
    int        *col_mapping;
    int         num_cols;
} ScyllaFdwScanState;

/*
 * Execution state for modification operations
 */
typedef struct ScyllaFdwModifyState
{
    /* Connection state */
    void       *conn;           /* CassSession* */
    void       *cluster;        /* CassCluster* */
    
    /* Prepared statements */
    void       *prepared;       /* CassPrepared* */
    
    /* Query string */
    char       *query;
    
    /* Relation info */
    Relation    rel;
    
    /* Target column info */
    int         num_params;
    FmgrInfo   *param_flinfo;
    List       *target_attrs;
    Oid        *param_types;
    
    /* Primary key info for UPDATE/DELETE */
    AttrNumber *pk_attrs;
    int         num_pk_attrs;
    
    /* Operation type */
    CmdType     operation;
    
    /* Tuple descriptor */
    TupleDesc   tupdesc;
} ScyllaFdwModifyState;

/*
 * CQL consistency level mapping
 */
typedef enum ScyllaCqlConsistency
{
    SCYLLA_CONSISTENCY_ANY = 0,
    SCYLLA_CONSISTENCY_ONE = 1,
    SCYLLA_CONSISTENCY_TWO = 2,
    SCYLLA_CONSISTENCY_THREE = 3,
    SCYLLA_CONSISTENCY_QUORUM = 4,
    SCYLLA_CONSISTENCY_ALL = 5,
    SCYLLA_CONSISTENCY_LOCAL_QUORUM = 6,
    SCYLLA_CONSISTENCY_EACH_QUORUM = 7,
    SCYLLA_CONSISTENCY_SERIAL = 8,
    SCYLLA_CONSISTENCY_LOCAL_SERIAL = 9,
    SCYLLA_CONSISTENCY_LOCAL_ONE = 10
} ScyllaCqlConsistency;

/*
 * Function declarations for C++ wrapper (extern "C" compatible)
 */
#ifdef __cplusplus
extern "C" {
#endif

/* Connection management */
void       *scylla_connect(const char *host, int port, const char *username,
                           const char *password, int connect_timeout,
                           bool use_ssl, const char *ssl_cert, 
                           const char *ssl_key, const char *ssl_ca,
                           char **error_msg);
void        scylla_disconnect(void *conn, void *cluster);

/* Query execution */
void       *scylla_execute_query(void *conn, const char *query, 
                                 int consistency, char **error_msg);
void       *scylla_prepare_query(void *conn, const char *query, char **error_msg);
void       *scylla_execute_prepared(void *conn, void *prepared, 
                                    void **params, int num_params,
                                    int consistency, char **error_msg);
void        scylla_free_result(void *result);
void        scylla_free_prepared(void *prepared);

/* Result iteration */
void       *scylla_result_iterator(void *result);
bool        scylla_iterator_next(void *iterator);
void        scylla_free_iterator(void *iterator);

/* Value extraction */
bool        scylla_get_bool(void *iterator, int col, bool *is_null);
int32_t     scylla_get_int32(void *iterator, int col, bool *is_null);
int64_t     scylla_get_int64(void *iterator, int col, bool *is_null);
float       scylla_get_float(void *iterator, int col, bool *is_null);
double      scylla_get_double(void *iterator, int col, bool *is_null);
const char *scylla_get_string(void *iterator, int col, size_t *len, bool *is_null);
const char *scylla_get_bytes(void *iterator, int col, size_t *len, bool *is_null);
const char *scylla_get_uuid(void *iterator, int col, bool *is_null);
const char *scylla_get_inet(void *iterator, int col, bool *is_null);
int64_t     scylla_get_timestamp(void *iterator, int col, bool *is_null);
int32_t     scylla_get_date(void *iterator, int col, bool *is_null);
int64_t     scylla_get_time(void *iterator, int col, bool *is_null);
const char *scylla_get_decimal(void *iterator, int col, bool *is_null);

/* Value type detection */
int         scylla_get_column_type(void *result, int col);
int         scylla_get_column_count(void *result);
const char *scylla_get_column_name(void *result, int col, size_t *len);

/* Statement binding */
void       *scylla_create_statement(void *prepared);
void        scylla_bind_null(void *statement, int index);
void        scylla_bind_bool(void *statement, int index, bool value);
void        scylla_bind_int32(void *statement, int index, int32_t value);
void        scylla_bind_int64(void *statement, int index, int64_t value);
void        scylla_bind_float(void *statement, int index, float value);
void        scylla_bind_double(void *statement, int index, double value);
void        scylla_bind_string(void *statement, int index, const char *value, size_t len);
void        scylla_bind_bytes(void *statement, int index, const char *value, size_t len);
void        scylla_bind_uuid(void *statement, int index, const char *value);
void        scylla_bind_timestamp(void *statement, int index, int64_t value);
void        scylla_free_statement(void *statement);

/* Utility functions */
const char *scylla_consistency_to_string(int consistency);
int         scylla_string_to_consistency(const char *str);
int64_t     scylla_result_row_count(void *result);

#ifdef __cplusplus
}
#endif

/*
 * Internal helper function declarations
 */

/* Option validation and extraction */
void scylla_get_options(Oid foreigntableid, List **server_options, 
                        List **table_options, List **user_options);
void scylla_extract_options(List *server_opts, List *table_opts, 
                           List *user_opts, ScyllaFdwRelationInfo *fpinfo);

/* CQL query generation */
char *scylla_build_select_query(PlannerInfo *root, RelOptInfo *baserel,
                                ScyllaFdwRelationInfo *fpinfo,
                                List *tlist, List *remote_conds,
                                List **retrieved_attrs);
char *scylla_build_insert_query(Relation rel, List *target_attrs);
char *scylla_build_update_query(Relation rel, List *target_attrs,
                                int *pk_attrs, int num_pk_attrs);
char *scylla_build_delete_query(Relation rel, int *pk_attrs, int num_pk_attrs);

/* WHERE clause deparsing */
void scylla_deparse_expr(Expr *expr, StringInfo buf, PlannerInfo *root,
                        RelOptInfo *baserel, bool *can_pushdown);
bool scylla_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
void scylla_classify_conditions(PlannerInfo *root, RelOptInfo *baserel,
                                List *input_conds, List **remote_conds,
                                List **local_conds);

/* Type conversion */
Datum scylla_convert_to_pg(void *iterator, int col, Oid pg_type,
                           int32 typmod, bool *is_null);
void scylla_convert_from_pg(Datum value, Oid pg_type, void *statement,
                            int index, bool is_null);

/* Connection caching */
void *scylla_get_connection(ForeignServer *server, UserMapping *user,
                            bool will_prep_stmt);
void scylla_release_connection(void *conn);

/* Utility */
char *scylla_quote_identifier(const char *ident);
void scylla_report_error(int elevel, const char *msg);

/* Cost estimation */
void estimate_path_cost_size(PlannerInfo *root, RelOptInfo *baserel,
                             List *join_conds, List *pathkeys,
                             double *p_rows, int *p_width,
                             Cost *p_startup_cost, Cost *p_total_cost);
List *scylla_get_useful_pathkeys(PlannerInfo *root, RelOptInfo *baserel);
List *scylla_get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *baserel);

/* Option handling */
void apply_server_options(ScyllaFdwRelationInfo *fpinfo, ForeignServer *server);
void apply_table_options(ScyllaFdwRelationInfo *fpinfo, ForeignTable *table);
void merge_fdw_options(ScyllaFdwRelationInfo *fpinfo,
                       ForeignServer *server,
                       ForeignTable *table,
                       UserMapping *user);
bool is_valid_option(const char *option, Oid context);

/* Column utilities */
int get_relation_column_count(Relation rel);
AttrNumber get_column_by_name(Relation rel, const char *colname);
List *parse_column_list(Relation rel, const char *collist);
bool is_partition_key_column(ScyllaFdwRelationInfo *fpinfo, AttrNumber attnum, Relation rel);
bool is_clustering_key_column(ScyllaFdwRelationInfo *fpinfo, AttrNumber attnum, Relation rel);

/*
 * FDW callback function prototypes (implemented in scylla_fdw_modify.c)
 */
extern void scyllaAddForeignUpdateTargets(PlannerInfo *root,
                                          Index rtindex,
                                          RangeTblEntry *target_rte,
                                          Relation target_relation);
extern List *scyllaPlanForeignModify(PlannerInfo *root,
                                     ModifyTable *plan,
                                     Index resultRelation,
                                     int subplan_index);
extern void scyllaBeginForeignModify(ModifyTableState *mtstate,
                                     ResultRelInfo *resultRelInfo,
                                     List *fdw_private,
                                     int subplan_index,
                                     int eflags);
extern TupleTableSlot *scyllaExecForeignInsert(EState *estate,
                                               ResultRelInfo *resultRelInfo,
                                               TupleTableSlot *slot,
                                               TupleTableSlot *planSlot);
extern TupleTableSlot *scyllaExecForeignUpdate(EState *estate,
                                               ResultRelInfo *resultRelInfo,
                                               TupleTableSlot *slot,
                                               TupleTableSlot *planSlot);
extern TupleTableSlot *scyllaExecForeignDelete(EState *estate,
                                               ResultRelInfo *resultRelInfo,
                                               TupleTableSlot *slot,
                                               TupleTableSlot *planSlot);
extern void scyllaEndForeignModify(EState *estate,
                                   ResultRelInfo *resultRelInfo);
extern void scyllaGetForeignJoinPaths(PlannerInfo *root,
                                      RelOptInfo *joinrel,
                                      RelOptInfo *outerrel,
                                      RelOptInfo *innerrel,
                                      JoinType jointype,
                                      JoinPathExtraData *extra);
#if PG_VERSION_NUM < 180000
extern void scyllaExplainForeignScan(ForeignScanState *node, ExplainState *es);
extern void scyllaExplainForeignModify(ModifyTableState *mtstate,
                                       ResultRelInfo *rinfo,
                                       List *fdw_private,
                                       int subplan_index,
                                       ExplainState *es);
#endif
extern bool scyllaAnalyzeForeignTable(Relation relation,
                                      AcquireSampleRowsFunc *func,
                                      BlockNumber *totalpages);
extern List *scyllaImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);

#endif   /* SCYLLA_FDW_H */
