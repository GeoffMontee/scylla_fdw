/*-------------------------------------------------------------------------
 *
 * scylla_fdw.c
 *        Foreign Data Wrapper for ScyllaDB using cpp-rs-driver
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "scylla_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/float.h"
#include "utils/formatting.h"
#include "utils/inet.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

PG_MODULE_MAGIC;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(scylla_fdw_handler);
PG_FUNCTION_INFO_V1(scylla_fdw_validator);
PG_FUNCTION_INFO_V1(scylla_fdw_version);

/*
 * FDW callback functions
 */
static void scyllaGetForeignRelSize(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    Oid foreigntableid);
static void scyllaGetForeignPaths(PlannerInfo *root,
                                  RelOptInfo *baserel,
                                  Oid foreigntableid);
static ForeignScan *scyllaGetForeignPlan(PlannerInfo *root,
                                         RelOptInfo *baserel,
                                         Oid foreigntableid,
                                         ForeignPath *best_path,
                                         List *tlist,
                                         List *scan_clauses,
                                         Plan *outer_plan);
static void scyllaBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *scyllaIterateForeignScan(ForeignScanState *node);
static void scyllaReScanForeignScan(ForeignScanState *node);
static void scyllaEndForeignScan(ForeignScanState *node);

/* Modification support - implemented in scylla_fdw_modify.c */
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

/* Join and upper path support - implemented in scylla_fdw_modify.c */
extern void scyllaGetForeignJoinPaths(PlannerInfo *root,
                                      RelOptInfo *joinrel,
                                      RelOptInfo *outerrel,
                                      RelOptInfo *innerrel,
                                      JoinType jointype,
                                      JoinPathExtraData *extra);

/* Explain support - implemented in scylla_fdw_modify.c */
#if PG_VERSION_NUM < 180000
extern void scyllaExplainForeignScan(ForeignScanState *node, ExplainState *es);
extern void scyllaExplainForeignModify(ModifyTableState *mtstate,
                                       ResultRelInfo *rinfo,
                                       List *fdw_private,
                                       int subplan_index,
                                       ExplainState *es);
#endif

/* Analyze support - implemented in scylla_fdw_modify.c */
extern bool scyllaAnalyzeForeignTable(Relation relation,
                                      AcquireSampleRowsFunc *func,
                                      BlockNumber *totalpages);

/* Import foreign schema support - implemented in scylla_fdw_modify.c */
extern List *scyllaImportForeignSchema(ImportForeignSchemaStmt *stmt,
                                       Oid serverOid);

/*
 * Helper functions - currently none needed
 */

/*
 * Valid options for scylla_fdw.
 */
static struct ScyllaFdwOption
{
    const char *keyword;
    Oid         context;    /* Oid of catalog in which option may appear */
} scylla_fdw_options[] =
{
    /* Server options */
    {OPT_HOST, ForeignServerRelationId},
    {OPT_PORT, ForeignServerRelationId},
    {OPT_PROTOCOL_VERSION, ForeignServerRelationId},
    {OPT_SSL, ForeignServerRelationId},
    {OPT_SSL_CERT, ForeignServerRelationId},
    {OPT_SSL_KEY, ForeignServerRelationId},
    {OPT_SSL_CA, ForeignServerRelationId},
    {OPT_CONNECT_TIMEOUT, ForeignServerRelationId},
    {OPT_REQUEST_TIMEOUT, ForeignServerRelationId},
    {OPT_CONSISTENCY, ForeignServerRelationId},

    /* User mapping options */
    {OPT_USERNAME, UserMappingRelationId},
    {OPT_PASSWORD, UserMappingRelationId},

    /* Table options */
    {OPT_KEYSPACE, ForeignTableRelationId},
    {OPT_TABLE, ForeignTableRelationId},
    {OPT_PRIMARY_KEY, ForeignTableRelationId},
    {OPT_CLUSTERING_KEY, ForeignTableRelationId},

    /* Sentinel */
    {NULL, InvalidOid}
};

/*
 * scylla_fdw_handler
 *        Return the FdwRoutine struct containing the FDW function pointers
 */
Datum
scylla_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *routine = makeNode(FdwRoutine);

    /* Required scan functions */
    routine->GetForeignRelSize = scyllaGetForeignRelSize;
    routine->GetForeignPaths = scyllaGetForeignPaths;
    routine->GetForeignPlan = scyllaGetForeignPlan;
    routine->BeginForeignScan = scyllaBeginForeignScan;
    routine->IterateForeignScan = scyllaIterateForeignScan;
    routine->ReScanForeignScan = scyllaReScanForeignScan;
    routine->EndForeignScan = scyllaEndForeignScan;

    /* Modification support */
    routine->AddForeignUpdateTargets = scyllaAddForeignUpdateTargets;
    routine->PlanForeignModify = scyllaPlanForeignModify;
    routine->BeginForeignModify = scyllaBeginForeignModify;
    routine->ExecForeignInsert = scyllaExecForeignInsert;
    routine->ExecForeignUpdate = scyllaExecForeignUpdate;
    routine->ExecForeignDelete = scyllaExecForeignDelete;
    routine->EndForeignModify = scyllaEndForeignModify;

    /* Join pushdown support */
    routine->GetForeignJoinPaths = scyllaGetForeignJoinPaths;

    /* Explain support */
#if PG_VERSION_NUM < 180000
    routine->ExplainForeignScan = scyllaExplainForeignScan;
    routine->ExplainForeignModify = scyllaExplainForeignModify;
#endif

    /* Analyze support */
    routine->AnalyzeForeignTable = scyllaAnalyzeForeignTable;

    /* Import foreign schema */
    routine->ImportForeignSchema = scyllaImportForeignSchema;

    PG_RETURN_POINTER(routine);
}

/*
 * scylla_fdw_validator
 *        Validate the options provided to a scylla_fdw foreign server/table/mapping
 */
Datum
scylla_fdw_validator(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *cell;

    foreach(cell, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(cell);
        bool        found = false;
        struct ScyllaFdwOption *opt;

        for (opt = scylla_fdw_options; opt->keyword; opt++)
        {
            if (strcmp(opt->keyword, def->defname) == 0)
            {
                if (catalog != opt->context)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("invalid option \"%s\"", def->defname),
                             errhint("Option \"%s\" is not valid for this object type.",
                                     def->defname)));
                }
                found = true;
                break;
            }
        }

        if (!found)
        {
            StringInfoData buf;
            struct ScyllaFdwOption *opt2;

            initStringInfo(&buf);
            for (opt2 = scylla_fdw_options; opt2->keyword; opt2++)
            {
                if (catalog == opt2->context)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
                                     opt2->keyword);
            }

            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\"", def->defname),
                     errhint("Valid options in this context are: %s", buf.data)));
        }

        /* Validate specific options */
        if (strcmp(def->defname, OPT_PORT) == 0)
        {
            char *endptr;
            long port = strtol(defGetString(def), &endptr, 10);
            if (*endptr != '\0' || port < 1 || port > 65535)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid port number: %s", defGetString(def))));
        }

        if (strcmp(def->defname, OPT_CONSISTENCY) == 0)
        {
            const char *val = defGetString(def);
            if (scylla_string_to_consistency(val) < 0)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid consistency level: %s", val),
                         errhint("Valid values are: any, one, two, three, quorum, all, "
                                 "local_quorum, each_quorum, serial, local_serial, local_one")));
        }
    }

    PG_RETURN_VOID();
}

/*
 * scylla_fdw_version
 *        Return the FDW version string
 */
Datum
scylla_fdw_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(SCYLLA_FDW_VERSION));
}

/*
 * scyllaGetForeignRelSize
 *        Obtain relation size estimates for a foreign table
 */
static void
scyllaGetForeignRelSize(PlannerInfo *root,
                        RelOptInfo *baserel,
                        Oid foreigntableid)
{
    ScyllaFdwRelationInfo *fpinfo;
    ListCell   *lc;

    /* Allocate FDW private info */
    fpinfo = (ScyllaFdwRelationInfo *) palloc0(sizeof(ScyllaFdwRelationInfo));
    baserel->fdw_private = (void *) fpinfo;

    /* Look up foreign server and table options */
    fpinfo->rel = table_open(foreigntableid, NoLock);

    /* Get server, table, and user options */
    {
        List *server_opts = NIL;
        List *table_opts = NIL;
        List *user_opts = NIL;

        scylla_get_options(foreigntableid, &server_opts, &table_opts, &user_opts);
        scylla_extract_options(server_opts, table_opts, user_opts, fpinfo);
    }

    /* Identify which baserestrictinfo clauses can be sent to the remote server */
    scylla_classify_conditions(root, baserel,
                               baserel->baserestrictinfo,
                               &fpinfo->remote_conds,
                               &fpinfo->local_conds);

    /* Identify which columns we need to fetch */
    fpinfo->attrs_used = NULL;
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
                   &fpinfo->attrs_used);
    foreach(lc, fpinfo->local_conds)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

        pull_varattnos((Node *) rinfo->clause, baserel->relid,
                       &fpinfo->attrs_used);
    }
    foreach(lc, fpinfo->remote_conds)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

        pull_varattnos((Node *) rinfo->clause, baserel->relid,
                       &fpinfo->attrs_used);
    }

    /* Estimate relation size */
    estimate_path_cost_size(root, baserel, NIL, NIL,
                            &fpinfo->rows, &fpinfo->width,
                            &fpinfo->startup_cost, &fpinfo->total_cost);

    /* Set the relation size estimate */
    baserel->rows = fpinfo->rows;
    baserel->tuples = fpinfo->rows;

    table_close(fpinfo->rel, NoLock);
}

/*
 * scyllaGetForeignPaths
 *        Create possible access paths for a foreign table
 */
static void
scyllaGetForeignPaths(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid)
{
    ScyllaFdwRelationInfo *fpinfo = (ScyllaFdwRelationInfo *) baserel->fdw_private;
    ForeignPath *path;

    /* Create a basic foreign path */
    /* Note: PG17 and PG18 changed the signature - check PG_VERSION_NUM */
#if PG_VERSION_NUM >= 180000
    /* PostgreSQL 18+ signature - added disabled_nodes parameter */
    path = create_foreignscan_path(root, baserel,
                                   NULL,                    /* default pathtarget */
                                   fpinfo->rows,            /* rows */
                                   0,                       /* disabled_nodes */
                                   fpinfo->startup_cost,    /* startup_cost */
                                   fpinfo->total_cost,      /* total_cost */
                                   NIL,                     /* pathkeys */
                                   NULL,                    /* required_outer */
                                   NULL,                    /* fdw_outerpath */
                                   NIL,                     /* fdw_restrictinfo */
                                   NIL);                    /* fdw_private */
#elif PG_VERSION_NUM >= 170000
    /* PostgreSQL 17 signature */
    path = create_foreignscan_path(root, baserel,
                                   NULL,    /* default pathtarget */
                                   fpinfo->rows,
                                   fpinfo->startup_cost,
                                   fpinfo->total_cost,
                                   NIL,     /* no pathkeys */
                                   baserel->lateral_relids,
                                   NULL,    /* no extra plan */
                                   NIL,     /* no fdw_restrictinfo */
                                   NIL);    /* no fdw_private */
#elif PG_VERSION_NUM >= 90600
    /* PostgreSQL 9.6 to 16 signature */
    path = create_foreignscan_path(root, baserel,
                                   NULL,    /* default pathtarget */
                                   fpinfo->rows,
                                   fpinfo->startup_cost,
                                   fpinfo->total_cost,
                                   NIL,     /* no pathkeys */
                                   baserel->lateral_relids,
                                   NULL,    /* no extra plan */
                                   NIL);    /* no fdw_private */
#else
    /* Pre-9.6 signature */
    path = create_foreignscan_path(root, baserel,
                                   fpinfo->rows,
                                   fpinfo->startup_cost,
                                   fpinfo->total_cost,
                                   NIL,     /* no pathkeys */
                                   baserel->lateral_relids,
                                   NULL,    /* no extra plan */
                                   NIL);    /* no fdw_private */
#endif
    add_path(baserel, (Path *) path);

    /* If we have ORDER BY pushdown possibility, add sorted path */
    /* ScyllaDB supports ORDER BY on clustering columns */
    /* Future enhancement: add ordered paths for clustering key columns */
}

/*
 * scyllaGetForeignPlan
 *        Create a ForeignScan plan node from the selected foreign path
 */
static ForeignScan *
scyllaGetForeignPlan(PlannerInfo *root,
                     RelOptInfo *baserel,
                     Oid foreigntableid,
                     ForeignPath *best_path,
                     List *tlist,
                     List *scan_clauses,
                     Plan *outer_plan)
{
    ScyllaFdwRelationInfo *fpinfo = (ScyllaFdwRelationInfo *) baserel->fdw_private;
    Index       scan_relid = baserel->relid;
    List       *fdw_private;
    List       *remote_exprs = NIL;
    List       *local_exprs = NIL;
    List       *params_list = NIL;
    List       *fdw_scan_tlist = NIL;
    List       *retrieved_attrs;
    StringInfoData sql;
    ListCell   *lc;

    /* Separate scan_clauses into those pushed down and those not */
    foreach(lc, scan_clauses)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

        if (list_member_ptr(fpinfo->remote_conds, rinfo))
            remote_exprs = lappend(remote_exprs, rinfo->clause);
        else
            local_exprs = lappend(local_exprs, rinfo->clause);
    }

    /* Build the CQL query */
    initStringInfo(&sql);
    {
        char *query = scylla_build_select_query(root, baserel, fpinfo,
                                                tlist, remote_exprs,
                                                &retrieved_attrs);
        appendStringInfoString(&sql, query);
        pfree(query);
    }

    /*
     * Build the fdw_private list that will be passed to BeginForeignScan.
     * Items:
     *  1) CQL query string
     *  2) Integer list of retrieved attribute numbers
     *  3) Remote conditions to be enforced
     */
    fdw_private = list_make3(makeString(sql.data),
                             retrieved_attrs,
                             remote_exprs);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
                            local_exprs,
                            scan_relid,
                            params_list,    /* params */
                            fdw_private,
                            fdw_scan_tlist,
                            remote_exprs,
                            outer_plan);
}

/*
 * scyllaBeginForeignScan
 *        Begin executing a foreign scan
 */
static void
scyllaBeginForeignScan(ForeignScanState *node, int eflags)
{
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    EState     *estate = node->ss.ps.state;
    ScyllaFdwScanState *fsstate;
    RangeTblEntry *rte;
    Oid         userid;
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user;
    int         rtindex;
    char       *error_msg = NULL;

    /* Do nothing for EXPLAIN without ANALYZE */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Allocate and initialize scan state */
    fsstate = (ScyllaFdwScanState *) palloc0(sizeof(ScyllaFdwScanState));
    node->fdw_state = (void *) fsstate;

    /* Get info about the foreign table */
    rtindex = bms_next_member(fsplan->fs_base_relids, -1);
    rte = exec_rt_fetch(rtindex, estate);
    
    /* Get the user ID for connection - in PG17+, use current user */
    userid = GetUserId();

    table = GetForeignTable(rte->relid);
    server = GetForeignServer(table->serverid);
    user = GetUserMapping(userid, server->serverid);

    /* Extract options */
    {
        ListCell *lc;
        char *host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        char *username = NULL;
        char *password = NULL;
        int connect_timeout = DEFAULT_CONNECT_TIMEOUT;
        bool use_ssl = false;
        char *ssl_cert = NULL;
        char *ssl_key = NULL;
        char *ssl_ca = NULL;

        /* Get server options */
        foreach(lc, server->options)
        {
            DefElem *def = (DefElem *) lfirst(lc);
            if (strcmp(def->defname, OPT_HOST) == 0)
                host = defGetString(def);
            else if (strcmp(def->defname, OPT_PORT) == 0)
                port = atoi(defGetString(def));
            else if (strcmp(def->defname, OPT_CONNECT_TIMEOUT) == 0)
                connect_timeout = atoi(defGetString(def));
            else if (strcmp(def->defname, OPT_SSL) == 0)
                use_ssl = defGetBoolean(def);
            else if (strcmp(def->defname, OPT_SSL_CERT) == 0)
                ssl_cert = defGetString(def);
            else if (strcmp(def->defname, OPT_SSL_KEY) == 0)
                ssl_key = defGetString(def);
            else if (strcmp(def->defname, OPT_SSL_CA) == 0)
                ssl_ca = defGetString(def);
        }

        /* Get user mapping options */
        foreach(lc, user->options)
        {
            DefElem *def = (DefElem *) lfirst(lc);
            if (strcmp(def->defname, OPT_USERNAME) == 0)
                username = defGetString(def);
            else if (strcmp(def->defname, OPT_PASSWORD) == 0)
                password = defGetString(def);
        }

        /* Connect to ScyllaDB */
        fsstate->conn = scylla_connect(host, port, username, password,
                                       connect_timeout, use_ssl,
                                       ssl_cert, ssl_key, ssl_ca,
                                       &error_msg);
        if (fsstate->conn == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                     errmsg("could not connect to ScyllaDB: %s",
                            error_msg ? error_msg : "unknown error")));
    }

    /* Get the CQL query from fdw_private */
    fsstate->query = strVal(list_nth(fsplan->fdw_private, 0));

    /* Initialize other fields */
    fsstate->rel = node->ss.ss_currentRelation;
    fsstate->tupdesc = RelationGetDescr(fsstate->rel);
    fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);
    fsstate->result = NULL;
    fsstate->iterator = NULL;
    fsstate->eof_reached = false;
    fsstate->fetch_ct = 0;

    /* Prepare column mapping */
    {
        List *retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
        int natts = fsstate->tupdesc->natts;
        int i;
        ListCell *lc;

        fsstate->num_cols = list_length(retrieved_attrs);
        fsstate->col_mapping = (int *) palloc(natts * sizeof(int));
        
        for (i = 0; i < natts; i++)
            fsstate->col_mapping[i] = -1;

        i = 0;
        foreach(lc, retrieved_attrs)
        {
            int attnum = lfirst_int(lc);
            if (attnum > 0 && attnum <= natts)
                fsstate->col_mapping[attnum - 1] = i;
            i++;
        }
    }
}

/*
 * scyllaIterateForeignScan
 *        Fetch one row from ScyllaDB
 */
static TupleTableSlot *
scyllaIterateForeignScan(ForeignScanState *node)
{
    ScyllaFdwScanState *fsstate = (ScyllaFdwScanState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    char *error_msg = NULL;

    /* Execute query if not already done */
    if (fsstate->result == NULL && !fsstate->eof_reached)
    {
        int consistency = SCYLLA_CONSISTENCY_LOCAL_QUORUM; /* Default */

        fsstate->result = scylla_execute_query(fsstate->conn, fsstate->query,
                                               consistency, &error_msg);
        if (fsstate->result == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("ScyllaDB query failed: %s",
                            error_msg ? error_msg : "unknown error")));

        fsstate->iterator = scylla_result_iterator(fsstate->result);
        if (fsstate->iterator == NULL)
        {
            fsstate->eof_reached = true;
            return ExecClearTuple(slot);
        }
    }

    /* Check if we've exhausted the result set */
    if (fsstate->eof_reached)
        return ExecClearTuple(slot);

    /* Fetch next row */
    if (!scylla_iterator_next(fsstate->iterator))
    {
        fsstate->eof_reached = true;
        return ExecClearTuple(slot);
    }

    /* Build the tuple */
    ExecClearTuple(slot);
    {
        TupleDesc tupdesc = fsstate->tupdesc;
        int natts = tupdesc->natts;
        Datum *values = slot->tts_values;
        bool *nulls = slot->tts_isnull;
        int i;

        /* Initialize all values as NULL */
        for (i = 0; i < natts; i++)
        {
            values[i] = (Datum) 0;
            nulls[i] = true;
        }

        /* Convert each column */
        for (i = 0; i < natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            int col_index = fsstate->col_mapping[i];
            bool is_null = true;

            if (col_index < 0 || attr->attisdropped)
                continue;

            values[i] = scylla_convert_to_pg(fsstate->iterator, col_index,
                                            attr->atttypid, attr->atttypmod,
                                            &is_null);
            nulls[i] = is_null;
        }
    }

    ExecStoreVirtualTuple(slot);
    fsstate->fetch_ct++;

    return slot;
}

/*
 * scyllaReScanForeignScan
 *        Restart the scan from the beginning
 */
static void
scyllaReScanForeignScan(ForeignScanState *node)
{
    ScyllaFdwScanState *fsstate = (ScyllaFdwScanState *) node->fdw_state;

    /* Release previous results */
    if (fsstate->iterator != NULL)
    {
        scylla_free_iterator(fsstate->iterator);
        fsstate->iterator = NULL;
    }
    if (fsstate->result != NULL)
    {
        scylla_free_result(fsstate->result);
        fsstate->result = NULL;
    }

    /* Reset state */
    fsstate->eof_reached = false;
    fsstate->fetch_ct = 0;
}

/*
 * scyllaEndForeignScan
 *        End the scan and release resources
 */
static void
scyllaEndForeignScan(ForeignScanState *node)
{
    ScyllaFdwScanState *fsstate = (ScyllaFdwScanState *) node->fdw_state;

    if (fsstate == NULL)
        return;

    /* Release iterator and result */
    if (fsstate->iterator != NULL)
        scylla_free_iterator(fsstate->iterator);
    if (fsstate->result != NULL)
        scylla_free_result(fsstate->result);
    if (fsstate->prepared != NULL)
        scylla_free_prepared(fsstate->prepared);

    /* Disconnect from ScyllaDB */
    if (fsstate->conn != NULL)
        scylla_disconnect(fsstate->conn, fsstate->cluster);
}
