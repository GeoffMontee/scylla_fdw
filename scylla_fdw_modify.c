/*-------------------------------------------------------------------------
 *
 * scylla_fdw_modify.c
 *        Modification support for ScyllaDB Foreign Data Wrapper
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_fdw_modify.c
 *
 *-------------------------------------------------------------------------
 */
#include "scylla_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"

/*
 * scyllaAddForeignUpdateTargets
 *        Add the primary key columns as junk columns for UPDATE/DELETE
 */
void
scyllaAddForeignUpdateTargets(PlannerInfo *root,
                              Index rtindex,
                              RangeTblEntry *target_rte,
                              Relation target_relation)
{
    Oid         relid = RelationGetRelid(target_relation);
    TupleDesc   tupdesc = RelationGetDescr(target_relation);
    ForeignTable *table;
    ListCell   *lc;
    char       *pk_str = NULL;
    List       *pk_cols = NIL;
    char       *str;
    char       *token;
    char       *saveptr;
    char       *end;
    int         i;

    table = GetForeignTable(relid);

    /* Get the primary_key option */
    foreach(lc, table->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_PRIMARY_KEY) == 0)
        {
            pk_str = defGetString(def);
            break;
        }
    }

    if (pk_str == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("primary_key option must be specified for UPDATE/DELETE operations")));
    }

    /* Parse primary key columns (comma-separated) */
    str = pstrdup(pk_str);

    for (token = strtok_r(str, ",", &saveptr);
         token != NULL;
         token = strtok_r(NULL, ",", &saveptr))
    {
        /* Trim whitespace */
        while (*token == ' ') token++;
        end = token + strlen(token) - 1;
        while (end > token && *end == ' ') *end-- = '\0';

        pk_cols = lappend(pk_cols, makeString(pstrdup(token)));
    }

    /* Add each primary key column as a junk attribute */
    foreach(lc, pk_cols)
    {
        char       *colname = strVal(lfirst(lc));
        int         attnum = 0;
        Var        *var;

        /* Find the attribute number */
        for (i = 0; i < tupdesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (strcmp(NameStr(attr->attname), colname) == 0)
            {
                attnum = i + 1;
                break;
            }
        }

        if (attnum == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
                     errmsg("primary key column \"%s\" not found", colname)));

        /* Make a Var representing the column */
        var = makeVar(rtindex,
                      attnum,
                      TupleDescAttr(tupdesc, attnum - 1)->atttypid,
                      TupleDescAttr(tupdesc, attnum - 1)->atttypmod,
                      TupleDescAttr(tupdesc, attnum - 1)->attcollation,
                      0);

        /* Add it as a resjunk entry */
        add_row_identity_var(root, var, rtindex, pstrdup(colname));
    }
}

/*
 * scyllaPlanForeignModify
 *        Plan an INSERT/UPDATE/DELETE operation
 */
List *
scyllaPlanForeignModify(PlannerInfo *root,
                        ModifyTable *plan,
                        Index resultRelation,
                        int subplan_index)
{
    CmdType     operation = plan->operation;
    RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
    Relation    rel;
    List       *targetAttrs = NIL;
    StringInfoData sql;
    TupleDesc   tupdesc;
    int         attnum;
    char       *pk_str = NULL;
    ListCell   *lc;
    ForeignTable *table;
    int        *pk_attrs;
    int         num_pk_attrs;
    char       *str;
    char       *token;
    char       *saveptr;
    char       *end;
    char       *query;
    int         idx;
    int         i;

    elog(DEBUG1, "scylla_fdw: planning %s operation for relation %u",
         operation == CMD_INSERT ? "INSERT" :
         operation == CMD_UPDATE ? "UPDATE" :
         operation == CMD_DELETE ? "DELETE" : "UNKNOWN",
         rte->relid);

    initStringInfo(&sql);

    /* Open the relation to get column info */
    rel = table_open(rte->relid, NoLock);
    tupdesc = RelationGetDescr(rel);

    /* Get target columns for the operation */
    if (operation == CMD_INSERT)
    {
        /* For INSERT, include all non-dropped columns */
        for (attnum = 1; attnum <= tupdesc->natts; attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

            if (!attr->attisdropped)
                targetAttrs = lappend_int(targetAttrs, attnum);
        }
    }
    else if (operation == CMD_UPDATE)
    {
        /* For UPDATE, include only columns being updated */
#if PG_VERSION_NUM >= 180000
        /* In PG18+, updatedCols is in the ModifyTable plan node */
        Bitmapset *updatedCols = plan->updateColnosLists ?
            (Bitmapset *) list_nth(plan->updateColnosLists, subplan_index) : NULL;
#else
        /* In PG17 and earlier, updatedCols is in RangeTblEntry */
        Bitmapset *updatedCols = rte->updatedCols;
#endif

        elog(DEBUG1, "scylla_fdw: updatedCols is %s (subplan_index=%d)",
             updatedCols ? "set" : "NULL", subplan_index);

        for (attnum = 1; attnum <= tupdesc->natts; attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

            if (!attr->attisdropped &&
                updatedCols != NULL &&
                bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber,
                              updatedCols))
            {
                elog(DEBUG1, "scylla_fdw: adding column %s (attnum=%d) to UPDATE SET clause",
                     NameStr(attr->attname), attnum);
                targetAttrs = lappend_int(targetAttrs, attnum);
            }
        }
        
        elog(DEBUG1, "scylla_fdw: UPDATE will modify %d column(s)", list_length(targetAttrs));
    }

    /*
     * Build the SQL command string.
     */
    switch (operation)
    {
        case CMD_INSERT:
            query = scylla_build_insert_query(rel, targetAttrs);
            appendStringInfoString(&sql, query);
            pfree(query);
            break;

        case CMD_UPDATE:
            table = GetForeignTable(rte->relid);

            /* Get primary key columns */
            foreach(lc, table->options)
            {
                DefElem *def = (DefElem *) lfirst(lc);
                if (strcmp(def->defname, OPT_PRIMARY_KEY) == 0)
                {
                    pk_str = defGetString(def);
                    break;
                }
            }

            if (pk_str == NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("primary_key option required for UPDATE")));

            /* Parse and convert to attribute numbers */
            num_pk_attrs = 1;
            for (str = pk_str; *str; str++)
                if (*str == ',') num_pk_attrs++;

            pk_attrs = (int *) palloc(num_pk_attrs * sizeof(int));
            str = pstrdup(pk_str);
            idx = 0;
            for (token = strtok_r(str, ",", &saveptr);
                 token != NULL;
                 token = strtok_r(NULL, ",", &saveptr))
            {
                while (*token == ' ') token++;
                end = token + strlen(token) - 1;
                while (end > token && *end == ' ') *end-- = '\0';

                for (i = 0; i < tupdesc->natts; i++)
                {
                    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                    if (strcmp(NameStr(attr->attname), token) == 0)
                    {
                        pk_attrs[idx++] = i + 1;
                        break;
                    }
                }
            }
            num_pk_attrs = idx;

            query = scylla_build_update_query(rel, targetAttrs, pk_attrs, num_pk_attrs);
            appendStringInfoString(&sql, query);
            pfree(query);

            /* Rebuild targetAttrs to match prepared statement order:
             * non-PK columns first (for SET), then PK columns (for WHERE) */
            targetAttrs = NIL;
            for (attnum = 1; attnum <= tupdesc->natts; attnum++)
            {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
                bool is_pk = false;

                if (attr->attisdropped)
                    continue;

                /* Check if this is a PK column */
                for (i = 0; i < num_pk_attrs; i++)
                {
                    if (pk_attrs[i] == attnum)
                    {
                        is_pk = true;
                        break;
                    }
                }

                /* Add non-PK columns first */
                if (!is_pk)
                    targetAttrs = lappend_int(targetAttrs, attnum);
            }
            /* Then add PK columns */
            for (i = 0; i < num_pk_attrs; i++)
                targetAttrs = lappend_int(targetAttrs, pk_attrs[i]);

            break;

        case CMD_DELETE:
            table = GetForeignTable(rte->relid);

            /* Get primary key columns */
            pk_str = NULL;
            foreach(lc, table->options)
            {
                DefElem *def = (DefElem *) lfirst(lc);
                if (strcmp(def->defname, OPT_PRIMARY_KEY) == 0)
                {
                    pk_str = defGetString(def);
                    break;
                }
            }

            if (pk_str == NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("primary_key option required for DELETE")));

            /* Parse and convert to attribute numbers */
            num_pk_attrs = 1;
            for (str = pk_str; *str; str++)
                if (*str == ',') num_pk_attrs++;

            pk_attrs = (int *) palloc(num_pk_attrs * sizeof(int));
            str = pstrdup(pk_str);
            idx = 0;
            for (token = strtok_r(str, ",", &saveptr);
                 token != NULL;
                 token = strtok_r(NULL, ",", &saveptr))
            {
                while (*token == ' ') token++;
                end = token + strlen(token) - 1;
                while (end > token && *end == ' ') *end-- = '\0';

                for (i = 0; i < tupdesc->natts; i++)
                {
                    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                    if (strcmp(NameStr(attr->attname), token) == 0)
                    {
                        pk_attrs[idx++] = i + 1;
                        break;
                    }
                }
            }
            num_pk_attrs = idx;

            query = scylla_build_delete_query(rel, pk_attrs, num_pk_attrs);
            appendStringInfoString(&sql, query);
            pfree(query);

            /* For DELETE, targetAttrs should contain only PK columns */
            for (i = 0; i < num_pk_attrs; i++)
                targetAttrs = lappend_int(targetAttrs, pk_attrs[i]);

            break;

        default:
            elog(ERROR, "unexpected operation: %d", (int) operation);
            break;
    }

    table_close(rel, NoLock);

    elog(DEBUG1, "scylla_fdw: generated CQL %s query: %s",
         operation == CMD_INSERT ? "INSERT" :
         operation == CMD_UPDATE ? "UPDATE" :
         operation == CMD_DELETE ? "DELETE" : "UNKNOWN",
         sql.data);

    /*
     * Return the command string as fdw_private list for use in executor.
     * Items:
     *  1) CQL command string
     *  2) Target attribute list
     */
    return list_make2(makeString(sql.data), targetAttrs);
}

/*
 * scyllaBeginForeignModify
 *        Begin a foreign modification operation
 */
void
scyllaBeginForeignModify(ModifyTableState *mtstate,
                         ResultRelInfo *resultRelInfo,
                         List *fdw_private,
                         int subplan_index,
                         int eflags)
{
    ScyllaFdwModifyState *fmstate;
    Relation    rel = resultRelInfo->ri_RelationDesc;
    Oid         userid;
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user;
    char       *error_msg = NULL;
    ListCell   *lc;
    char       *host = DEFAULT_HOST;
    int         port = DEFAULT_PORT;
    char       *username = NULL;
    char       *password = NULL;
    int         connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    bool        use_ssl = false;
    char       *ssl_cert = NULL;
    char       *ssl_key = NULL;
    char       *ssl_ca = NULL;

    /* Do nothing for EXPLAIN without ANALYZE */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Allocate and initialize modify state */
    fmstate = (ScyllaFdwModifyState *) palloc0(sizeof(ScyllaFdwModifyState));
    resultRelInfo->ri_FdwState = fmstate;
    
    /* Set operation early so it can be used in log messages */
    fmstate->operation = mtstate->operation;

    /* Get the user ID for connection */
    userid = GetUserId();

    table = GetForeignTable(RelationGetRelid(rel));
    server = GetForeignServer(table->serverid);
    user = GetUserMapping(userid, server->serverid);

    /* Extract connection options */
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

    foreach(lc, user->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_USERNAME) == 0)
            username = defGetString(def);
        else if (strcmp(def->defname, OPT_PASSWORD) == 0)
            password = defGetString(def);
    }

    /* Connect to ScyllaDB */
    elog(DEBUG1, "scylla_fdw: connecting to ScyllaDB at %s:%d for %s operation",
         host, port,
         fmstate->operation == CMD_INSERT ? "INSERT" :
         fmstate->operation == CMD_UPDATE ? "UPDATE" :
         fmstate->operation == CMD_DELETE ? "DELETE" : "UNKNOWN");
    fmstate->conn = scylla_connect(host, port, username, password,
                                   connect_timeout, use_ssl,
                                   ssl_cert, ssl_key, ssl_ca,
                                   &error_msg);
    if (fmstate->conn == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg("could not connect to ScyllaDB: %s",
                        error_msg ? error_msg : "unknown error")));
    elog(DEBUG1, "scylla_fdw: successfully connected to ScyllaDB");

    /* Get the CQL command from fdw_private */
    fmstate->query = strVal(list_nth(fdw_private, 0));
    fmstate->target_attrs = (List *) list_nth(fdw_private, 1);

    ereport(NOTICE,
            (errmsg("scylla_fdw: preparing remote %s statement",
                    fmstate->operation == CMD_INSERT ? "INSERT" :
                    fmstate->operation == CMD_UPDATE ? "UPDATE" :
                    fmstate->operation == CMD_DELETE ? "DELETE" : "UNKNOWN"),
             errdetail("%s", fmstate->query)));

    /* Prepare the statement */
    fmstate->prepared = scylla_prepare_query(fmstate->conn, fmstate->query, &error_msg);
    if (fmstate->prepared == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("could not prepare ScyllaDB statement: %s",
                        error_msg ? error_msg : "unknown error")));

    /* Store additional state */
    fmstate->rel = rel;
    fmstate->tupdesc = RelationGetDescr(rel);
    fmstate->num_params = list_length(fmstate->target_attrs);
    
    /* For UPDATE/DELETE, we need to find junk attributes for PK columns */
    if (fmstate->operation == CMD_UPDATE || fmstate->operation == CMD_DELETE)
    {
        Plan       *subplan = outerPlanState(mtstate)->plan;
        char       *pk_str = NULL;
        char       *str;
        char       *token;
        char       *saveptr;
        char       *end;
        int         num_pk;
        int         idx;
        
        /* Get primary key column names */
        foreach(lc, table->options)
        {
            DefElem *def = (DefElem *) lfirst(lc);
            if (strcmp(def->defname, OPT_PRIMARY_KEY) == 0)
            {
                pk_str = defGetString(def);
                break;
            }
        }
        
        if (pk_str == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("primary_key option required for UPDATE/DELETE")));
        
        /* Count PK columns */
        num_pk = 1;
        for (str = pk_str; *str; str++)
            if (*str == ',') num_pk++;
        
        /* Allocate array for junk attribute numbers */
        fmstate->junk_att_nums = (AttrNumber *) palloc(num_pk * sizeof(AttrNumber));
        fmstate->num_pk_attrs = num_pk;
        
        /* Find junk attribute number for each PK column */
        str = pstrdup(pk_str);
        idx = 0;
        for (token = strtok_r(str, ",", &saveptr);
             token != NULL;
             token = strtok_r(NULL, ",", &saveptr))
        {
            AttrNumber attnum;
            
            /* Trim whitespace */
            while (*token == ' ') token++;
            end = token + strlen(token) - 1;
            while (end > token && *end == ' ') *end-- = '\0';
            
            /* Find junk attribute in targetlist */
            attnum = ExecFindJunkAttributeInTlist(subplan->targetlist, token);
            if (!AttributeNumberIsValid(attnum))
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("primary key column \"%s\" not found in junk attributes", token)));
            
            fmstate->junk_att_nums[idx++] = attnum;
        }
        pfree(str);
    }
    else
    {
        fmstate->junk_att_nums = NULL;
        fmstate->num_pk_attrs = 0;
    }
}

/*
 * scyllaExecForeignInsert
 *        Insert one row into a foreign table
 */
TupleTableSlot *
scyllaExecForeignInsert(EState *estate,
                        ResultRelInfo *resultRelInfo,
                        TupleTableSlot *slot,
                        TupleTableSlot *planSlot)
{
    ScyllaFdwModifyState *fmstate = (ScyllaFdwModifyState *) resultRelInfo->ri_FdwState;
    void       *statement;
    char       *error_msg = NULL;
    ListCell   *lc;
    int         pindex = 0;

    elog(DEBUG1, "scylla_fdw: executing INSERT");

    /* Create a statement from the prepared query */
    statement = scylla_create_statement(fmstate->prepared);
    if (statement == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("could not create ScyllaDB statement")));

    /* Bind parameters from the slot */
    foreach(lc, fmstate->target_attrs)
    {
        int         attnum = lfirst_int(lc);
        Datum       value;
        bool        isnull;

        value = slot_getattr(slot, attnum, &isnull);
        scylla_convert_from_pg(value, TupleDescAttr(fmstate->tupdesc, attnum - 1)->atttypid,
                               statement, pindex, isnull);
        pindex++;
    }

    /* Execute the statement */
    if (scylla_execute_prepared(fmstate->conn, fmstate->prepared, 
                                &statement, 1,
                                SCYLLA_CONSISTENCY_LOCAL_QUORUM, &error_msg) == NULL)
    {
        scylla_free_statement(statement);
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("ScyllaDB INSERT failed: %s",
                        error_msg ? error_msg : "unknown error")));
    }

    scylla_free_statement(statement);

    return slot;
}

/*
 * scyllaExecForeignUpdate
 *        Update one row in a foreign table
 */
TupleTableSlot *
scyllaExecForeignUpdate(EState *estate,
                        ResultRelInfo *resultRelInfo,
                        TupleTableSlot *slot,
                        TupleTableSlot *planSlot)
{
    ScyllaFdwModifyState *fmstate = (ScyllaFdwModifyState *) resultRelInfo->ri_FdwState;
    void       *statement;
    char       *error_msg = NULL;
    TupleDesc   tupdesc = fmstate->tupdesc;
    int         num_non_pk;
    int         i;
    int         pindex = 0;

    elog(DEBUG1, "scylla_fdw: executing UPDATE");

    /* Create a statement from the prepared query */
    statement = scylla_create_statement(fmstate->prepared);
    if (statement == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("could not create ScyllaDB statement")));

    /* 
     * Bind parameters: non-PK columns from slot (new values),
     * then PK columns from planSlot (junk attributes for WHERE clause)
     */
    num_non_pk = list_length(fmstate->target_attrs) - fmstate->num_pk_attrs;
    
    /* Bind non-PK columns (SET clause) from slot */
    for (i = 0; i < num_non_pk; i++)
    {
        int attnum = list_nth_int(fmstate->target_attrs, i);
        Datum value;
        bool isnull;
        
        value = slot_getattr(slot, attnum, &isnull);
        scylla_convert_from_pg(value, TupleDescAttr(tupdesc, attnum - 1)->atttypid,
                               statement, pindex, isnull);
        pindex++;
    }
    
    /* Bind PK columns (WHERE clause) from planSlot junk attributes */
    for (i = 0; i < fmstate->num_pk_attrs; i++)
    {
        Datum value;
        bool isnull;
        int attnum;
        
        /* Get value from junk attribute in planSlot */
        value = ExecGetJunkAttribute(planSlot, fmstate->junk_att_nums[i], &isnull);
        
        /* Get the actual attribute number to determine the type */
        attnum = list_nth_int(fmstate->target_attrs, num_non_pk + i);
        
        scylla_convert_from_pg(value, TupleDescAttr(tupdesc, attnum - 1)->atttypid,
                               statement, pindex, isnull);
        pindex++;
    }

    /* Execute the statement */
    if (scylla_execute_prepared(fmstate->conn, fmstate->prepared,
                                &statement, 1,
                                SCYLLA_CONSISTENCY_LOCAL_QUORUM, &error_msg) == NULL)
    {
        scylla_free_statement(statement);
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("ScyllaDB UPDATE failed: %s",
                        error_msg ? error_msg : "unknown error")));
    }

    scylla_free_statement(statement);

    return slot;
}

/*
 * scyllaExecForeignDelete
 *        Delete one row from a foreign table
 */
TupleTableSlot *
scyllaExecForeignDelete(EState *estate,
                        ResultRelInfo *resultRelInfo,
                        TupleTableSlot *slot,
                        TupleTableSlot *planSlot)
{
    ScyllaFdwModifyState *fmstate = (ScyllaFdwModifyState *) resultRelInfo->ri_FdwState;
    void       *statement;
    char       *error_msg = NULL;
    TupleDesc   tupdesc = fmstate->tupdesc;
    int         i;
    int         pindex = 0;

    elog(DEBUG1, "scylla_fdw: executing DELETE");

    /* Create a statement from the prepared query */
    statement = scylla_create_statement(fmstate->prepared);
    if (statement == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("could not create ScyllaDB statement")));

    /* Bind primary key values from planSlot junk attributes */
    for (i = 0; i < fmstate->num_pk_attrs; i++)
    {
        Datum value;
        bool isnull;
        int attnum;
        
        /* Get value from junk attribute in planSlot */
        value = ExecGetJunkAttribute(planSlot, fmstate->junk_att_nums[i], &isnull);
        
        /* Get the actual attribute number to determine the type */
        attnum = list_nth_int(fmstate->target_attrs, i);
        
        scylla_convert_from_pg(value, TupleDescAttr(tupdesc, attnum - 1)->atttypid,
                               statement, pindex, isnull);
        pindex++;
    }

    /* Execute the statement */
    if (scylla_execute_prepared(fmstate->conn, fmstate->prepared,
                                &statement, 1,
                                SCYLLA_CONSISTENCY_LOCAL_QUORUM, &error_msg) == NULL)
    {
        scylla_free_statement(statement);
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("ScyllaDB DELETE failed: %s",
                        error_msg ? error_msg : "unknown error")));
    }

    scylla_free_statement(statement);

    return slot;
}

/*
 * scyllaEndForeignModify
 *        End a foreign modification operation
 */
void
scyllaEndForeignModify(EState *estate,
                       ResultRelInfo *resultRelInfo)
{
    ScyllaFdwModifyState *fmstate = (ScyllaFdwModifyState *) resultRelInfo->ri_FdwState;

    if (fmstate == NULL)
        return;

    elog(DEBUG1, "scylla_fdw: ending foreign modify operation");

    /* Release prepared statement */
    if (fmstate->prepared != NULL)
        scylla_free_prepared(fmstate->prepared);

    /* Disconnect from ScyllaDB */
    if (fmstate->conn != NULL)
        scylla_disconnect(fmstate->conn, fmstate->cluster);
}

/*
 * scyllaGetForeignJoinPaths
 *        Create paths for joining foreign tables on the same server
 *
 * Note: ScyllaDB doesn't support JOINs in CQL, so we don't add any paths.
 */
void
scyllaGetForeignJoinPaths(PlannerInfo *root,
                          RelOptInfo *joinrel,
                          RelOptInfo *outerrel,
                          RelOptInfo *innerrel,
                          JoinType jointype,
                          JoinPathExtraData *extra)
{
    /*
     * ScyllaDB doesn't support JOINs at the CQL level, so we can't push down
     * the join operation. Just return without adding any join paths.
     */
}

#if PG_VERSION_NUM < 180000
/*
 * scyllaExplainForeignScan
 *        Produce extra output for EXPLAIN
 */
void
scyllaExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    ForeignScan *plan = castNode(ForeignScan, node->ss.ps.plan);
    List       *fdw_private = plan->fdw_private;
    char       *sql;

    if (fdw_private != NIL)
    {
        sql = strVal(list_nth(fdw_private, 0));
        ExplainPropertyText("ScyllaDB Query", sql, es);
    }
}

/*
 * scyllaExplainForeignModify
 *        Produce extra output for EXPLAIN of modification operations
 */
void
scyllaExplainForeignModify(ModifyTableState *mtstate,
                           ResultRelInfo *rinfo,
                           List *fdw_private,
                           int subplan_index,
                           ExplainState *es)
{
    char       *sql;

    if (fdw_private != NIL)
    {
        sql = strVal(list_nth(fdw_private, 0));
        ExplainPropertyText("ScyllaDB Query", sql, es);
    }
}
#endif

/*
 * scyllaAnalyzeForeignTable
 *        Test whether analyzing this table is supported
 */
bool
scyllaAnalyzeForeignTable(Relation relation,
                          AcquireSampleRowsFunc *func,
                          BlockNumber *totalpages)
{
    /*
     * ScyllaDB doesn't provide easy access to table statistics,
     * so we don't support ANALYZE for now.
     */
    return false;
}

/*
 * scyllaImportForeignSchema
 *        Import foreign schema
 */
List *
scyllaImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    ForeignServer *server;
    UserMapping *user;
    List       *commands = NIL;
    void       *conn = NULL;
    void       *result = NULL;
    void       *iterator = NULL;
    char       *error_msg = NULL;
    StringInfoData sql;
    StringInfoData cmd;
    char       *host = DEFAULT_HOST;
    int         port = DEFAULT_PORT;
    char       *username = NULL;
    char       *password = NULL;
    ListCell   *lc;
    char       *current_table = NULL;
    char       *pk_cols = NULL;
    bool        in_table = false;

    server = GetForeignServer(serverOid);
    user = GetUserMapping(GetUserId(), serverOid);

    /* Extract connection options */
    foreach(lc, server->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_HOST) == 0)
            host = defGetString(def);
        else if (strcmp(def->defname, OPT_PORT) == 0)
            port = atoi(defGetString(def));
    }

    foreach(lc, user->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_USERNAME) == 0)
            username = defGetString(def);
        else if (strcmp(def->defname, OPT_PASSWORD) == 0)
            password = defGetString(def);
    }

    /* Connect to ScyllaDB */
    conn = scylla_connect(host, port, username, password,
                          DEFAULT_CONNECT_TIMEOUT, false,
                          NULL, NULL, NULL, &error_msg);
    if (conn == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                 errmsg("could not connect to ScyllaDB: %s",
                        error_msg ? error_msg : "unknown error")));

    /* Query system_schema.columns for the keyspace */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT table_name, column_name, type, kind "
                     "FROM system_schema.columns "
                     "WHERE keyspace_name = '%s' "
                     "ORDER BY table_name, position",
                     stmt->remote_schema);

    result = scylla_execute_query(conn, sql.data,
                                  SCYLLA_CONSISTENCY_LOCAL_ONE, &error_msg);
    if (result == NULL)
    {
        scylla_disconnect(conn, NULL);
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("could not query ScyllaDB schema: %s",
                        error_msg ? error_msg : "unknown error")));
    }

    iterator = scylla_result_iterator(result);
    if (iterator != NULL)
    {
        initStringInfo(&cmd);

        while (scylla_iterator_next(iterator))
        {
            size_t      len;
            bool        is_null;
            const char *table_name;
            const char *column_name;
            const char *cql_type;
            const char *kind;
            char       *pg_type;

            table_name = scylla_get_string(iterator, 0, &len, &is_null);
            if (is_null) continue;

            column_name = scylla_get_string(iterator, 1, &len, &is_null);
            if (is_null) continue;

            cql_type = scylla_get_string(iterator, 2, &len, &is_null);
            if (is_null) continue;

            kind = scylla_get_string(iterator, 3, &len, &is_null);

            /* Check if we've moved to a new table */
            if (current_table == NULL || strcmp(current_table, table_name) != 0)
            {
                /* Finish previous table if any */
                if (in_table)
                {
                    /* Remove trailing comma and newline */
                    cmd.len -= 2;
                    cmd.data[cmd.len] = '\0';

                    appendStringInfo(&cmd,
                                     "\n) SERVER %s\n"
                                     "OPTIONS (keyspace '%s', \"table\" '%s'",
                                     quote_identifier(server->servername),
                                     stmt->remote_schema,
                                     current_table);

                    if (pk_cols != NULL)
                        appendStringInfo(&cmd, ", primary_key '%s'", pk_cols);

                    appendStringInfoString(&cmd, ");");

                    commands = lappend(commands, pstrdup(cmd.data));
                }

                /* Start new table */
                current_table = pstrdup(table_name);
                pk_cols = NULL;
                resetStringInfo(&cmd);

                appendStringInfo(&cmd,
                                 "CREATE FOREIGN TABLE %s (\n",
                                 quote_identifier(table_name));
                in_table = true;
            }

            /* Map CQL type to PostgreSQL type */
            if (strcmp(cql_type, "uuid") == 0 || strcmp(cql_type, "timeuuid") == 0)
                pg_type = "uuid";
            else if (strcmp(cql_type, "text") == 0 || strcmp(cql_type, "ascii") == 0 ||
                     strcmp(cql_type, "varchar") == 0)
                pg_type = "text";
            else if (strcmp(cql_type, "int") == 0)
                pg_type = "integer";
            else if (strcmp(cql_type, "bigint") == 0 || strcmp(cql_type, "counter") == 0)
                pg_type = "bigint";
            else if (strcmp(cql_type, "smallint") == 0)
                pg_type = "smallint";
            else if (strcmp(cql_type, "tinyint") == 0)
                pg_type = "smallint";
            else if (strcmp(cql_type, "float") == 0)
                pg_type = "real";
            else if (strcmp(cql_type, "double") == 0)
                pg_type = "double precision";
            else if (strcmp(cql_type, "boolean") == 0)
                pg_type = "boolean";
            else if (strcmp(cql_type, "timestamp") == 0)
                pg_type = "timestamp with time zone";
            else if (strcmp(cql_type, "date") == 0)
                pg_type = "date";
            else if (strcmp(cql_type, "time") == 0)
                pg_type = "time";
            else if (strcmp(cql_type, "blob") == 0)
                pg_type = "bytea";
            else if (strcmp(cql_type, "inet") == 0)
                pg_type = "inet";
            else if (strcmp(cql_type, "decimal") == 0 || strcmp(cql_type, "varint") == 0)
                pg_type = "numeric";
            else
                pg_type = "text";  /* Default fallback */

            appendStringInfo(&cmd, "    %s %s,\n",
                             quote_identifier(column_name), pg_type);

            /* Track partition key columns */
            if (kind != NULL && strcmp(kind, "partition_key") == 0)
            {
                if (pk_cols == NULL)
                    pk_cols = pstrdup(column_name);
                else
                {
                    char *new_pk = psprintf("%s, %s", pk_cols, column_name);
                    pfree(pk_cols);
                    pk_cols = new_pk;
                }
            }
        }

        /* Finish last table */
        if (in_table)
        {
            cmd.len -= 2;
            cmd.data[cmd.len] = '\0';

            appendStringInfo(&cmd,
                             "\n) SERVER %s\n"
                             "OPTIONS (keyspace '%s', \"table\" '%s'",
                             quote_identifier(server->servername),
                             stmt->remote_schema,
                             current_table);

            if (pk_cols != NULL)
                appendStringInfo(&cmd, ", primary_key '%s'", pk_cols);

            appendStringInfoString(&cmd, ");");

            commands = lappend(commands, pstrdup(cmd.data));
        }

        scylla_free_iterator(iterator);
    }

    scylla_free_result(result);
    scylla_disconnect(conn, NULL);

    return commands;
}
