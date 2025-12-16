/*-------------------------------------------------------------------------
 *
 * scylla_fdw_helper.c
 *        Helper functions for ScyllaDB Foreign Data Wrapper
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_fdw_helper.c
 *
 *-------------------------------------------------------------------------
 */
#include "scylla_fdw.h"

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

#include <math.h>

/*
 * LOG2 macro for sorting cost estimation
 */
#ifndef LOG2
#define LOG2(x) (log(x) / 0.693147180559945)
#endif

/*
 * Default cost estimates for foreign table scans
 */
#define DEFAULT_FDW_STARTUP_COST    100.0
#define DEFAULT_FDW_TUPLE_COST      0.01
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * estimate_path_cost_size
 *        Estimate the cost and result size of a foreign scan
 *
 * This is a simplified cost model. A more sophisticated implementation
 * would query ScyllaDB for actual statistics.
 */
void
estimate_path_cost_size(PlannerInfo *root, RelOptInfo *baserel,
                        List *join_conds, List *pathkeys,
                        double *p_rows, int *p_width,
                        Cost *p_startup_cost, Cost *p_total_cost)
{
    ScyllaFdwRelationInfo *fpinfo = (ScyllaFdwRelationInfo *) baserel->fdw_private;
    double      rows;
    int         width;
    Cost        startup_cost;
    Cost        total_cost;
    Cost        cpu_per_tuple;
    double      selectivity;
    QualCost    qual_cost;
    double      ntuples;

    /*
     * Estimate the number of rows. If we have restriction quals, apply
     * selectivity estimates.
     */
    if (fpinfo != NULL && fpinfo->remote_conds != NIL)
    {
        /*
         * Apply selectivity for remote conditions. For equality on partition
         * key, this should be very selective. For other conditions, use
         * default estimates.
         */
        selectivity = clauselist_selectivity(root,
                                            fpinfo->remote_conds,
                                            baserel->relid,
                                            JOIN_INNER,
                                            NULL);
        ntuples = clamp_row_est(baserel->tuples * selectivity);
    }
    else
    {
        /*
         * No remote conditions, so we'll scan the whole table.
         * Use a default estimate if we don't have better info.
         */
        ntuples = (baserel->tuples > 0) ? baserel->tuples : 1000;
    }

    /* Apply local conditions selectivity */
    if (fpinfo != NULL && fpinfo->local_conds != NIL)
    {
        selectivity = clauselist_selectivity(root,
                                            fpinfo->local_conds,
                                            baserel->relid,
                                            JOIN_INNER,
                                            NULL);
        rows = clamp_row_est(ntuples * selectivity);
    }
    else
    {
        rows = ntuples;
    }

    /* Estimate width */
    width = baserel->reltarget->width;
    if (width <= 0)
        width = 100;  /* Default estimate */

    /*
     * Cost model:
     * - Startup cost: Connection setup + query preparation
     * - Per-tuple cost: Network transfer + local processing
     */
    startup_cost = DEFAULT_FDW_STARTUP_COST;

    /* Cost of network transfer depends on data volume */
    cpu_per_tuple = cpu_tuple_cost + DEFAULT_FDW_TUPLE_COST;

    /* Add cost of evaluating local conditions */
    if (fpinfo != NULL && fpinfo->local_conds != NIL)
    {
        cost_qual_eval(&qual_cost, fpinfo->local_conds, root);
        cpu_per_tuple += qual_cost.per_tuple;
        startup_cost += qual_cost.startup;
    }

    total_cost = startup_cost + cpu_per_tuple * ntuples;

    /* Add additional cost if we need to fetch more data than we return */
    if (ntuples > rows)
    {
        total_cost += (ntuples - rows) * cpu_tuple_cost * 0.5;
    }

    /* If sorted output is requested but we can't push down ORDER BY,
     * add an estimate for local sorting cost */
    if (pathkeys != NIL)
    {
        /* Rough estimate: sorting adds O(n log n) cost */
        double      sort_tuples = rows;
        Cost        sort_cost;
        
        if (sort_tuples > 1.0)
            sort_cost = sort_tuples * LOG2(sort_tuples) * cpu_operator_cost;
        else
            sort_cost = 0.0;
        
        startup_cost += sort_cost;
        total_cost += sort_cost;
    }

    *p_rows = rows;
    *p_width = width;
    *p_startup_cost = startup_cost;
    *p_total_cost = total_cost;
}

/*
 * scylla_get_useful_pathkeys
 *        Determine which pathkeys might be useful for ordering results
 *
 * ScyllaDB can provide sorted results on clustering columns, so we look
 * for pathkeys that match those columns.
 */
List *
scylla_get_useful_pathkeys(PlannerInfo *root, RelOptInfo *baserel)
{
    ScyllaFdwRelationInfo *fpinfo = (ScyllaFdwRelationInfo *) baserel->fdw_private;
    List       *useful_pathkeys = NIL;

    /*
     * If we have clustering key info, we could potentially push down ORDER BY.
     * For now, return empty list as this requires more complex implementation.
     */
    if (fpinfo == NULL || fpinfo->clustering_key == NULL)
        return NIL;

    /* Future enhancement: Parse clustering_key and create pathkeys */

    return useful_pathkeys;
}

/*
 * scylla_get_useful_ecs_for_relation
 *        Get useful equivalence classes for a foreign relation
 */
List *
scylla_get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *baserel)
{
    List       *dominated_ecs = NIL;
    List       *dominated_eclasses;
    ListCell   *lc;

    /* Get all ECs that mention this relation */
    dominated_eclasses = NIL;
    foreach(lc, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(lc);

        /* Skip if EC has been merged into parent */
        if (ec->ec_merged != NULL)
            continue;

        /* Skip if EC doesn't mention this relation */
        if (!bms_is_member(baserel->relid, ec->ec_relids))
            continue;

        dominated_eclasses = lappend(dominated_eclasses, ec);
    }

    return dominated_eclasses;
}

/*
 * apply_server_options
 *        Extract and apply server-level options
 */
void
apply_server_options(ScyllaFdwRelationInfo *fpinfo, ForeignServer *server)
{
    ListCell   *lc;

    foreach(lc, server->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, OPT_HOST) == 0)
            fpinfo->host = defGetString(def);
        else if (strcmp(def->defname, OPT_PORT) == 0)
            fpinfo->port = atoi(defGetString(def));
        else if (strcmp(def->defname, OPT_CONSISTENCY) == 0)
            fpinfo->consistency = defGetString(def);
    }
}

/*
 * apply_table_options
 *        Extract and apply table-level options
 */
void
apply_table_options(ScyllaFdwRelationInfo *fpinfo, ForeignTable *table)
{
    ListCell   *lc;

    foreach(lc, table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, OPT_KEYSPACE) == 0)
            fpinfo->keyspace = defGetString(def);
        else if (strcmp(def->defname, OPT_TABLE) == 0)
            fpinfo->table = defGetString(def);
        else if (strcmp(def->defname, OPT_PRIMARY_KEY) == 0)
            fpinfo->primary_key = defGetString(def);
        else if (strcmp(def->defname, OPT_CLUSTERING_KEY) == 0)
            fpinfo->clustering_key = defGetString(def);
    }
}

/*
 * merge_fdw_options
 *        Merge options from multiple sources with proper precedence
 */
void
merge_fdw_options(ScyllaFdwRelationInfo *fpinfo,
                  ForeignServer *server,
                  ForeignTable *table,
                  UserMapping *user)
{
    /* Start with defaults */
    fpinfo->host = DEFAULT_HOST;
    fpinfo->port = DEFAULT_PORT;
    fpinfo->consistency = DEFAULT_CONSISTENCY;
    fpinfo->keyspace = NULL;
    fpinfo->table = NULL;
    fpinfo->primary_key = NULL;
    fpinfo->clustering_key = NULL;
    fpinfo->username = NULL;
    fpinfo->password = NULL;

    /* Apply server options */
    if (server != NULL)
        apply_server_options(fpinfo, server);

    /* Apply table options */
    if (table != NULL)
        apply_table_options(fpinfo, table);

    /* Apply user mapping options */
    if (user != NULL)
    {
        ListCell   *lc;

        foreach(lc, user->options)
        {
            DefElem    *def = (DefElem *) lfirst(lc);

            if (strcmp(def->defname, OPT_USERNAME) == 0)
                fpinfo->username = defGetString(def);
            else if (strcmp(def->defname, OPT_PASSWORD) == 0)
                fpinfo->password = defGetString(def);
        }
    }
}

/*
 * is_valid_option
 *        Check if an option is valid for a given context
 */
bool
is_valid_option(const char *option, Oid context)
{
    /* Server options */
    if (context == ForeignServerRelationId)
    {
        if (strcmp(option, OPT_HOST) == 0 ||
            strcmp(option, OPT_PORT) == 0 ||
            strcmp(option, OPT_SSL) == 0 ||
            strcmp(option, OPT_SSL_CERT) == 0 ||
            strcmp(option, OPT_SSL_KEY) == 0 ||
            strcmp(option, OPT_SSL_CA) == 0 ||
            strcmp(option, OPT_CONNECT_TIMEOUT) == 0 ||
            strcmp(option, OPT_REQUEST_TIMEOUT) == 0 ||
            strcmp(option, OPT_CONSISTENCY) == 0 ||
            strcmp(option, OPT_PROTOCOL_VERSION) == 0)
            return true;
    }

    /* User mapping options */
    if (context == UserMappingRelationId)
    {
        if (strcmp(option, OPT_USERNAME) == 0 ||
            strcmp(option, OPT_PASSWORD) == 0)
            return true;
    }

    /* Table options */
    if (context == ForeignTableRelationId)
    {
        if (strcmp(option, OPT_KEYSPACE) == 0 ||
            strcmp(option, OPT_TABLE) == 0 ||
            strcmp(option, OPT_PRIMARY_KEY) == 0 ||
            strcmp(option, OPT_CLUSTERING_KEY) == 0)
            return true;
    }

    return false;
}

/*
 * get_relation_column_count
 *        Get the number of columns in a relation
 */
int
get_relation_column_count(Relation rel)
{
    TupleDesc   tupdesc = RelationGetDescr(rel);
    int         count = 0;
    int         i;

    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (!attr->attisdropped)
            count++;
    }

    return count;
}

/*
 * get_column_by_name
 *        Find a column's attribute number by name
 */
AttrNumber
get_column_by_name(Relation rel, const char *colname)
{
    TupleDesc   tupdesc = RelationGetDescr(rel);
    int         i;

    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (!attr->attisdropped &&
            strcmp(NameStr(attr->attname), colname) == 0)
            return i + 1;
    }

    return InvalidAttrNumber;
}

/*
 * parse_column_list
 *        Parse a comma-separated list of column names into attribute numbers
 */
List *
parse_column_list(Relation rel, const char *collist)
{
    List       *result = NIL;
    char       *str;
    char       *token;
    char       *saveptr;

    if (collist == NULL || collist[0] == '\0')
        return NIL;

    str = pstrdup(collist);

    for (token = strtok_r(str, ",", &saveptr);
         token != NULL;
         token = strtok_r(NULL, ",", &saveptr))
    {
        AttrNumber  attnum;

        /* Trim whitespace */
        while (*token == ' ' || *token == '\t')
            token++;
        char *end = token + strlen(token) - 1;
        while (end > token && (*end == ' ' || *end == '\t'))
            *end-- = '\0';

        attnum = get_column_by_name(rel, token);
        if (attnum != InvalidAttrNumber)
            result = lappend_int(result, attnum);
    }

    pfree(str);
    return result;
}

/*
 * is_partition_key_column
 *        Check if a column is part of the partition key
 */
bool
is_partition_key_column(ScyllaFdwRelationInfo *fpinfo, AttrNumber attnum, Relation rel)
{
    List       *pk_cols;
    ListCell   *lc;

    if (fpinfo->primary_key == NULL)
        return false;

    pk_cols = parse_column_list(rel, fpinfo->primary_key);
    
    foreach(lc, pk_cols)
    {
        if (lfirst_int(lc) == attnum)
            return true;
    }

    return false;
}

/*
 * is_clustering_key_column
 *        Check if a column is part of the clustering key
 */
bool
is_clustering_key_column(ScyllaFdwRelationInfo *fpinfo, AttrNumber attnum, Relation rel)
{
    List       *ck_cols;
    ListCell   *lc;

    if (fpinfo->clustering_key == NULL)
        return false;

    ck_cols = parse_column_list(rel, fpinfo->clustering_key);
    
    foreach(lc, ck_cols)
    {
        if (lfirst_int(lc) == attnum)
            return true;
    }

    return false;
}
