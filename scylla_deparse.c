/*-------------------------------------------------------------------------
 *
 * scylla_deparse.c
 *        CQL query generation and WHERE clause deparsing for ScyllaDB FDW
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        scylla_fdw/scylla_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "scylla_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

/*
 * Context for deparseExpr
 */
typedef struct DeparseContext
{
    StringInfo  buf;            /* Output buffer */
    PlannerInfo *root;          /* Planner info */
    RelOptInfo  *foreignrel;    /* Foreign relation */
    RelOptInfo  *scanrel;       /* Scan relation */
    List       *params_list;    /* List of param references */
    bool        can_pushdown;   /* Can this expr be pushed down? */
} DeparseContext;

/* Local function prototypes */
static void deparseExpr(Expr *node, DeparseContext *context);
static void deparseVar(Var *node, DeparseContext *context);
static void deparseConst(Const *node, DeparseContext *context);
static void deparseBoolExpr(BoolExpr *node, DeparseContext *context);
static void deparseOpExpr(OpExpr *node, DeparseContext *context);
static void deparseNullTest(NullTest *node, DeparseContext *context);
static void deparseRelabelType(RelabelType *node, DeparseContext *context);
static bool is_pushdown_safe_type(Oid typeid);
static const char *get_cql_operator(Oid opno);
static char *cql_quote_literal(const char *str);
static char *cql_quote_identifier(const char *ident);
static bool needs_allow_filtering(PlannerInfo *root, RelOptInfo *baserel,
                                   ScyllaFdwRelationInfo *fpinfo,
                                   List *remote_conds, Relation rel);

/*
 * We don't use a hardcoded operator table because operator OIDs can vary.
 * Instead, we check operators dynamically in get_cql_operator().
 */

/*
 * scylla_get_options
 *        Extract option lists from foreign table catalog entries
 */
void
scylla_get_options(Oid foreigntableid, List **server_options,
                   List **table_options, List **user_options)
{
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user;
    Oid         serverid;
    Oid         userid = GetUserId();

    table = GetForeignTable(foreigntableid);
    serverid = table->serverid;
    server = GetForeignServer(serverid);

    *server_options = server->options;
    *table_options = table->options;

    PG_TRY();
    {
        user = GetUserMapping(userid, serverid);
        *user_options = user->options;
    }
    PG_CATCH();
    {
        FlushErrorState();
        *user_options = NIL;
    }
    PG_END_TRY();
}

/*
 * scylla_extract_options
 *        Extract individual option values into fpinfo struct
 */
void
scylla_extract_options(List *server_opts, List *table_opts,
                       List *user_opts, ScyllaFdwRelationInfo *fpinfo)
{
    ListCell   *lc;

    /* Set defaults */
    fpinfo->host = DEFAULT_HOST;
    fpinfo->port = DEFAULT_PORT;
    fpinfo->username = NULL;
    fpinfo->password = NULL;
    fpinfo->keyspace = NULL;
    fpinfo->table = NULL;
    fpinfo->primary_key = NULL;
    fpinfo->clustering_key = NULL;
    fpinfo->consistency = DEFAULT_CONSISTENCY;

    /* Process server options */
    foreach(lc, server_opts)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, OPT_HOST) == 0)
            fpinfo->host = defGetString(def);
        else if (strcmp(def->defname, OPT_PORT) == 0)
            fpinfo->port = atoi(defGetString(def));
        else if (strcmp(def->defname, OPT_CONSISTENCY) == 0)
            fpinfo->consistency = defGetString(def);
    }

    /* Process table options */
    foreach(lc, table_opts)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, OPT_KEYSPACE) == 0)
            fpinfo->keyspace = defGetString(def);
        else if (strcmp(def->defname, OPT_TABLE) == 0)
            fpinfo->table = defGetString(def);
        else if (strcmp(def->defname, OPT_PRIMARY_KEY) == 0)
            fpinfo->primary_key = defGetString(def);
        else if (strcmp(def->defname, OPT_CLUSTERING_KEY) == 0)
            fpinfo->clustering_key = defGetString(def);
    }

    /* Process user options */
    foreach(lc, user_opts)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, OPT_USERNAME) == 0)
            fpinfo->username = defGetString(def);
        else if (strcmp(def->defname, OPT_PASSWORD) == 0)
            fpinfo->password = defGetString(def);
    }
}

/*
 * scylla_classify_conditions
 *        Classify restriction clauses into pushdown and local categories
 */
void
scylla_classify_conditions(PlannerInfo *root, RelOptInfo *baserel,
                           List *input_conds, List **remote_conds,
                           List **local_conds)
{
    ListCell   *lc;

    *remote_conds = NIL;
    *local_conds = NIL;

    foreach(lc, input_conds)
    {
        RestrictInfo *ri = lfirst_node(RestrictInfo, lc);

        if (scylla_is_foreign_expr(root, baserel, ri->clause))
            *remote_conds = lappend(*remote_conds, ri);
        else
            *local_conds = lappend(*local_conds, ri);
    }
}

/*
 * scylla_is_foreign_expr
 *        Check if an expression can be pushed down to ScyllaDB
 */
bool
scylla_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr)
{
    /*
     * ScyllaDB has limited WHERE clause support compared to PostgreSQL.
     * We can push down:
     *  - Equality conditions on partition key columns (required)
     *  - Range conditions on clustering key columns
     *  - Simple boolean expressions combining the above
     *
     * We cannot push down:
     *  - Arbitrary function calls
     *  - LIKE patterns (ScyllaDB has limited support)
     *  - Subqueries
     *  - Aggregates
     */

    if (expr == NULL)
        return false;

    switch (nodeTag(expr))
    {
        case T_Var:
            {
                Var *var = (Var *) expr;
                /* Variable must belong to our foreign table */
                if (bms_is_member(var->varno, baserel->relids) &&
                    var->varattno > 0)
                    return is_pushdown_safe_type(var->vartype);
                return false;
            }

        case T_Const:
            {
                Const *c = (Const *) expr;
                return is_pushdown_safe_type(c->consttype);
            }

        case T_OpExpr:
            {
                OpExpr *op = (OpExpr *) expr;
                ListCell *lc;

                /* Check if operator is supported */
                if (get_cql_operator(op->opno) == NULL)
                    return false;

                /* Check all arguments */
                foreach(lc, op->args)
                {
                    if (!scylla_is_foreign_expr(root, baserel, lfirst(lc)))
                        return false;
                }
                return true;
            }

        case T_BoolExpr:
            {
                BoolExpr *b = (BoolExpr *) expr;
                ListCell *lc;

                /* ScyllaDB supports AND but not OR in WHERE */
                if (b->boolop == OR_EXPR)
                    return false;

                foreach(lc, b->args)
                {
                    if (!scylla_is_foreign_expr(root, baserel, lfirst(lc)))
                        return false;
                }
                return true;
            }

        case T_NullTest:
            /* ScyllaDB doesn't support IS NULL / IS NOT NULL in WHERE */
            return false;

        case T_RelabelType:
            {
                RelabelType *r = (RelabelType *) expr;
                return scylla_is_foreign_expr(root, baserel, r->arg);
            }

        default:
            return false;
    }
}

/*
 * scylla_build_select_query
 *        Build CQL SELECT query for a foreign table scan
 */
char *
scylla_build_select_query(PlannerInfo *root, RelOptInfo *baserel,
                          ScyllaFdwRelationInfo *fpinfo,
                          List *tlist, List *remote_conds,
                          List **retrieved_attrs)
{
    StringInfoData buf;
    RangeTblEntry *rte;
    Relation    rel;
    bool        first;
    ListCell   *lc;
    int         i;

    *retrieved_attrs = NIL;

    rte = planner_rt_fetch(baserel->relid, root);
    rel = table_open(rte->relid, NoLock);

    initStringInfo(&buf);

    /* SELECT */
    appendStringInfoString(&buf, "SELECT ");

    /* Build column list */
    first = true;
    for (i = 1; i <= RelationGetDescr(rel)->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), i - 1);

        if (attr->attisdropped)
            continue;

        /* Check if this column is needed */
        if (fpinfo->attrs_used == NULL ||
            bms_is_member(i - FirstLowInvalidHeapAttributeNumber, fpinfo->attrs_used))
        {
            if (!first)
                appendStringInfoString(&buf, ", ");
            appendStringInfoString(&buf, cql_quote_identifier(NameStr(attr->attname)));
            *retrieved_attrs = lappend_int(*retrieved_attrs, i);
            first = false;
        }
    }

    /* If no columns selected, select first non-dropped column */
    if (first)
    {
        for (i = 1; i <= RelationGetDescr(rel)->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), i - 1);
            if (!attr->attisdropped)
            {
                appendStringInfoString(&buf, cql_quote_identifier(NameStr(attr->attname)));
                *retrieved_attrs = lappend_int(*retrieved_attrs, i);
                break;
            }
        }
    }

    /* FROM */
    appendStringInfo(&buf, " FROM %s.%s",
                     cql_quote_identifier(fpinfo->keyspace),
                     cql_quote_identifier(fpinfo->table));

    /* WHERE */
    if (remote_conds != NIL)
    {
        appendStringInfoString(&buf, " WHERE ");
        first = true;

        foreach(lc, remote_conds)
        {
            Expr *expr = (Expr *) lfirst(lc);
            DeparseContext context;

            if (!first)
                appendStringInfoString(&buf, " AND ");

            context.buf = &buf;
            context.root = root;
            context.foreignrel = baserel;
            context.scanrel = baserel;
            context.params_list = NIL;
            context.can_pushdown = true;

            deparseExpr(expr, &context);
            first = false;
        }
    }

    /* Check if we need ALLOW FILTERING */
    if (needs_allow_filtering(root, baserel, fpinfo, remote_conds, rel))
    {
        appendStringInfoString(&buf, " ALLOW FILTERING");
    }

    table_close(rel, NoLock);

    return buf.data;
}

/*
 * scylla_build_insert_query
 *        Build CQL INSERT query
 */
char *
scylla_build_insert_query(Relation rel, List *target_attrs)
{
    StringInfoData buf;
    TupleDesc   tupdesc = RelationGetDescr(rel);
    bool        first;
    int         attnum;
    int         i;
    ForeignTable *table;
    char       *keyspace = NULL;
    char       *tablename = NULL;
    ListCell   *lc;
    int         num_attrs;
    Form_pg_attribute attr;

    table = GetForeignTable(RelationGetRelid(rel));
    foreach(lc, table->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_KEYSPACE) == 0)
            keyspace = defGetString(def);
        else if (strcmp(def->defname, OPT_TABLE) == 0)
            tablename = defGetString(def);
    }

    initStringInfo(&buf);

    appendStringInfo(&buf, "INSERT INTO %s.%s (",
                     cql_quote_identifier(keyspace),
                     cql_quote_identifier(tablename));

    /* Column names */
    first = true;
    foreach(lc, target_attrs)
    {
        attnum = lfirst_int(lc);
        attr = TupleDescAttr(tupdesc, attnum - 1);

        if (!first)
            appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, cql_quote_identifier(NameStr(attr->attname)));
        first = false;
    }

    appendStringInfoString(&buf, ") VALUES (");

    /* Placeholders - just need count, not the values */
    num_attrs = list_length(target_attrs);
    first = true;
    for (i = 0; i < num_attrs; i++)
    {
        if (!first)
            appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, "?");
        first = false;
    }

    appendStringInfoString(&buf, ")");

    return buf.data;
}

/*
 * scylla_build_update_query
 *        Build CQL UPDATE query
 */
char *
scylla_build_update_query(Relation rel, List *target_attrs,
                          int *pk_attrs, int num_pk_attrs)
{
    StringInfoData buf;
    TupleDesc   tupdesc = RelationGetDescr(rel);
    bool        first;
    bool        is_pk;
    int         i;
    int         attnum;
    ForeignTable *table;
    char       *keyspace = NULL;
    char       *tablename = NULL;
    ListCell   *lc;
    Form_pg_attribute attr;

    table = GetForeignTable(RelationGetRelid(rel));
    foreach(lc, table->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_KEYSPACE) == 0)
            keyspace = defGetString(def);
        else if (strcmp(def->defname, OPT_TABLE) == 0)
            tablename = defGetString(def);
    }

    initStringInfo(&buf);

    appendStringInfo(&buf, "UPDATE %s.%s SET ",
                     cql_quote_identifier(keyspace),
                     cql_quote_identifier(tablename));

    /* SET clause */
    first = true;
    foreach(lc, target_attrs)
    {
        attnum = lfirst_int(lc);
        attr = TupleDescAttr(tupdesc, attnum - 1);
        is_pk = false;

        /* Skip primary key columns in SET clause */
        for (i = 0; i < num_pk_attrs; i++)
        {
            if (pk_attrs[i] == attnum)
            {
                is_pk = true;
                break;
            }
        }
        if (is_pk)
            continue;

        if (!first)
            appendStringInfoString(&buf, ", ");
        appendStringInfo(&buf, "%s = ?",
                         cql_quote_identifier(NameStr(attr->attname)));
        first = false;
    }

    /* WHERE clause */
    appendStringInfoString(&buf, " WHERE ");
    first = true;
    for (i = 0; i < num_pk_attrs; i++)
    {
        attr = TupleDescAttr(tupdesc, pk_attrs[i] - 1);

        if (!first)
            appendStringInfoString(&buf, " AND ");
        appendStringInfo(&buf, "%s = ?",
                         cql_quote_identifier(NameStr(attr->attname)));
        first = false;
    }

    return buf.data;
}

/*
 * scylla_build_delete_query
 *        Build CQL DELETE query
 */
char *
scylla_build_delete_query(Relation rel, int *pk_attrs, int num_pk_attrs)
{
    StringInfoData buf;
    TupleDesc   tupdesc = RelationGetDescr(rel);
    bool        first;
    int         i;
    ForeignTable *table;
    char       *keyspace = NULL;
    char       *tablename = NULL;
    ListCell   *lc;
    Form_pg_attribute attr;

    table = GetForeignTable(RelationGetRelid(rel));
    foreach(lc, table->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, OPT_KEYSPACE) == 0)
            keyspace = defGetString(def);
        else if (strcmp(def->defname, OPT_TABLE) == 0)
            tablename = defGetString(def);
    }

    initStringInfo(&buf);

    appendStringInfo(&buf, "DELETE FROM %s.%s WHERE ",
                     cql_quote_identifier(keyspace),
                     cql_quote_identifier(tablename));

    first = true;
    for (i = 0; i < num_pk_attrs; i++)
    {
        attr = TupleDescAttr(tupdesc, pk_attrs[i] - 1);

        if (!first)
            appendStringInfoString(&buf, " AND ");
        appendStringInfo(&buf, "%s = ?",
                         cql_quote_identifier(NameStr(attr->attname)));
        first = false;
    }

    return buf.data;
}

/*
 * deparseExpr
 *        Deparse an expression into CQL
 */
static void
deparseExpr(Expr *node, DeparseContext *context)
{
    if (node == NULL)
        return;

    switch (nodeTag(node))
    {
        case T_Var:
            deparseVar((Var *) node, context);
            break;
        case T_Const:
            deparseConst((Const *) node, context);
            break;
        case T_OpExpr:
            deparseOpExpr((OpExpr *) node, context);
            break;
        case T_BoolExpr:
            deparseBoolExpr((BoolExpr *) node, context);
            break;
        case T_NullTest:
            deparseNullTest((NullTest *) node, context);
            break;
        case T_RelabelType:
            deparseRelabelType((RelabelType *) node, context);
            break;
        default:
            elog(ERROR, "unsupported expression type for CQL deparse: %d",
                 (int) nodeTag(node));
            break;
    }
}

/*
 * deparseVar
 *        Deparse a Var node
 */
static void
deparseVar(Var *node, DeparseContext *context)
{
    RangeTblEntry *rte;
    char       *colname;

    if (node->varattno == SelfItemPointerAttributeNumber)
    {
        /* Not supported */
        context->can_pushdown = false;
        return;
    }

    rte = planner_rt_fetch(node->varno, context->root);
    colname = get_attname(rte->relid, node->varattno, false);

    appendStringInfoString(context->buf, cql_quote_identifier(colname));
}

/*
 * deparseConst
 *        Deparse a Const node
 */
static void
deparseConst(Const *node, DeparseContext *context)
{
    Oid         typoutput;
    bool        typIsVarlena;
    char       *extval;

    if (node->constisnull)
    {
        appendStringInfoString(context->buf, "NULL");
        return;
    }

    getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
    extval = OidOutputFunctionCall(typoutput, node->constvalue);

    switch (node->consttype)
    {
        case BOOLOID:
            if (strcmp(extval, "t") == 0)
                appendStringInfoString(context->buf, "true");
            else
                appendStringInfoString(context->buf, "false");
            break;

        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            /* Numbers don't need quotes in CQL */
            appendStringInfoString(context->buf, extval);
            break;

        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        case NAMEOID:
            /* Strings need single quotes */
            appendStringInfoString(context->buf, cql_quote_literal(extval));
            break;

        case UUIDOID:
            /* UUIDs are unquoted in CQL */
            appendStringInfoString(context->buf, extval);
            break;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            /* Timestamps need single quotes */
            appendStringInfoString(context->buf, cql_quote_literal(extval));
            break;

        case DATEOID:
            appendStringInfoString(context->buf, cql_quote_literal(extval));
            break;

        case BYTEAOID:
            /* Blob in hex format */
            appendStringInfo(context->buf, "0x%s", extval + 2);  /* Skip \x prefix */
            break;

        default:
            appendStringInfoString(context->buf, cql_quote_literal(extval));
            break;
    }

    pfree(extval);
}

/*
 * deparseOpExpr
 *        Deparse an OpExpr node
 */
static void
deparseOpExpr(OpExpr *node, DeparseContext *context)
{
    const char *cql_op;
    Expr       *left;
    Expr       *right;

    cql_op = get_cql_operator(node->opno);
    if (cql_op == NULL)
    {
        context->can_pushdown = false;
        return;
    }

    if (list_length(node->args) != 2)
    {
        context->can_pushdown = false;
        return;
    }

    left = linitial(node->args);
    right = lsecond(node->args);

    deparseExpr(left, context);
    appendStringInfo(context->buf, " %s ", cql_op);
    deparseExpr(right, context);
}

/*
 * deparseBoolExpr
 *        Deparse a BoolExpr node
 */
static void
deparseBoolExpr(BoolExpr *node, DeparseContext *context)
{
    const char *op;
    bool        first;
    ListCell   *lc;

    switch (node->boolop)
    {
        case AND_EXPR:
            op = " AND ";
            break;
        case OR_EXPR:
            /* ScyllaDB doesn't support OR in WHERE */
            context->can_pushdown = false;
            return;
        case NOT_EXPR:
            /* NOT is only partially supported */
            context->can_pushdown = false;
            return;
        default:
            context->can_pushdown = false;
            return;
    }

    appendStringInfoChar(context->buf, '(');
    first = true;
    foreach(lc, node->args)
    {
        if (!first)
            appendStringInfoString(context->buf, op);
        deparseExpr((Expr *) lfirst(lc), context);
        first = false;
    }
    appendStringInfoChar(context->buf, ')');
}

/*
 * deparseNullTest
 *        Deparse a NullTest node
 */
static void
deparseNullTest(NullTest *node, DeparseContext *context)
{
    /* ScyllaDB doesn't support IS NULL / IS NOT NULL in WHERE */
    context->can_pushdown = false;
}

/*
 * deparseRelabelType
 *        Deparse a RelabelType node
 */
static void
deparseRelabelType(RelabelType *node, DeparseContext *context)
{
    deparseExpr(node->arg, context);
}

/*
 * get_cql_operator
 *        Get the CQL equivalent of a PostgreSQL operator
 */
static const char *
get_cql_operator(Oid opno)
{
    char       *opname;
    Oid         opnamespace;
    HeapTuple   tuple;
    Form_pg_operator oprform;

    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (!HeapTupleIsValid(tuple))
        return NULL;

    oprform = (Form_pg_operator) GETSTRUCT(tuple);
    opname = NameStr(oprform->oprname);
    opnamespace = oprform->oprnamespace;

    ReleaseSysCache(tuple);

    /* Only consider operators in pg_catalog */
    if (opnamespace != PG_CATALOG_NAMESPACE)
        return NULL;

    /* Map PostgreSQL operators to CQL */
    if (strcmp(opname, "=") == 0)
        return "=";
    if (strcmp(opname, "<") == 0)
        return "<";
    if (strcmp(opname, ">") == 0)
        return ">";
    if (strcmp(opname, "<=") == 0)
        return "<=";
    if (strcmp(opname, ">=") == 0)
        return ">=";
    if (strcmp(opname, "<>") == 0 || strcmp(opname, "!=") == 0)
        return "!=";  /* Note: CQL has limited != support */

    return NULL;
}

/*
 * is_pushdown_safe_type
 *        Check if a type can be safely pushed down
 */
static bool
is_pushdown_safe_type(Oid typeid)
{
    switch (typeid)
    {
        case BOOLOID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        case BYTEAOID:
        case UUIDOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case DATEOID:
        case TIMEOID:
        case INETOID:
            return true;
        default:
            return false;
    }
}

/*
 * cql_quote_literal
 *        Quote a string literal for CQL
 */
static char *
cql_quote_literal(const char *str)
{
    StringInfoData buf;
    const char *p;

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '\'');

    for (p = str; *p; p++)
    {
        if (*p == '\'')
            appendStringInfoString(&buf, "''");
        else
            appendStringInfoChar(&buf, *p);
    }

    appendStringInfoChar(&buf, '\'');
    return buf.data;
}

/*
 * cql_quote_identifier
 *        Quote an identifier for CQL
 */
static char *
cql_quote_identifier(const char *ident)
{
    StringInfoData buf;
    bool        need_quotes = false;
    const char *p;

    /* Check if quoting is needed */
    if (!((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_'))
        need_quotes = true;
    else
    {
        for (p = ident; *p; p++)
        {
            if (!(((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '_')))
            {
                need_quotes = true;
                break;
            }
        }
    }

    if (!need_quotes)
        return pstrdup(ident);

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '"');
    for (p = ident; *p; p++)
    {
        if (*p == '"')
            appendStringInfoString(&buf, "\"\"");
        else
            appendStringInfoChar(&buf, *p);
    }
    appendStringInfoChar(&buf, '"');

    return buf.data;
}

/*
 * scylla_quote_identifier
 *        Public version of cql_quote_identifier
 */
char *
scylla_quote_identifier(const char *ident)
{
    return cql_quote_identifier(ident);
}

/*
 * needs_allow_filtering
 *        Determine if ALLOW FILTERING clause is needed
 *
 * ALLOW FILTERING is required when:
 * 1. No WHERE clause at all
 * 2. Partition key columns are not all specified with = or IN operators
 * 3. Partition key columns use non-equality operators (<, >, <=, >=, !=)
 */
static bool
needs_allow_filtering(PlannerInfo *root, RelOptInfo *baserel,
                      ScyllaFdwRelationInfo *fpinfo,
                      List *remote_conds, Relation rel)
{
    TupleDesc   tupdesc = RelationGetDescr(rel);
    char       *pk_str = fpinfo->primary_key;
    char       *str;
    char       *token;
    char       *saveptr;
    char       *end;
    ListCell   *lc;
    bool        all_pk_have_equality = true;
    
    /* If no WHERE clause, we need ALLOW FILTERING */
    if (remote_conds == NIL)
        return true;
    
    /* If no primary key defined, be conservative and add ALLOW FILTERING */
    if (pk_str == NULL || pk_str[0] == '\0')
        return true;
    
    /* Parse each partition key column and check if it has equality condition */
    str = pstrdup(pk_str);
    for (token = strtok_r(str, ",", &saveptr);
         token != NULL;
         token = strtok_r(NULL, ",", &saveptr))
    {
        bool        found_equality = false;
        int         pk_attnum = 0;
        int         i;
        
        /* Trim whitespace */
        while (*token == ' ') token++;
        end = token + strlen(token) - 1;
        while (end > token && *end == ' ') *end-- = '\0';
        
        /* Find attribute number for this PK column */
        for (i = 0; i < tupdesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (strcmp(NameStr(attr->attname), token) == 0)
            {
                pk_attnum = i + 1;
                break;
            }
        }
        
        if (pk_attnum == 0)
            continue;  /* Column not found, skip */
        
        /* Check if this PK column appears in remote_conds with = or IN operator */
        foreach(lc, remote_conds)
        {
            Expr *expr = (Expr *) lfirst(lc);
            
            if (expr == NULL)
                continue;
            
            /* Check if this is an OpExpr (= operator) */
            if (IsA(expr, OpExpr))
            {
                OpExpr *opexpr = (OpExpr *) expr;
                Expr *left;
                Oid  opno = opexpr->opno;
                const char *op_str = get_cql_operator(opno);
                
                /* Check if operator is = */
                if (op_str != NULL && strcmp(op_str, "=") == 0)
                {
                    /* Check if left side is our PK column */
                    if (list_length(opexpr->args) >= 1)
                    {
                        left = linitial(opexpr->args);
                        
                        if (IsA(left, Var))
                        {
                            Var *var = (Var *) left;
                            if (var->varattno == pk_attnum &&
                                bms_is_member(var->varno, baserel->relids))
                            {
                                found_equality = true;
                                break;
                            }
                        }
                    }
                }
            }
            /* Check if this is a ScalarArrayOpExpr (IN operator) */
            else if (IsA(expr, ScalarArrayOpExpr))
            {
                ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) expr;
                Expr *left;
                
                /* ScalarArrayOpExpr with useOr=true represents IN() */
                if (saop->useOr)
                {
                    /* Check if left side is our PK column */
                    if (list_length(saop->args) >= 1)
                    {
                        left = linitial(saop->args);
                        
                        if (IsA(left, Var))
                        {
                            Var *var = (Var *) left;
                            if (var->varattno == pk_attnum &&
                                bms_is_member(var->varno, baserel->relids))
                            {
                                found_equality = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        /* If this PK column doesn't have equality condition, we need ALLOW FILTERING */
        if (!found_equality)
        {
            all_pk_have_equality = false;
            break;
        }
    }
    
    pfree(str);
    
    return !all_pk_have_equality;
}
