/*-------------------------------------------------------------------------
 *
 * planner.c
 *	  The query optimizer external interface.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planner.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/genam.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "jit/jit.h"
#include "lib/bipartite_match.h"
#include "lib/knapsack.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "nodes/supportnodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/paramassign.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "partitioning/partdesc.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

/* GUC parameters */
double		cursor_tuple_fraction = DEFAULT_CURSOR_TUPLE_FRACTION;
int			debug_parallel_query = DEBUG_PARALLEL_OFF;
bool		parallel_leader_participation = true;
bool		enable_distinct_reordering = true;


/*Temporary GUC parameters, they will be transferred to the extension*/
int			constraint_exclusion = 1;
double      seq_page_cost = 1.0;
double		random_page_cost = 4.0;
double		cpu_tuple_cost = 0.01;
double		cpu_index_tuple_cost = 0.005;
double		cpu_operator_cost = 0.025;
double		parallel_tuple_cost = 0.1;
double		parallel_setup_cost = 1000.0;
double		recursive_worktable_factor = 10.0;
int			effective_cache_size = 524288;
int			max_parallel_workers_per_gather = 2;
bool		enable_seqscan = true;
bool		enable_indexscan = true;
bool		enable_indexonlyscan = true;
bool		enable_bitmapscan = true;
bool		enable_tidscan = true;
bool		enable_sort = true;
bool		enable_incremental_sort = true;
bool		enable_hashagg = true;
bool		enable_nestloop = true;
bool		enable_material = true;
bool		enable_memoize = true;
bool		enable_mergejoin = true;
bool		enable_hashjoin = true;
bool		enable_gathermerge = true;
bool		enable_partitionwise_join = false;
bool		enable_partitionwise_aggregate = false;
bool		enable_parallel_append = true;
bool		enable_parallel_hash = true;
bool		enable_partition_pruning = true;
bool		enable_presorted_aggregate = true;
bool		enable_async_append = true;

int			Geqo_effort;
int			Geqo_pool_size;
int			Geqo_generations;
double		Geqo_selection_bias;
double		Geqo_seed;
bool		enable_geqo = false;	/* just in case GUC doesn't set it */
int			geqo_threshold;
int			min_parallel_table_scan_size;
int			min_parallel_index_scan_size;

int			from_collapse_limit;
int			join_collapse_limit;

bool		enable_group_by_reordering = true;




/* Hook for plugins to get control in planner() */
planner_hook_type planner_hook = NULL;

/*hooks from clasuses.h*/
expression_returns_set_rows_hook_type expression_returns_set_rows_hook = NULL;
contain_subplans_hook_type contain_subplans_hook = NULL;
is_pseudo_constant_clause_hook_type is_pseudo_constant_clause_hook = NULL;
NumRelids_hook_type NumRelids_hook = NULL;

/*hooks from optimizer.h*/
expression_planner_hook_type expression_planner_hook = NULL;
expression_planner_with_deps_hook_type expression_planner_with_deps_hook = NULL;
pull_varattnos_hook_type pull_varattnos_hook = NULL;



/*****************************************************************************
 *
 *	   Query optimizer entry point
 *
 * To support loadable plugins that monitor or modify planner behavior,
 * we provide a hook variable that lets a plugin get control before and
 * after the standard planning process.  The plugin would normally call
 * standard_planner().
 *
 * Note to plugin authors: standard_planner() scribbles on its Query input,
 * so you'd better copy that data structure if you want to plan more than once.
 *
 *****************************************************************************/
PlannedStmt *
planner(Query *parse, const char *query_string, int cursorOptions,
		ParamListInfo boundParams)
{
	PlannedStmt *result;

	if (planner_hook)
		result = (*planner_hook) (parse, query_string, cursorOptions, boundParams);
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("planner have not implemented")));
		result = NULL;//standard_planner(parse, query_string, cursorOptions, boundParams);
	}

	return result;
}