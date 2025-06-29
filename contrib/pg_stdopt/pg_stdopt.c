#include "postgres.h"
#include "utils/builtins.h"
#include "c.h"
#include "optimizer/planner.h"


PG_MODULE_MAGIC;


PlannedStmt *external_std_planner (Query *parse,
										   const char *query_string,
										   int cursorOptions,
										   ParamListInfo boundParams);

planner_hook_type prev_planner_hook = NULL;

void _PG_init(void) {
    //planner_hook
    prev_planner_hook = planner_hook;
    planner_hook = external_std_planner;
}


PlannedStmt *external_std_planner (Query *parse,
										   const char *query_string,
										   int cursorOptions,
										   ParamListInfo boundParams) {
    return standard_planner(parse, query_string, cursorOptions, boundParams);
}


