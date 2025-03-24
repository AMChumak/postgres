#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "postmaster/bgworker.h"
#include <string.h>
#include <storage/lwlock.h>
#include "tcop/tcopprot.h"
#include <storage/shmem.h>
#include "postmaster/interrupt.h"
#include "miscadmin.h"
#include <storage/ipc.h>
#include "access/relation.h"
#include "executor/spi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "utils/snapmgr.h"
#include "utils/guc.h"

static ExecutorStart_hook_type prev_ExecutorStart = NULL;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


typedef struct SharedStruct {
    LWLock* lock;
    int count;
} SharedStruct;

SharedStruct *sharedStruct;

char *message = "advantage C hello, world!";
static bool hello_logs = true;

struct node {
    char *name;
    int state;
};

struct node my_node = {NULL, 42};
struct node my_boot_node = {NULL, 42};

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(hello_cworld);

PG_FUNCTION_INFO_V1(get_logs_count);

PG_FUNCTION_INFO_V1(access_scan_column);

static void hello_ExecutorStart(QueryDesc *queryDesc, int eflags);

static void custom_shmem_request(void);

static void custom_shmem_startup(void);

static void create_bgworker(void);

PGDLLEXPORT int hello_bg_main(Datum main_arg);


Datum hello_cworld(PG_FUNCTION_ARGS) {
    PG_RETURN_TEXT_P(cstring_to_text(message));
}

Datum get_logs_count(PG_FUNCTION_ARGS) {
    int32 result;

    LWLockAcquire(sharedStruct->lock, LW_SHARED);
    result = sharedStruct->count;
    LWLockRelease(sharedStruct->lock);

    PG_RETURN_INT32(result);
}


void _PG_init(void) {
    const char *node_typename = "node";
    const char *node_signature = "string name; int state";
    DefineCustomStructType(node_typename, node_signature);
    DefineCustomStructVariable("hello_world.my_node", "physical node", "this structure describes on node of cluster",
        node_typename, &my_node, &my_boot_node, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("hello_world.log_enabled", "this flag turns logging on/off",\
     "this flag turns logging on/off - if true then logging is on else logging is off",\
        &hello_logs, true, PGC_USERSET, 0, NULL, NULL, NULL);

    MarkGUCPrefixReserved("hello_world");

    create_bgworker();

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = custom_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = custom_shmem_startup;

    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = hello_ExecutorStart;
}


static void hello_ExecutorStart(QueryDesc *queryDesc, int eflags) {
    if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (hello_logs)
    ereport(LOG, (errmsg("hello from hook: start executing query"), errdetail("query: %s", queryDesc->sourceText), errhint("Hi also from hint!!!")) );
}


static void custom_shmem_request(void) {
    if (prev_shmem_request_hook) 
        prev_shmem_request_hook();

    RequestAddinShmemSpace(MAXALIGN(sizeof(SharedStruct)));
    RequestNamedLWLockTranche("communication", 1);
}

static void custom_shmem_startup(void) {
    bool found;

    if(prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    sharedStruct = ShmemInitStruct("SharedStruct", sizeof(SharedStruct), &found);
    
    if(!found) {
        sharedStruct->count = 0;
        sharedStruct->lock =
            &(GetNamedLWLockTranche("communication"))->lock;
    }

    LWLockRelease(AddinShmemInitLock);
}





static void create_bgworker(void){
    BackgroundWorker bgWorker;

    memset(&bgWorker, 0, sizeof(BackgroundWorker));
    bgWorker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    bgWorker.bgw_start_time = BgWorkerStart_PostmasterStart;
    bgWorker.bgw_restart_time = BGW_NEVER_RESTART;
    strcpy(bgWorker.bgw_library_name, "hello_world");
    strcpy(bgWorker.bgw_function_name, "hello_bg_main");
    strcpy(bgWorker.bgw_name, "hello_world - healthcheck");
    strcpy(bgWorker.bgw_type, "hello_world - healthcheck - type");

    RegisterBackgroundWorker(&bgWorker);

}

int hello_bg_main(Datum main_arg){

    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    //pqsignal(SIGTERM, die);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();

    while (!ShutdownRequestPending)
	{
		CHECK_FOR_INTERRUPTS(); //????? for what purposes

		ereport(LOG, errmsg("pg_hello_health_check"));
        

        //increment counter

        LWLockAcquire(sharedStruct->lock, LW_EXCLUSIVE);
        (sharedStruct->count)++;
        LWLockRelease(sharedStruct->lock);

        LWLockAcquire(sharedStruct->lock, LW_SHARED);
        ereport(LOG, errmsg("count - %d", sharedStruct->count ));
        LWLockRelease(sharedStruct->lock);



		sleep(3);
	}

    return 0;
}



//scan function

Datum access_scan_column(PG_FUNCTION_ARGS) {
    Oid relationOid = PG_GETARG_OID(0);
    text *rawColumnName = PG_GETARG_TEXT_PP(1);

    char* columnName = text_to_cstring(rawColumnName);

    Relation scannedRelation = relation_open(relationOid, AccessShareLock);

    TupleDesc tupleDescriptor = RelationGetDescr(scannedRelation);

    int columnNum = SPI_fnumber(tupleDescriptor, columnName);

    if (columnName == SPI_ERROR_NOATTRIBUTE) return;

    TableScanDesc tableDescriptor = table_beginscan(scannedRelation, GetActiveSnapshot(), 0, NULL);
    
    ereport(LOG, errmsg("init successfuly finished!"));

    for(;;){
        HeapTuple nextTuple = heap_getnext(tableDescriptor, ForwardScanDirection);
        if(nextTuple == NULL) break;
        bool isNullAttr = false;
        Datum attribute =  heap_getattr(nextTuple, columnNum, tupleDescriptor, &isNullAttr);
        if(!isNullAttr){
            elog(INFO, "Column %s: \"%s\"", columnName, TextDatumGetCString(attribute));
        }
    }
    table_endscan(tableDescriptor);
    relation_close(scannedRelation, NoLock);
}