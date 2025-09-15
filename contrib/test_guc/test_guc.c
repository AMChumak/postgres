#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/elog.h"
#include "string.h"

PG_MODULE_MAGIC;

struct LogLevel
{
	char *name;
	int level;
};

struct LogLevel logLevelsMapping[5];
struct LogLevel logLevelsMappingBoot[5];


struct NodeConfig
{
	char *name;
	char *ip;
	int port;
	int max_connections;
	bool enable_connections;
};

struct NodeArray
{
	struct NodeConfig *data;
	int size;
};

struct ClusterConfig
{
	char *name;
	char *leader;
	struct NodeArray nodes;
};

struct ClusterConfig clusterConfig;
struct ClusterConfig clusterConfigBoot;


bool validateCluster(void *newValue, void **extra, GucSource source);

void _PG_init(void);

void
_PG_init(void)
{
	DefineCustomCompositeType("ext.loglevelmap", "string name; int level");

	DefineCustomCompositeVariable("ext.loglevels",
								  "Mapping between level names and values",
								  NULL,
								  "ext.loglevelmap[5]",
								  &logLevelsMapping,
								  &logLevelsMappingBoot,
								  PGC_USERSET,
								  0,
								  NULL,
								  NULL,
								  NULL);



	DefineCustomCompositeType("ext.nodeConfig", "string name; string ip; int port; int max_connections; bool enable_connections");
	DefineCustomCompositeType("ext.clusterConfig", "string name; string leader; ext.nodeConfig[] nodes");

	DefineCustomCompositeVariable("ext.cluster",
								  "Example of multi-level structure",
								  NULL,
								  "ext.clusterConfig",
								  &clusterConfig,
								  &clusterConfigBoot,
								  PGC_USERSET,
								  0,
								  &validateCluster,
								  NULL,
								  NULL);

	MarkGUCPrefixReserved("ext");
}


bool
validateCluster(void *newValueRaw, void **extra, GucSource source)
{
	struct ClusterConfig *newValue = (struct ClusterConfig *)newValueRaw;
	bool found_leader = false;
	int  max_connections = 0;

	if (newValue->name == NULL)
		return true;

	for (int i = 0; i < newValue->nodes.size; i++)
	{
		if (strcmp(newValue->leader, newValue->nodes.data[i].name) == 0)
		{
			found_leader = true;
			max_connections = newValue->nodes.data[i].max_connections;
			break;
		}
	}

	if (!found_leader)
		return false;

	for (int i = 0; i < newValue->nodes.size; i++)
	{
		if (newValue->nodes.data[i].max_connections > max_connections)
			return false;
	}

	return true;
}