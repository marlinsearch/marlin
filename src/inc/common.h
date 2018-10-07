#ifndef COMMON_H
#define COMMON_H
#include <inttypes.h>
#include <sys/types.h>
#include <stdbool.h>
#include <stdlib.h>

#include "platform.h"
#include "mlog.h"

#define VALGRIND_TEST 1

#ifdef VALGRIND_TEST
#define MDB_ENV_SIZE        655360000
#else
#define MDB_ENV_SIZE        65536000000
#endif

// File names
#define SETTINGS_PATH   "./settings.json"
#define APPS_FILE       "apps.json"
#define INDEXES_FILE    "indexes.json"
#define INDEX_FILE      "index.json"
#define MAPPING_FILE    "mapping.json"
#define SHARD_FILE      "shard.json"
#define SETTINGS_FILE   "settings.json"

// Settings
#define APPID_SIZE      8
#define APIKEY_SIZE     32
#define MASTER_APPID    "masterAppId"
#define MASTER_APIKEY   "masterApiKey"
#define DB_LOCATION     "dbLocation"
#define SSL_CERT        "certificate"
#define SSL_KEY         "privateKey"
#define PORT            "port"
#define HTTPS           "https"
#define NUMTHREADS      "numThreads"

// Json responses
#define J_SUCCESS   "{\"success\":true}"
#define J_FAILURE   "{\"success\":false}"

/* API JSON field names */
#define J_NAME          "name"
#define J_APIKEY        "apiKey"
#define J_APPID         "appId"
#define J_CREATED       "created"
#define J_UPDATED       "updated"
#define J_NUM_SHARDS    "numShards"
#define J_NUM_JOBS      "numJobs"
#define J_ID            "_id"
#define J_SID           "_sid"
#define J_TYPE          "type"
#define J_PROPERTIES    "properties"
#define J_FIELDID       "fieldId"
#define J_IS_INDEXED    "isIndexed"
#define J_IS_FACET      "isFacet"
// API Settings fields
#define J_S_INDEXFIELDS "indexedFields"
#define J_S_FACETFIELDS "facetFields"


// config / settings field names
#define J_FULL_SCHEMA   "fullSchema"
#define J_INDEX_SCHEMA  "indexSchema"
#define J_INDEX_READY   "readyToIndex"
#define J_STRINGS       "strings"
#define J_NUMBERS       "numbers"
#define J_BOOLS         "booleans"
#define J_FACETS        "facets"
#define J_NUM_FIELDS    "numFields"
#define J_INDEX_PRIORITY "indexPrority"
#define J_FACET_PRIORITY "facetPriority"

#endif

