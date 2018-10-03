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
#define MASTER_APPID    "master_app_id"
#define MASTER_APIKEY   "master_api_key"
#define DB_LOCATION     "db_location"
#define SSL_CERT        "certificate"
#define SSL_KEY         "private_key"
#define PORT            "port"
#define HTTPS           "https"
#define NUMTHREADS      "num_threads"

// Json responses
#define J_SUCCESS   "{\"success\":true}"
#define J_FAILURE   "{\"success\":false}"

/* API JSON field names */
#define J_NAME          "name"
#define J_APIKEY        "api_key"
#define J_APPID         "app_id"
#define J_CREATED       "created"
#define J_UPDATED       "updated"
#define J_NUM_SHARDS    "num_shards"
#define J_NUM_JOBS      "num_jobs"
#define J_ID            "_id"
#define J_SID           "_sid"
#define J_TYPE          "type"
#define J_PROPERTIES    "properties"
#define J_FIELDID       "field_id"
#define J_IS_INDEXED    "is_indexed"
#define J_IS_FACET      "is_facet"
// API Settings fields
#define J_S_INDEXFIELDS "indexed_fields"
#define J_S_FACETFIELDS "facet_fields"


// config / settings field names
#define J_FULL_SCHEMA   "full_schema"
#define J_INDEX_SCHEMA  "index_schema"
#define J_INDEX_READY   "ready_to_index"
#define J_STRINGS       "strings"
#define J_NUMBERS       "numbers"
#define J_BOOLS         "booleans"
#define J_FACETS        "facets"
#define J_NUM_FIELDS    "num_fields"
#define J_INDEX_PRIORITY "index_priority"
#define J_FACET_PRIORITY "facet_priority"

#endif

