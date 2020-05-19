#ifndef COMMON_H
#define COMMON_H
#include <inttypes.h>
#include <sys/types.h>
#include <stdbool.h>
#include <stdlib.h>

#include "platform.h"
#include "mlog.h"
#include "khash.h"

//#define VALGRIND_TEST 1

#if VALGRIND_TEST
#define MDB_ENV_SIZE        655360000
#define MAPSIZE             655360000
#else
#define MDB_ENV_SIZE        65536000000
#define MAPSIZE             PSIZE * PSIZE * 100
#endif

#define IDPRIORITY(id, priority) ((uint64_t) id << 32 | priority << 16)
#define IDPHRASE(id, id2) ((uint64_t) id << 32 | id2 << 12)
#define IDNUM(id, priority) ((uint64_t) id << 32 | priority << 24)
#define IDFACET(id, priority) ((uint64_t) id << 32 | priority << 16)

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

// Maximum supported attributes?  // TODO: Too less ?
#define MAX_FIELDS      0xFF

// File names
#define SETTINGS_PATH   "./settings.json"
#define APPS_FILE       "apps.json"
#define INDEXES_FILE    "indexes.json"
#define KEYS_FILE       "keys.json"
#define INDEX_FILE      "index.json"
#define MAPPING_FILE    "mapping.json"
#define SHARD_FILE      "shard.json"
#define SETTINGS_FILE   "settings.json"
#define MDB_DATA_FILE   "data.mdb"
#define MDB_LOCK_FILE   "lock.mdb"
#define DTRIE_FILE      "dtrie.db"

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
#define J_NUM_INDEXES   "numIndexes"
#define J_NUM_SHARDS    "numShards"
#define J_NUM_JOBS      "numJobs"
#define J_NUM_DOCS      "numDocuments"
#define J_NUM_WORDS     "numWords"
#define J_NUM_TWORDS    "numTopLevelWords"
#define J_SHARDS        "shards"
#define J_ID            "_id"
#define J_DOCID         "_docid"
#define J_HIGHLIGHT     "_highlight"
#define J_TYPE          "type"
#define J_PROPERTIES    "properties"
#define J_FIELDID       "fieldId"
#define J_IS_INDEXED    "isIndexed"
#define J_IS_FACET      "isFacet"
#define J_PERMISSIONS   "permissions"
#define J_INDEXES       "indexes"
#define J_DESCRIPTION   "description"
#define J_MIN_DD        "minDocDataLength"
#define J_SUM_DD        "sumDocDataLength"
#define J_MAX_DD        "maxDocDataLength"
#define J_AVG_DD        "avgDocDataLength"

// API Settings fields
#define J_S_INDEXFIELDS     "indexedFields"
#define J_S_FACETFIELDS     "facetFields"
#define J_S_HITS_PER_PAGE   "hitsPerPage"
#define J_S_MAX_HITS        "maxHits"
#define J_S_MAX_FACET_RESULTS "maxFacetResults"
#define J_S_RANK_BY         "rankBy"
#define J_S_SORT_BY         "sortBy"
#define J_S_RANKALGO        "rankAlgorithm"
#define J_S_FULLSCAN        "fullScan"
#define J_S_FULLSCAN_THRES  "fullScanThreshold"
#define J_S_GET_FIELDS      "getFields"
#define J_S_HIGHLIGHT_FIELDS    "highlightFields"
#define J_S_HIGHLIGHT_SOURCE    "highlightSource"

// config / settings field names
#define J_FULL_SCHEMA   "fullSchema"
#define J_INDEX_SCHEMA  "indexSchema"
#define J_INDEX_READY   "readyToIndex"
#define J_STRINGS       "strings"
#define J_NUMBERS       "numbers"
#define J_BOOLS         "booleans"
#define J_FACETS        "facets"
#define J_NUM_FIELDS     "numFields"
#define J_INDEX_PRIORITY "indexPrority"
#define J_FACET_PRIORITY "facetPriority"

// Query attributes
#define J_QUERY         "q"
#define J_FILTER        "filter"
#define J_AGGS          "aggs"
#define J_PAGE          "page"
#define J_EXPLAIN       "explain"
#define J_REQUESTS      "requests"
#define J_INDEXNAME     "indexName"

// Query response attributes
#define J_R_TOTALHITS     "totalHits"
#define J_R_NUMHITS       "numHits"
#define J_R_HITS          "hits"
#define J_R_TOOK          "took"
#define J_R_PAGE          "page"
#define J_R_NUMPAGES      "numPages"
#define J_R_COUNT         "count"
#define J_R_KEY           "key"
#define J_R_FACETS        "facets"
#define J_R_QUERYTEXT     "queryText"
#define J_R_RESULTS       "results"
#define J_R_SUCCESS       "success"

#define ORDER_ASC           "asc"
#define ORDER_DESC          "desc"
#define MAX_FIELD_NAME 256

KHASH_MAP_INIT_INT64(IDNUM2DBL, double *) // Aggregation id num to double array

#endif

