#ifndef __INDEX_H_
#define __INDEX_H_
#include <jansson.h>

#include "common.h"
#include "app.h"
#include "threadpool.h"
#include "shard.h"
#include "flakeid.h"
#include "kvec.h"
#include "mapping.h"
#include "query.h"
#include "keys.h"

#define MAX_INDEX_NAME      128
#define DEFAULT_NUM_SHARDS  1
#define JOB_QUEUE_LEN       4096

// Index default query settings
#define DEF_HITS_PER_PAGE   20
#define DEF_MAX_HITS        500
#define MAX_HITS_LIMIT      1000
#define DEF_FACET_RESULTS   10
#define DEF_FULLSCAN_THRES  25000

typedef enum jobtype {
    JOB_ADD,
    JOB_DELETE,
    JOB_UPDATE,
    JOB_REPLACE,
    JOB_CLEARDS,
    JOB_REINDEX,
    JOB_BULK,
} JOB_TYPE;


/* Configuration information for an Index */
struct index_cfg {
    bool configured;
    kvec_t(char *) index_fields; 
    kvec_t(char *) facet_fields; 
    struct query_cfg *qcfg; // Default query configuration
};

/**
 * This is an index, which is analogous to a table
 * in a database.  An index belongs to an app as 
 * defined in app.h.  Each index contains one or more
 * shards as defined in shard.h
 * */
struct index {
    char name[MAX_INDEX_NAME];
    struct app *app;
    struct index_cfg cfg;
    int num_shards;
    struct mapping *mapping;

    // The shards this index contains
    kvec_t(struct shard *) shards;

    // Tracking info
    uint64_t time_created;
    uint64_t time_updated;
    flakeid_ctx_t *fctx; 

    // Threadpool for index modification
    threadpool_t *wpool;
    pthread_rwlock_t wpool_lock;
    uint16_t job_count;
};

/* An index job, which can be of one of the above jobtypes
 * based on the jobtype some of the fields may not be used */
struct in_job {
    struct index *index;
    json_t *j;
    json_t *j2;
    JOB_TYPE type;
    uint32_t id;
};



struct index *index_new(const char *name, struct app *a, int num_shards);
void index_free(struct index *in);
void index_clear(struct index *in);
void index_delete(struct index *in);
struct schema *get_field_schema(struct index *in, const char *key);
void index_apply_key(struct index *in, struct key *k, KEY_ACCESS access);
void index_delete_key(struct index *in, struct key *k);

#endif
