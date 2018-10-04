#ifndef __INDEX_H_
#define __INDEX_H_
#include <jansson.h>

#include "common.h"
#include "app.h"
#include "threadpool.h"

#define MAX_INDEX_NAME      128
#define MAX_NUM_SHARDS      16
#define DEFAULT_NUM_SHARDS  1
#define JOB_QUEUE_LEN       4096

typedef enum jobtype {
    JOB_ADD,
    JOB_DELETE,
    JOB_UPDATE,
    JOB_REPLACE,
    JOB_CLEARDS,
    JOB_REINDEX,
    JOB_BULK,
} JOB_TYPE;

/* An index job, which can be of one of the above jobtypes
 * based on the jobtype some of the fields may not be used */
struct in_job {
    struct index *index;
    json_t *j;
    json_t *j2;
    JOB_TYPE type;
    uint32_t id;
};


/* Configuration information for an Index */
struct index_cfg {
    int num_shards;
};

/**
 * This is an index, which is analogous to a table
 * in a database.  An index belongs to an app as 
 * defined in app.h.  Each index contains one or more
 * shards as defined in shard.h*/
struct index {
    char name[MAX_INDEX_NAME];
    struct app *app;
    struct index_cfg *cfg;

    uint64_t time_created;
    uint64_t time_updated;

    // Threadpool for index modification
    threadpool_t *wpool;
    pthread_rwlock_t wpool_lock;
    uint16_t job_count;

};

struct index *index_new(const char *name, struct app *a);

#endif
