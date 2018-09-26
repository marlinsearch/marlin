#ifndef __INDEX_H_
#define __INDEX_H_

#include "common.h"
#include "app.h"


#define MAX_INDEX_NAME  128

/* Configuration information for an Index */
struct index_cfg {
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
};

struct index *index_new(const char *name, struct app *a);

#endif
