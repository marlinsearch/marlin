#ifndef _SHARD_QUERY_H_
#define _SHARD_QUERY_H_

#include "query.h"
#include "workers.h"

struct squery {
    struct worker *worker;
    struct query *q;
    int shard_idx;
};

void execute_squery(void *w);

#endif

