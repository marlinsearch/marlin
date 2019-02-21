#ifndef _SHARD_QUERY_H_
#define _SHARD_QUERY_H_

#include "query.h"
#include "kvec.h"
#include "workers.h"

struct squery_result {
    kvec_t(termresult_t *) termresults;
};

struct squery {
    struct worker *worker;
    struct query *q;
    struct squery_result *sqres;
    int shard_idx;
};

void execute_squery(void *w);

#endif

