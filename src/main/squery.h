#ifndef _SHARD_QUERY_H_
#define _SHARD_QUERY_H_

#include "query.h"
#include "kvec.h"
#include "workers.h"
#include "mbmap.h"

typedef struct termdata {
    termresult_t *tresult;
    struct bmap *tbmap;
} termdata_t;

struct squery_result {
    termdata_t *termdata;
    struct bmap *docid_map;
};

struct squery {
    struct worker *worker;
    struct query *q;
    struct squery_result *sqres;
    int shard_idx;
    MDB_txn *txn;
};

void execute_squery(void *w);
void sqresult_free(struct squery_result *sqres);

#endif

