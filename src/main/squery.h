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

// When querying a remote shard, ranks will only contain
// qualifying documents.  In case of a local shard we 
// just pass the pointer as is
// TODO: Rewrite this when clustering support is added
struct squery_result {
    termdata_t *termdata;
    struct bmap *docid_map;
    struct docrank *ranks;
    struct bmap **exact_docid_map; // Documents which are exact match of size q->num_words
    int rank_count;
    int num_hits;
};

struct squery {
    struct worker *worker;
    struct query *q;
    struct squery_result *sqres;
    struct shard *shard;
    int shard_idx;
    MDB_txn *txn;
};

void execute_squery(void *w);
void sqresult_free(struct query *q, struct squery_result *sqres);

#endif

