#ifndef _SHARD_QUERY_H_
#define _SHARD_QUERY_H_

#include "query.h"
#include "kvec.h"
#include "workers.h"
#include "mbmap.h"
#include "hashtable.h"

typedef struct termdata {
    termresult_t *tresult;
    struct bmap *tbmap;
} termdata_t;


struct facet_hash {
    struct hashtable *h;
};

typedef struct facet_count {
    uint8_t shard_id;
    uint32_t facet_id;
    uint32_t count;
} facet_count_t;


// When querying a remote shard, ranks will only contain
// qualifying documents.  In case of a local shard we 
// just pass the pointer as is
// TODO: Rewrite this when clustering support is added
struct squery_result {
    termdata_t *termdata;
    struct bmap *docid_map;
    struct docrank *ranks;
    struct bmap **exact_docid_map;      // Documents which are exact match of size q->num_words
    struct facet_hash *fh;              // Facet hashtables for enabled facets
    struct facet_count **fc;            // Facet count result
    khash_t(WID2TYPOS) *all_wordids;    // All matching word ids to typos for all terms
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

