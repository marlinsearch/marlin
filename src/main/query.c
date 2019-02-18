#include "query.h"
#include "squery.h"
#include "marlin.h"

/* Executes a parsed query.  This inturn sends the query to multiple shards
 * and further processes the results before sending the final response*/
char *execute_query(struct query *q) {
    struct index *in = q->in;
    // Initialize a worker
    struct worker worker;
    worker_init(&worker, in->num_shards);
    struct squery *sq = malloc(sizeof(struct squery) * in->num_shards);

    for (int i = 0; i < in->num_shards; i++) {
        sq[i].q = q;
        sq[i].worker = &worker;
        sq[i].shard_idx = i;
        threadpool_add(search_pool, execute_squery, &sq[i], 0);
    }

    wait_for_workers(&worker);
    worker_destroy(&worker);
    return "";
}

struct query *query_new(struct index *in) {
    struct query *q = malloc(sizeof(struct query));
    q->num_words = 0;
    q->in = in;
    kv_init(q->words);
    return q;
}

void query_free(struct query *q) {
    //TODO: free word inside words
    kv_destroy(q->words);
    free(q);
}

