/**
 * Shard query deals with querying a given sindex (shard index) and sdata (shard data)
 * to form results for a query for this shard
 */
#include "squery.h"
#include "workers.h"

static struct squery_result *squery_result_new(void) {
    struct squery_result *sqres = calloc(1, sizeof(struct squery_result));
    kv_init(sqres->termresults);
    return sqres;
}

static void lookup_terms(struct squery *sq, struct sindex *si) {
    int num_terms = kv_size(sq->q->terms);
    struct squery_result *sqres = sq->sqres;

    for (int i = 0; i < num_terms; i++) {
        struct termresult *tr = dtrie_lookup_term(si->trie, kv_A(sq->q->terms, i));
        kv_push(termresult_t *, sqres->termresults, tr);
    }
}

void execute_squery(void *w) {
    struct squery *sq = w;
    M_INFO("Performing squery for shard %d", sq->shard_idx);
    // First allocate a sq_result
    sq->sqres = squery_result_new();
    // Let the shard index handle the query now
    struct shard *s = kv_A(sq->q->in->shards, sq->shard_idx);
    struct sindex *si = s->sindex;

    // First lookup all terms
    lookup_terms(sq, si);

    worker_done(sq->worker);
}

