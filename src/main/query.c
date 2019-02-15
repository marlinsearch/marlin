#include "query.h"

struct query *query_new(struct index *in) {
    struct query *q = malloc(sizeof(struct query));
    q->num_words = 0;
    kv_init(q->words);
    return q;
}

void query_free(struct query *q) {
    //TODO: free word inside words
    kv_destroy(q->words);
    free(q);
}

