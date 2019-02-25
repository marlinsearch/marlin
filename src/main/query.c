#include "query.h"
#include "squery.h"
#include "marlin.h"

static void term_free(term_t *t) {
    wordfree(t->word);
    free(t);
}

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
        sq[i].shard = kv_A(in->shards, i);
        sq[i].sqres = NULL; // This gets allocated when query is executed.
        threadpool_add(search_pool, execute_squery, &sq[i], 0);
        // TODO: Avoid touching sqres while processing results, this will
        // in the futue be executed on a remote shard
    }

    wait_for_workers(&worker);
    worker_destroy(&worker);

    // TODO: Process response within shardquery
    for (int i = 0; i < in->num_shards; i++) {
        sqresult_free(sq[i].sqres);
    }
    free(sq);
    return "";
}

/* Generates query terms for a given query.  THis is specific to the ranking model 
 * used, so this will get moved in the near future
 * TODO: move it to the appropriate ranking model */
void generate_query_terms(struct query *q) {
    // Do not bother looking at query with no words
    if (q->num_words == 0) return;

    // Single word maps to a single term
    if (q->num_words == 1) {
        term_t *t = calloc(1, sizeof(term_t));
        t->word = worddup(kv_A(q->words, 0));
        // If the query needs a prefix search, do that
        if (q->cfg.prefix != PREFIX_NONE) {
            t->prefix = 1;
        }
        // If typos are allowed by the query and if we are above the min typo check
        // length, enable search with typos
        if ((q->cfg.typos == TYPO_OK) && (t->word->length > LEVLIMIT)) {
            t->typos = 1;
        }
        kv_push(term_t *, q->terms, t);
        return;
    }

    // 2 words, generates 3 terms [w1, w2, w1+w2 (no typos)]
    if (q->num_words == 2) {
        // Add the first 2 words as search terms
        for (int i = 0; i < 2; i ++) {
            term_t *t = calloc(1, sizeof(term_t));
            t->word = worddup(kv_A(q->words, i));
            // If the query needs a prefix search, do that
            if (q->cfg.prefix == PREFIX_ALL) {
                t->prefix = 1;
            } else if (q->cfg.prefix == PREFIX_LAST && i == 1) {
                t->prefix = 1;
            }
            // If typos are allowed by the query and if we are above the min typo check
            // length, enable search with typos
            if ((q->cfg.typos == TYPO_OK) && (t->word->length > LEVLIMIT)) {
                t->typos = 1;
            }
            kv_push(term_t *, q->terms, t);
        }

        // The third term is w1 + w2 but no typos are allowed, prefix search may be ok
        term_t *t = calloc(1, sizeof(term_t));
        t->word = wordadd(kv_A(q->words, 0), kv_A(q->words, 1));
        t->typos = 0;
        if (q->cfg.prefix != PREFIX_NONE) {
            t->prefix = 1;
        }
        kv_push(term_t *, q->terms, t);
        return;
    }

    /* Anything more than 2 words is handled differently. 
     * Consider the words [w1, w2, w3].  The following terms are 
     * generated.
     * First the words themselves
     * [w1, w2, w3]
     * Then the adjacent words with no typos allowed
     * [w1+w2, w2+w3]
     * Finally all words combined
     * [w1+w2+w3]
     * */
    // This is the combined term
    term_t *ct = calloc(1, sizeof(term_t));
    ct->word = wordnew();
    ct->typos = 0;
    if (q->cfg.prefix != PREFIX_NONE) {
        ct->prefix = 1;
    }

    for (int i = 0; i < q->num_words; i++) {
        // Every word ends up in the combined term, do that
        wordcat(ct->word, kv_A(q->words, i));

        // Now create a term for the current word
        term_t *t = calloc(1, sizeof(term_t));
        t->word = worddup(kv_A(q->words, i));
        // If typos are ok, set it
        if ((q->cfg.typos == TYPO_OK) && (t->word->length > LEVLIMIT)) {
            t->typos = 1;
        }
        // If the query needs a prefix search, do that
        if (q->cfg.prefix == PREFIX_ALL) {
            t->prefix = 1;
        } else if (q->cfg.prefix == PREFIX_LAST && i == (q->num_words - 1)) {
            t->prefix = 1;
        }
        kv_push(term_t *, q->terms, t);

        // Now create a term for current word + next word, which is not 
        // required for the last word
        if (i != (q->num_words - 1)) {
            term_t *tt = calloc(1, sizeof(term_t));
            tt->word = wordadd(kv_A(q->words, i), kv_A(q->words, i+1));
            // typos not allowed and prefix only for last pair
            if ((q->cfg.prefix != PREFIX_NONE) && (i + 2 == q->num_words)) {
                tt->prefix = 1;
            }
            kv_push(term_t *, q->terms, tt);
        }
    }
    // Finally add the all words combined term
    kv_push(term_t *, q->terms, ct);
}

struct query *query_new(struct index *in) {
    struct query *q = calloc(1, sizeof(struct query));
    q->in = in;
    kv_init(q->words);
    kv_init(q->terms);
    return q;
}

void query_free(struct query *q) {
    free(q->text);
    // free words
    for (int i = 0; i < q->num_words; i++) {
        wordfree(kv_A(q->words, i));
    }
    kv_destroy(q->words);

    // Free terms and words in it
    for (int i = 0; i < kv_size(q->terms); i++) {
        term_free(kv_A(q->terms, i));
    }
    kv_destroy(q->terms);

    free(q);
}


