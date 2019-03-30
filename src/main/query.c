#include "query.h"
#include "squery.h"
#include "marlin.h"
#include "utils.h"
#include "filter.h"
#include "debug.h"
#include "hashtable.h"

#define facetgt(a, b) ((a).count > (b).count)
KSORT_INIT(facetsort, facet_count_t, facetgt)

static void term_free(term_t *t) {
    wordfree(t->word);
    free(t);
}

static void explain_rank(struct query *q, json_t *j, struct docrank *r) {
    json_t *e = json_object();
    int i = 0;
    while (q->rank_rule[i] != R_DONE) {
        if (q->rank_rule[i] == R_TYPO) {
            json_object_set_new(e, rule_to_rulestr(q->rank_rule[i]), json_integer(r->typos));
        } else if (q->rank_rule[i] == R_PROX) {
            json_object_set_new(e, rule_to_rulestr(q->rank_rule[i]), json_integer(r->proximity));
        } else if (q->rank_rule[i] == R_POS) {
            json_object_set_new(e, rule_to_rulestr(q->rank_rule[i]), json_integer(r->position));
        } else if (q->rank_rule[i] == R_EXACT) {
            json_object_set_new(e, rule_to_rulestr(q->rank_rule[i]), json_integer(r->exact));
        } else if (q->rank_rule[i] == R_FIELD) {
            json_object_set_new(e, rule_to_rulestr(q->rank_rule[i]), json_integer(r->field));
        }
        i++;
    }
    json_object_set_new(j, "_explain", e);
}

static struct facet_count *process_facet_results(struct squery *sq, int f, int *count) {
    struct query *q = sq[0].q;
    struct facet_count *fc;
    int rcount = 0;
    // This is a simpler case, when we have a single shard, Just
    // sort the result count and return
    if (q->in->num_shards == 1) {
        int rcount = sq[0].sqres->fh[f].rcount;
        // Sort the results
        ks_introsort(facetsort, rcount, sq[0].sqres->fc[f]);
        fc = sq[0].sqres->fc[f];
        // Limit result count to max facet results
        if (rcount > q->cfg.max_facet_results) {
            rcount = q->cfg.max_facet_results;
        }
        *count = rcount;
        return fc;
    }

    // Merge results use a hashtable to store position in a 
    // preallocated fc array.
    // Allocate an array big enough to handle the worst case scenario
    fc = malloc(sizeof(struct facet_count) * q->in->num_shards * q->cfg.max_facet_results * 2);
    struct hashtable *h = hashtable_new(512);
    for (int i = 0; i < q->in->num_shards; i++) {
        struct squery_result *sqres = sq[i].sqres;
        int size = sqres->fh[f].rcount;
        for (int j = 0; j < size; j++) {
            struct facet_count *ifc = &sqres->fc[f][j];
            struct cell *c;
            // If we already have this facet stored in the array, update its value
            if ((c = hashtable_lookup(h, ifc->facet_id)) != NULL) {
                fc[c->value].count += ifc->count;
            } else {
                // Add the facet data
                c = hashtable_insert(h, ifc->facet_id);
                memcpy(&fc[rcount], ifc, sizeof(struct facet_count));
                c->value = rcount;
                rcount++;
            }
        }
    }

    // Now sort the results
    ks_introsort(facetsort, rcount, fc);

    // Limit result count to max facet results
    if (rcount > q->cfg.max_facet_results) {
        rcount = q->cfg.max_facet_results;
    }
    *count = rcount;
    hashtable_free(h);
    return fc;
}

static json_t *form_facet_result(struct query *q, struct squery *sq, F_TYPE type, int f) {
    json_t *ja = json_array();
    int count;

    struct facet_count *fc = process_facet_results(sq, f, &count);
    for (int i = 0; i < count; i++) {
        // Retrieve the actual facet string from the shard
        struct shard *s = kv_A(q->in->shards, fc[i].shard_id);
        char *fstr = sindex_lookup_facet(s->sindex, fc[i].facet_id);
        // This should never happen
        if (!fstr) {
            M_ERR("Failed to lookup facet for facet_id %u on shard %d\n", fc->facet_id, fc->shard_id);
            continue; 
        }
        // Based on type set the correct key with the correct type
        json_t *jf = json_object();
        json_object_set_new(jf, J_R_COUNT, json_integer(fc[i].count));
        switch (type) {
            case F_STRING:
            case F_STRLIST:
                json_object_set_new(jf, J_R_KEY, json_string(fstr));
                break;
            case F_NUMBER:
            case F_NUMLIST:
                json_object_set_new(jf, J_R_KEY, json_real(atof(fstr)));
                break;
            default:
                break;
        }
        free(fstr);
        json_array_append_new(ja, jf);
    }

    // If number of shards was more, we used a temp list to merge the results,
    // free it
    if (q->in->num_shards > 1) {
        free(fc);
    }

    return ja;
}

static json_t *get_facet_results(struct query *q, struct squery *sq) {
    json_t *jf = json_object();
    for (int i = 0; i < q->in->mapping->num_facets; i++) {
        if (q->cfg.facet_enabled[i]) {
            const struct facet_info *fi = get_facet_info(q->in->mapping, i);
            json_t *ja = form_facet_result(q, sq, fi->type, i);
            json_object_set_new(jf, fi->name, ja);
        }
    }
    return jf;
}

static json_t *form_result(struct query *q, struct squery *sq) {
    json_t *j = json_object();
    // Process response within shardquery
    int total_hits = 0;
    int total_ranks = 0;
    for (int i = 0; i < q->in->num_shards; i++) {
        M_DBG("Num hits from shard %d is %lu", i, sq[i].sqres->num_hits);
        total_hits += sq[i].sqres->num_hits;
        total_ranks += sq[i].sqres->rank_count;
    }

    // Make sure we are within the limits
    int page_s = q->cfg.hits_per_page * (q->page_num -1);
    if (page_s > total_hits) {
        page_s = total_hits;
    }
    int page_e = page_s + q->cfg.hits_per_page;
    if (page_e > total_hits) {
        page_e = total_hits;
    }
    int num_hits = page_e - page_s;
    int max_hits = q->cfg.max_hits;
    if (max_hits > total_hits) {
        max_hits = total_hits;
    }

    json_t *jhits = json_array();
    // sort the final response
    struct docrank *ranks = NULL;
    if (q->in->num_shards == 1) {
        // If we have a single shard, take the result as is
        ranks = sq[0].sqres->ranks;
        if (ranks) {
            rank_sort(sq[0].sqres->rank_count, ranks, q->rank_rule);
            for (int j = 0; j < sq[0].sqres->rank_count; j++) {
                sq[0].sqres->ranks[j].shard_id = 0;
            }
        }
    } else {
        // If we have more than one shard, allocate data to combine all shard results
        ranks = malloc(sizeof(struct docrank) * total_ranks);
        int pos = 0;
        for (int i = 0; i < q->in->num_shards; i++) {
            // Set shardid, we need that lookup documents
            for (int j = 0; j < sq[i].sqres->rank_count; j++) {
                sq[i].sqres->ranks[j].shard_id = i;
            }
            // Now copy shard ranks to full ranks
            memcpy(&ranks[pos], sq[i].sqres->ranks, sq[i].sqres->rank_count * sizeof(struct docrank));
            pos += sq[i].sqres->rank_count;
        }
        M_DBG("Sorting %d hits\n", total_ranks);
        rank_sort(total_ranks, ranks, q->rank_rule);
    }

    for (int i=page_s; i<page_e; i++) {
        struct shard *s = kv_A(q->in->shards, ranks[i].shard_id);
        char *o = sdata_get_document_byid(s->sdata, ranks[i].docid);
        if (o) {
            json_error_t error;
            json_t *hit = json_loads(o, 0, &error);
            if (hit) {
                if (q->explain) {
                    explain_rank(q, hit, &ranks[i]);
                }
                json_array_append_new(jhits, hit);
            }
            free(o);
        }
    }

    json_t *jf = get_facet_results(q, sq);
    if (jf) {
        json_object_set_new(j, J_R_FACETS, jf);
    }

    if (q->in->num_shards > 1) {
        free(ranks);
    }
     
    // Fill the response
    json_object_set_new(j, J_R_TOTALHITS, json_integer(total_hits));
    json_object_set_new(j, J_R_NUMHITS, json_integer(num_hits));
    json_object_set_new(j, J_R_PAGE, json_integer(q->page_num));
    json_object_set_new(j, J_R_NUMPAGES, json_integer(LIKELY(q->cfg.hits_per_page)?((float)max_hits/q->cfg.hits_per_page) + 0.9999:0));
    json_object_set_new(j, J_R_HITS, jhits);
    json_object_set_new(j, J_R_QUERYTEXT, json_string(q->text));
    return j;
}

/* Executes a parsed query.  This inturn sends the query to multiple shards
 * and further processes the results before sending the final response*/
char *execute_query(struct query *q) {

    struct timeval start, stop;
    struct index *in = q->in;
    struct squery *sq = malloc(sizeof(struct squery) * in->num_shards);

    // Start time
    gettimeofday(&start, NULL);

    if (in->num_shards > 1) {
        // Initialize a worker
        struct worker worker;
        worker_init(&worker, in->num_shards);

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
    } else {
        // For single shard indices, directly invoke execute_squery instead of
        // using the threadpool
        sq[0].q = q;
        sq[0].shard_idx = 0;
        sq[0].worker = NULL;
        sq[0].shard = kv_A(in->shards, 0);
        sq[0].sqres = NULL; // This gets allocated when query is executed.
        execute_squery(&sq[0]);
    }
    trace_query("Got results in ", &start);

    json_t *j = form_result(q, sq);

    trace_query("Formed result in ", &start);
    for (int i = 0; i < in->num_shards; i++) {
        sqresult_free(q, sq[i].sqres);
    }
    free(sq);

    // Finally set the time taken
    gettimeofday(&stop, NULL);
    float took = timedifference_msec(start, stop);
    json_object_set_new(j, J_R_TOOK, json_integer(took + 0.99));

    char *response = json_dumps(j, JSON_PRESERVE_ORDER|JSON_COMPACT);
    json_decref(j);
#ifdef TRACE_QUERY
    printf("\n");
#endif
    return response;
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
        if (q->text[strlen(q->text) - 1] == ' ') {
            t->prefix = 0;
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
                if (q->text[strlen(q->text) - 1] == ' ') {
                    t->prefix = 0;
                } else {
                    t->prefix = 1;
                }
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
            if (q->text[strlen(q->text) - 1] == ' ') {
                t->prefix = 0;
            } else {
                t->prefix = 1;
            }
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

    if (q->filter) {
        filter_free(q->filter);
    }

    free(q);
}


