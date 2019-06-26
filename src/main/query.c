#include "query.h"
#include "squery.h"
#include "marlin.h"
#include "utils.h"
#include "filter.h"
#include "debug.h"
#include "hashtable.h"
#include "highlight.h"
#include "aggs.h"

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

// Look a field in json and return a new copy if it is a valid field
static json_t *json_lookup_field(const struct json_t* j, const char *field) {
    json_t *jf = json_object_get(j, field);
    if (!jf) return NULL;
    return json_deep_copy(jf);
}

static void json_handle_limit_field(struct json_t *newj, const json_t *j, struct field *f) {
    while (f) {
        json_t *jo = json_lookup_field(j, f->name);
        if (jo) {
            // Special handling for objects and arrays, rest go as is
            if (json_is_object(jo)) {
                if (f->child) {
                    json_t *jco = json_object();
                    json_handle_limit_field(jco, jo, f->child);
                    json_object_set_new(newj, f->name, jco);
                    json_decref(jo);
                } else {
                    json_object_set_new(newj, f->name, jo);
                }
            } else if (json_is_array(jo)) {
                for (int i = 0; i < json_array_size(jo); i++) {
                    json_t *av = json_array_get(jo, i);
                    // Handle array inner elements
                    if (json_is_object(av) && f->child) {
                        json_t *an = json_object();
                        json_handle_limit_field(an, av, f->child);
                        json_array_set_new(jo, i, an);
                    }
                }
                json_object_set_new(newj, f->name, jo);
            } else {
                json_object_set_new(newj, f->name, jo);
            }
        }
        f = f->next;
    }
}

/* Iterates all elements / inner elements of an object and highlights all strings */
static struct json_t *highlight_json(struct json_t *j, struct query *q) {
    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        if (json_is_string(value)) {
            char *htxt = highlight(json_string_value(value), q, 0);
            if (htxt) {
                json_object_set_new(j, key, json_string(htxt));
                free(htxt);
            }
        } else if (json_is_object(value)) {
            highlight_json(value, q);
        } else if (json_is_array(value)) {
            json_t *ja = value;
            for (int i=0; i<json_array_size(ja); i++) {
                json_t *av = json_array_get(ja, i);
                if (json_is_string(av)) {
                    char *htxt = highlight(json_string_value(av), q, 0);
                    if (htxt) {
                        json_array_set_new(ja, i, json_string(htxt));
                        free(htxt);
                    }
                } else if (json_is_object(av)) {
                    highlight_json(av, q);
                }
            }
        }
    }
    return j;
}

static struct json_t *highlight_all_fields(struct json_t *hit, struct query *q) {
    if (q->cfg.highlight_source) {
        return highlight_json(hit, q);
    }
    // Not highlighting source, make a copy and highlight that
    json_t *hit_dup = json_deep_copy(hit);
    hit_dup = highlight_json(hit_dup, q);
    // Finally set that in source under _highlight
    json_object_set_new(hit, J_HIGHLIGHT, hit_dup);
    return hit;
}

static struct field * get_limited_field(struct field *f, const char *key) {
    while (f) {
        if (strcmp(f->name, key) == 0) return f;
        f = f->next;
    }
    return NULL;
}

static struct json_t *highlight_json_limit_field(struct json_t *j, struct query *q, struct field *f) {
    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        struct field *lf = get_limited_field(f, key);
        if (!lf) continue;

        if (json_is_string(value)) {
            char *htxt = highlight(json_string_value(value), q, 0);
            if (htxt) {
                json_object_set_new(j, key, json_string(htxt));
                free(htxt);
            }
        } else if (json_is_object(value)) {
            if (lf->child) {
                highlight_json_limit_field(value, q, lf->child);
            }
        } else if (json_is_array(value)) {
            json_t *ja = value;
            for (int i=0; i<json_array_size(ja); i++) {
                json_t *av = json_array_get(ja, i);
                if (json_is_string(av)) {
                    char *htxt = highlight(json_string_value(av), q, 0);
                    if (htxt) {
                        json_array_set_new(ja, i, json_string(htxt));
                        free(htxt);
                    }
                } else if (json_is_object(av)) {
                    if (lf->child) {
                        highlight_json_limit_field(av, q, lf->child);
                    }
                }
            }
        }
    }
    return j;
}

static struct json_t *highlight_some_fields(struct json_t *hit, struct query *q) {
    if (q->cfg.highlight_source) {
        return highlight_json_limit_field(hit, q, q->cfg.highlight_fields);
    }
    // Not highlighting source, make a copy and highlight that
    json_t *hit_dup = json_deep_copy(hit);
    hit_dup = highlight_json_limit_field(hit_dup, q, q->cfg.highlight_fields);
    // Finally set that in source under _highlight
    json_object_set_new(hit, J_HIGHLIGHT, hit_dup);
    return hit;
}

/* Takes a hit and applies query processing like getFields or highlightFields */
static struct json_t *hit_query_processing(struct json_t *hit, struct query *q) {
    if (!q->cfg.get_fields && !q->cfg.highlight_fields) {
        return hit;
    }
    struct json_t *new_hit = hit;
    if (q->cfg.get_fields) {
        new_hit = json_object();
        json_handle_limit_field(new_hit, hit, q->cfg.get_fields);
        json_decref(hit);
    }

    // We highlight all fields, this is '*' / default setting
    if (q->cfg.highlight_fields->name[0] == '\0') {
        new_hit = highlight_all_fields(new_hit, q);
    } else {
        new_hit = highlight_some_fields(new_hit, q);
    }

    return new_hit;
}

static json_t *form_result(struct query *q, struct squery *sq) {
    json_t *j = json_object();
    // Process response within shardquery
    int total_hits = 0;
    int total_ranks = 0;
    bool fullScan = true; 

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
        if (sq[0].fast_rank) {
            fullScan = false;
        }
        if (sq[0].sqres->agg) {
            json_object_set_new(j, J_AGGS, sq[0].sqres->agg->as_json(sq[0].sqres->agg));
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
            if (sq[i].fast_rank) {
                fullScan = false;
            }
            if (q->agg) {
                q->agg->merge(q->agg, sq[i].sqres->agg);
            }
        }
        M_DBG("Sorting %d hits\n", total_ranks);
        rank_sort(total_ranks, ranks, q->rank_rule);

        if (q->agg) {
            json_object_set_new(j, J_AGGS, q->agg->as_json(q->agg));
        }
    }

    for (int i=page_s; i<page_e; i++) {
        struct shard *s = kv_A(q->in->shards, ranks[i].shard_id);
        char *o = sdata_get_document_byid(s->sdata, ranks[i].docid);
        if (o) {
            json_error_t error;
            json_t *hit = json_loads(o, 0, &error);
            // process hit to highlight and or filter fields
            hit = hit_query_processing(hit, q);
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
    json_object_set_new(j, J_S_FULLSCAN, json_boolean(fullScan));
    return j;
}

/* Executes a parsed query.  This inturn sends the query to multiple shards
 * and further processes the results before sending the final response*/
char *execute_query(struct query *q) {

    struct timeval start, stop;
    struct index *in = q->in;
    struct squery *sq = calloc(in->num_shards, sizeof(struct squery));

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

    if (q->agg) {
        q->agg->free(q->agg);
    }

    free(q);
}


