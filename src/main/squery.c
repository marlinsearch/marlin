/**
 * Shard query deals with querying a given sindex (shard index) and sdata (shard data)
 * to form results for a query for this shard
 */
#include "squery.h"
#include "workers.h"
#include "debug.h"
#include "docrank.h"
#include "utils.h"
#include "sindex.h"
#include "filter.h"
#include "filter_apply.h"
#include "bmap.h"
#include "ksort.h"
#include "aggs.h"

#define facet_gt(a, b) ((a).count > (b).count)
KSORT_INIT(facet_sort, facet_count_t, facet_gt)

static struct squery_result *squery_result_new(struct query *q) {
    struct squery_result *sqres = calloc(1, sizeof(struct squery_result));
    sqres->exact_docid_map = calloc(q->num_words, sizeof(struct bmap *));
    sqres->all_wordids = kh_init(WID2TYPOS);
    return sqres;
}

static struct facet_hash *init_facet_hash(struct index *in, struct query_cfg *cfg) {
    struct facet_hash *fh = malloc(in->mapping->num_facets * sizeof(struct facet_hash));
    for (int i = 0; i < in->mapping->num_facets; i++) {
        if (cfg->facet_enabled[i]) {
            fh[i].h = hashtable_new(1024);
        } else {
            fh[i].h = NULL;
        }
    }
    return fh;
}

static struct bmap *get_twid_to_docids(struct squery *sq, struct sindex *si, uint32_t twid) {
    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    // read the query to find that out
    struct bmap *b = mbmap_load_bmap(sq->txn, si->twid2bmap_dbi, IDPRIORITY(twid, 0));
    // If we have any docids under this twid, return it
    // Return an empty bitmap otherwise
    return b ? b : bmap_new();
}

static inline void add_wid_to_wordids(uint32_t wid, void *data) {
    khash_t(WID2TYPOS) *wordids = data;
    khiter_t k;
    kh_set(WID2TYPOS, wordids, wid, 0);
}

// TODO: Make sure we set the least distance when we get multiple entries with different dist
// for same word id
static inline void add_wid_dist_to_wordids(uint32_t wid, int dist, void *data) {
    khash_t(WID2TYPOS) *wordids = data;
    khiter_t k;
    kh_set(WID2TYPOS, wordids, wid, dist);
}

static void set_wids_under_twid(struct squery *sq, struct sindex *si, termresult_t *tr) {
    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    struct bmap *b = mbmap_load_bmap(sq->txn, si->twid2widbmap_dbi, IDPRIORITY(tr->twid, 0));
    if (b) {
        bmap_iterate(b, add_wid_to_wordids, tr->wordids);
        bmap_iterate(b, add_wid_to_wordids, sq->sqres->all_wordids);
    }
    bmap_free(b);
}

static void process_termresult(struct squery *sq, struct sindex *si, struct termdata *td) {
    termresult_t *tr = td->tresult;
    // Check if we have a twid or word ids set, if not just bail out
    // we do not have any documents matching that term
    if ((tr->twid == 0) && (kh_size(tr->wordids) == 0)) {
        td->tbmap = bmap_new();
        td->tzero_typo_bmap = bmap_new();
        return;
    }

    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    // If we have a top level word id set, we can use the top level document matches
    // We will need to load the words under this top level id though
    if (tr->twid) {
        td->tbmap = get_twid_to_docids(sq, si, tr->twid);
        // Top level word ids have zero typos, so just set it
        td->tzero_typo_bmap = get_twid_to_docids(sq, si, tr->twid);
        // If we have docids under this twid, let us get all the words that are under it
        if (bmap_cardinality(td->tbmap)) {
            set_wids_under_twid(sq, si, tr);
        }
    } else {
        // TODO: we may have to delete some words if we do not have results
        // especially when we start handling matches in particular fields only
        struct oper *o = oper_new();
        struct oper *oz = oper_new();
        uint32_t wid;
        int dist;
        khash_t(WID2TYPOS) *all_wordids = sq->sqres->all_wordids;
        kh_foreach(tr->wordids, wid, dist, {
            struct bmap *b = mbmap_load_bmap(sq->txn, si->wid2bmap_dbi, IDPRIORITY(wid, 0));
            if (b) {
                add_wid_dist_to_wordids(wid, dist, all_wordids);
                oper_add(o, b);
                // Track documents with no typos
                if (dist == 0) {
                    oper_add(oz, b);
                }
            }
        });
        td->tbmap = oper_or(o);
        td->tzero_typo_bmap = oper_or(oz);
        // Free operation and all bitmaps under it
        oper_total_free(o);
        // Free just the operation, the zero typo bitmaps have already been freed
        oper_free(oz);
    }

#ifdef TRACK_WIDS
#if 1
    uint32_t wid;
    printf("Num matches %u\n", kh_size(tr->wordids));
    kh_foreach_key(tr->wordids, wid, {
        MDB_val key;
        MDB_val data;
        key.mv_size = sizeof(uint32_t);
        key.mv_data = (void *)&wid;
        // If it already exists, we need not write so set a NULL value to 
        // avoid looking up mdb everytime we encounter this facetid
        if (mdb_get(sq->txn, si->wid2chr_dbi, &key, &data) == 0) {
            printf("wid %u : ", wid);
            chr_t *chars = data.mv_data;
            for (int i = 0; i < data.mv_size/sizeof(chr_t); i++) {
                printf("%c", (char)chars[i]);
            }
            printf("\n");
        }
    });
#endif
#endif
}

static void lookup_terms(struct squery *sq, struct sindex *si) {
    int num_terms = kv_size(sq->q->terms);
    struct squery_result *sqres = sq->sqres;
    sqres->termdata = calloc(num_terms, sizeof(struct termdata));

    // First collect matching words with distance and the corresponding document ids
    for (int i = 0; i < num_terms; i++) {
        struct termdata *td = &sqres->termdata[i];
        td->tresult = dtrie_lookup_term(si->trie, kv_A(sq->q->terms, i));
        process_termresult(sq, si, td);
        dump_termresult(td->tresult);
        dump_bmap(td->tbmap);
    }

    // Get exact matches for words
    for (int i = 0; i < sq->q->num_words; i++) {
        uint32_t wid = dtrie_lookup_exact(si->trie, kv_A(sq->q->words,i));
        if (wid) {
            M_INFO("Exact wid is %u", wid);
            // TODO: Handle field restricted queries
            sqres->exact_docid_map[i] = mbmap_load_bmap(sq->txn, si->wid2bmap_dbi, IDPRIORITY(wid, 0));
        }
    }
}

// For a given term, OR matching documents with adjacent terms
static struct bmap *get_term_docids(struct squery_result *sqres, int term_pos, int num_terms) {
    struct oper *o = oper_new();
    /* (anew | new | newhope) */ 
    if (term_pos > 0) {
        oper_add(o, sqres->termdata[term_pos - 1].tbmap);
    }

    oper_add(o, sqres->termdata[term_pos].tbmap);

    if (term_pos < num_terms - 2) {
        oper_add(o, sqres->termdata[term_pos + 1].tbmap);
    }

    struct bmap *ret = oper_or(o);
    oper_free(o);

    return ret;
}

// Same as above with zero typo doc ids
static struct bmap *get_term_zero_typo_docids(struct squery_result *sqres, int term_pos, 
        int num_terms) {
    struct oper *o = oper_new();
    /* (anew | new | newhope) */ 
    if (term_pos > 0) {
        oper_add(o, sqres->termdata[term_pos - 1].tzero_typo_bmap);
    }

    oper_add(o, sqres->termdata[term_pos].tzero_typo_bmap);

    if (term_pos < num_terms - 2) {
        oper_add(o, sqres->termdata[term_pos + 1].tzero_typo_bmap);
    }

    struct bmap *ret = oper_or(o);
    oper_free(o);

    return ret;
}

/* Returns the total final matching document ids for the given terms.  Returns an empty
 * bmap if no matching results are found */
static struct bmap *get_matching_docids(struct squery *sq) {
    int num_terms = kv_size(sq->q->terms);
    struct squery_result *sqres = sq->sqres;

    // This happens when the query text is empty or not set
    if (num_terms == 0) {
        // Send all available docids
        return shard_get_all_docids(sq->shard);
    }

    // This happens when the query text is a single word
    if (num_terms == 1) {
        termdata_t *td = &sqres->termdata[0];
        return bmap_duplicate(td->tbmap);
    }

    struct bmap *ret = NULL;
    // This happens when query text is 2 words
    // 'a new' => (a & new) | anew 
    if (num_terms == 3) {
        ret = bmap_and(sqres->termdata[0].tbmap, sqres->termdata[1].tbmap);
        // Handle the last term
        goto last_term;
    }

    /* In case of multi word queries, a bunch of terms are generated.
     * Example for a search of 'a new hope', the terms are as follows
     * a) a
     * b) anew
     * c) new
     * d) newhope
     * e) hope
     * f) anewhope
     *
     * The final docids are 
     *
     * ((a | anew) & (anew | new | newhope) & (hope | anewhope)) | (anewhope)
     * a ab b bc c cd d abcd
     * 0 1  2  3 4 5  6  7 
     */
    struct oper *o = oper_new();
    for (int i = 0; i < num_terms; i += 2) {
        struct bmap *b = get_term_docids(sqres, i, num_terms);
        oper_add(o, b);
    }
    // We have been matching data until now
    ret = oper_and(o);
    oper_total_free(o);

last_term:
    // Finally or with the last combined terms search
    // Only process if we have any results for the combined terms search
    if (bmap_cardinality(sqres->termdata[num_terms-1].tbmap)) {
        // TODO: Implement bmap_inplace_or !
        struct oper *o = oper_new();
        oper_add(o, ret);
        oper_add(o, sqres->termdata[num_terms-1].tbmap);
        struct bmap *temp_ret = oper_or(o);
        oper_free(o);
        bmap_free(ret);
        ret = temp_ret;
    }
    return ret;
}

// Same as above for zero typo docids
static struct bmap *get_matching_zero_typo_docids(struct squery *sq) {
    int num_terms = kv_size(sq->q->terms);
    struct squery_result *sqres = sq->sqres;

    // This happens when the query text is empty or not set
    if (num_terms == 0) {
        // Send all available docids
        return shard_get_all_docids(sq->shard);
    }

    // This happens when the query text is a single word
    if (num_terms == 1) {
        termdata_t *td = &sqres->termdata[0];
        return bmap_duplicate(td->tzero_typo_bmap);
    }

    struct bmap *ret = NULL;
    // This happens when query text is 2 words
    // 'a new' => (a & new) | anew 
    if (num_terms == 3) {
        ret = bmap_and(sqres->termdata[0].tzero_typo_bmap, sqres->termdata[1].tzero_typo_bmap);
        // Handle the last term
        goto last_term;
    }

    struct oper *o = oper_new();
    for (int i = 0; i < num_terms; i += 2) {
        struct bmap *b = get_term_zero_typo_docids(sqres, i, num_terms);
        oper_add(o, b);
    }
    // We have been matching data until now
    ret = oper_and(o);
    oper_total_free(o);

last_term:
    // Finally or with the last combined terms search
    // Only process if we have any results for the combined terms search
    if (bmap_cardinality(sqres->termdata[num_terms-1].tzero_typo_bmap)) {
        // TODO: Implement bmap_inplace_or !
        struct oper *o = oper_new();
        oper_add(o, ret);
        oper_add(o, sqres->termdata[num_terms-1].tzero_typo_bmap);
        struct bmap *temp_ret = oper_or(o);
        oper_free(o);
        bmap_free(ret);
        ret = temp_ret;
    }
    return ret;
}

static void termdata_free(termdata_t *td) {
    termresult_free(td->tresult);
    bmap_free(td->tbmap);
    bmap_free(td->tzero_typo_bmap);
}

void sqresult_free(struct query *q, struct squery_result *sqres) {
    for (int i = 0; i < q->num_words; i++) {
        if (sqres->exact_docid_map[i]) {
            bmap_free(sqres->exact_docid_map[i]);
        }
    }
    kh_destroy(WID2TYPOS, sqres->all_wordids);
    free(sqres->exact_docid_map);
    free(sqres->ranks);
    for (int i = 0; i < q->in->mapping->num_facets; i++) {
        hashtable_free(sqres->fh[i].h);
        free(sqres->fc[i]);
    }
    // Free aggregations
    if (sqres->agg) {
        sqres->agg->free(sqres->agg);
    }
    free(sqres->fc);
    free(sqres->fh);
    free(sqres);
}

static inline int sort_results(struct query *q, struct docrank *ranks, uint32_t resultcount) {
    // How many entries do we want to partially sort?
    // If user requests for page 1 we only need 1 * hitsperpage
    int sortcnt = q->cfg.hits_per_page * q->page_num;
    // Hits per page can be 0 if you want to perform an aggregate query
    // or disjunctive facet lookup
    if (q->cfg.hits_per_page > 0) {
        // we only need a partial sort here, final sorting will be done
        // when all shard query results are merged
        if (resultcount >= PARTIAL_SORT_LIMIT  &&  resultcount > sortcnt) {
            rank_partial_sort(ranks, 0, resultcount-1, sortcnt, q->rank_rule);
        } else {
            rank_sort(resultcount, ranks, q->rank_rule);
        }
    }

    return (sortcnt < resultcount) ? sortcnt : resultcount;
}

static void squery_apply_filters(struct squery *sq) {
    struct sindex *si = sq->shard->sindex;
    struct filter *sf = filter_dup(sq->q->filter);
    struct bmap *fb = filter_apply(si, sf, sq->txn, sq->sqres->docid_map);
    if (fb) {
        /* If we have a result after applying filters, update docid_map
         * with documents matching the filter */
        struct bmap *fdocs = bmap_and(fb, sq->sqres->docid_map);
        bmap_free(sq->sqres->docid_map);
        sq->sqres->docid_map = fdocs;
    } else {
        /* If we have no matches, set an empty bitmap for this result */
        bmap_free(sq->sqres->docid_map);
        sq->sqres->docid_map = bmap_new();
    }
    // dump_filter(sf, 0);
    // Free the duplicated shard filter
    filter_free(sf);
}


static uint32_t get_facet_totalcount(struct squery *sq, uint32_t facet_id, 
        int priority, struct bmap *rbmap) {
    uint32_t result = 0;
    uint64_t fhid = IDPRIORITY(facet_id, priority);
    struct bmap *tbmap = mbmap_load_bmap(sq->txn, sq->shard->sindex->facetid2bmap_dbi, fhid);
    if (tbmap) {
        result = bmap_and_cardinality(tbmap, rbmap);
        bmap_free(tbmap);
    }
    return result;
}


static struct facet_count **sort_facets(struct squery *sq) {
    struct mapping *m = sq->q->in->mapping;
    struct facet_count **fc = calloc(m->num_facets, sizeof(struct facet_count *));

    // Iterate all facets and set final facet counts
    for (int i = 0; i < m->num_facets; i++) {
        if (!sq->q->cfg.facet_enabled[i]) continue;
        // Get the result hashtable holding all facets and counts
        struct hashtable *h = sq->sqres->fh[i].h;
        fc[i] = malloc(sizeof(struct facet_count) * h->m_population);
        int j = 0;
        // Lookup all hashtable cells and set facet-id and counts
        for (int x=0; x<h->m_arraySize; x++) {
            if (h->m_cells[x].key) {
                fc[i][j].facet_id = h->m_cells[x].key;
                fc[i][j].count = h->m_cells[x].value;
                j++;
            }
        }

        // Take twice the max facet results
        int rcount = sq->q->cfg.max_facet_results * 2;
        if (h->m_population > rcount) {
            ks_partialsort(facet_sort, fc[i], 0, h->m_population-1, rcount);
        } else {
            rcount = h->m_population;
        }

        // If we did a fast rank, find the accurate facet counts as we skipped many documents
        if (sq->fast_rank) {
            // Get actual facet counts
            for (int x = 0; x < rcount; x++) {
                fc[i][x].count = get_facet_totalcount(sq, fc[i][x].facet_id, i, 
                        sq->sqres->docid_map);
            }
        } 

        // Set shard_id for necessary results
        for (int x = 0; x < rcount; x++) {
            fc[i][x].shard_id = sq->shard_idx;
        }

        // Store the count of facets that actually matter
        sq->sqres->fh[i].rcount = rcount;
    }
    return fc;
}

void execute_squery(void *w) {
    struct timeval start;
    struct squery *sq = w;

    // Start time
    gettimeofday(&start, NULL);

    int num_terms = kv_size(sq->q->terms);
    M_DBG("Performing squery for shard %d", sq->shard_idx);
    // First allocate a sq_result
    sq->sqres = squery_result_new(sq->q);
    // Let the shard index handle the query now
    struct shard *s = sq->shard;
    struct sindex *si = s->sindex;
    sq->sqres->fh = init_facet_hash(sq->q->in, &sq->q->cfg);

    // Setup a mdb txn
    mdb_txn_begin(si->env, NULL, MDB_RDONLY, &sq->txn);

    // First lookup all terms
    lookup_terms(sq, si);

    // From the term data, find all documents which match our query
    sq->sqres->docid_map = get_matching_docids(sq);
    trace_query("Lookup documents in", &start);

    // From the term data, find all documents with zero typos matching our query
    sq->sqres->zero_typo_docid_map = get_matching_zero_typo_docids(sq);

    // Apply filters
    if (sq->q->filter) {
        squery_apply_filters(sq);
    }

    trace_query("Applied filters in", &start);
    sq->kh_idnum2dbl = kh_init(IDNUM2DBL);
    sq->kh_id2data = kh_init(ID2DATA);
    // Copy aggs if required
    if (sq->q->agg) {
        sq->sqres->agg = aggs_dup(sq->q->agg);
    }

    uint32_t resultcount = 0;
    sq->sqres->num_hits = bmap_cardinality(sq->sqres->docid_map);

    // Perform ranking
    resultcount = sq->sqres->num_hits;
    struct docrank *ranks = perform_ranking(sq, sq->sqres->docid_map, &resultcount);
    trace_query("Ranks calculated in", &start);
    //dump_bmap(sq->sqres->docid_map);

    // Aggregation should be done by now, clear the cache
    kh_destroy(IDNUM2DBL, sq->kh_idnum2dbl);
    kh_destroy(ID2DATA, sq->kh_id2data);

    // Sort facet results
    sq->sqres->fc = sort_facets(sq);
    trace_query("Facets sorted in", &start);
    
    sq->sqres->rank_count = sort_results(sq->q, ranks, resultcount);
    trace_query("Ranks sorted in", &start);
    sq->sqres->ranks = ranks;

    // Free docid maps
    bmap_free(sq->sqres->docid_map);
    bmap_free(sq->sqres->zero_typo_docid_map);

    // cleanup all termdata
    for (int i = 0; i < num_terms; i++) {
        termdata_free(&sq->sqres->termdata[i]);
    }
    free(sq->sqres->termdata);
    
    // Abort the read only transaction, we are done executing the query
    mdb_txn_abort(sq->txn);

    if (sq->worker) {
        worker_done(sq->worker);
    }
}

