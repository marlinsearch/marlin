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


static struct squery_result *squery_result_new(struct query *q) {
    struct squery_result *sqres = calloc(1, sizeof(struct squery_result));
    sqres->exact_docid_map = calloc(q->num_words, sizeof(struct bmap *));
    sqres->all_wordids = kh_init(WID2TYPOS);
    return sqres;
}

static struct bmap *get_twid_to_docids(struct squery *sq, struct sindex *si, uint32_t twid) {
    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    // read the query to find that out
    struct bmap *b = mbmap_load_bmap(sq->txn, si->twid2bmap_dbi, IDPRIORITY(twid, 0));
    // If we have any docids under this twid, return it
    if (b && bmap_cardinality(b)) {
        return b;
    } else {
        bmap_free(b);
        return NULL;
    }
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
    if ((tr->twid == 0) && (kh_size(tr->wordids) == 0)) return;

    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    // If we have a top level word id set, we can use the top level document matches
    // We will need to load the words under this top level id though
    if (tr->twid) {
        td->tbmap = get_twid_to_docids(sq, si, tr->twid);
        // If we have docids under this twid, let us get all the words that are under it
        if (td->tbmap) {
            set_wids_under_twid(sq, si, tr);
        }
    } else {
        // TODO: we may have to delete some words if we do not have results
        // especially when we start handling matches in particular fields only
        struct oper *o = oper_new();
        uint32_t wid;
        int dist;
        khash_t(WID2TYPOS) *all_wordids = sq->sqres->all_wordids;
        kh_foreach(tr->wordids, wid, dist, {
            struct bmap *b = mbmap_load_bmap(sq->txn, si->wid2bmap_dbi, IDPRIORITY(wid, 0));
            if (b) {
                add_wid_dist_to_wordids(wid, dist, all_wordids);
                oper_add(o, b);
            }
        });
        td->tbmap = oper_or(o);
        oper_total_free(o);
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

    for (int i = 0; i < sq->q->num_words; i++) {
        uint32_t wid = dtrie_lookup_exact(si->trie, kv_A(sq->q->words,i));
        if (wid) {
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

    // oper_or by default returns an empty bitmap if all above bitmaps are null.
    // Return a NULL instead when no matches are found
    struct bmap *ret = o->count ? oper_or(o) : NULL;
    oper_free(o);
    return ret;
}

/* Returns the total final matching document ids for the given terms */
static struct bmap *get_matching_docids(struct squery *sq) {
    int num_terms = kv_size(sq->q->terms);
    struct squery_result *sqres = sq->sqres;

    // This happens when the query text is empty or not set
    if (num_terms == 0) {
        // Duplicate and send all available docids
        return bmap_duplicate(shard_get_all_docids(sq->shard));
    }

    // This happens when the query text is a single word
    if (num_terms == 1) {
        termdata_t *td = &sqres->termdata[0];
        if (td->tbmap) {
            // TODO: optimize, see if we can get away without duplicating this
            return bmap_duplicate(td->tbmap);
        } else {
            return NULL;
        }
    }

    struct bmap *ret = NULL;
    // This happens when query text is 2 words
    // 'a new' => (a & new) | anew 
    if (num_terms == 3) {
        // Make sure we have matches for both words
        if (sqres->termdata[0].tbmap && sqres->termdata[1].tbmap) {
            struct oper *o = oper_new();
            oper_add(o, sqres->termdata[0].tbmap);
            oper_add(o, sqres->termdata[1].tbmap);
            ret = oper_and(o);
            // Make sure we have common results or throw it out
            if (ret && bmap_cardinality(ret) == 0) {
                bmap_free(ret);
                ret = NULL;
            }
            oper_free(o);
        }
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
     */
    struct oper *o = oper_new();
    for (int i = 0; i < num_terms; i += 2) {
        struct bmap *b = get_term_docids(sqres, i, num_terms);
        // If not term ids are found, we do not have matching results bail out
        if (!b) {
            oper_total_free(o);
            goto last_term;
        } else {
            oper_add(o, b);
        }
    }
    // We have been matching data until now
    ret = oper_and(o);
    oper_total_free(o);
    // If not matches were found, return NULL
    if (bmap_cardinality(ret) == 0) {
        bmap_free(ret);
        ret = NULL;
    }

last_term:
    // Finally or with the last combined terms search
    if (sqres->termdata[num_terms-1].tbmap) {
        if (!ret) {
            return bmap_duplicate(sqres->termdata[num_terms-1].tbmap);
        }
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

static void termdata_free(termdata_t *td) {
    termresult_free(td->tresult);
    bmap_free(td->tbmap);
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
    dump_filter(sf, 0);
    // Free the duplicated shard filter
    filter_free(sf);
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

    // Setup a mdb txn
    mdb_txn_begin(si->env, NULL, MDB_RDONLY, &sq->txn);

    // First lookup all terms
    lookup_terms(sq, si);

    // From the term data, find all documents which match our query
    sq->sqres->docid_map = get_matching_docids(sq);
    if (sq->sqres->docid_map == NULL) {
       sq->sqres->num_hits = 0;
       goto cleanup;
    }
    trace_query("Lookup documents in", &start);

    // Apply filters
    if (sq->q->filter) {
        squery_apply_filters(sq);
    }
    trace_query("Applied filters in", &start);

    sq->sqres->num_hits = bmap_cardinality(sq->sqres->docid_map);

    // Perform ranking
    uint32_t resultcount = sq->sqres->num_hits;
    struct docrank *ranks = perform_ranking(sq, sq->sqres->docid_map, &resultcount);
    trace_query("Ranks calculated in", &start);
    //dump_bmap(sq->sqres->docid_map);
    sq->sqres->rank_count = sort_results(sq->q, ranks, resultcount);
    trace_query("Ranks sorted in", &start);
    sq->sqres->ranks = ranks;

    bmap_free(sq->sqres->docid_map);

cleanup:
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

