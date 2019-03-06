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

#define TRACE_QUERY 1

static struct squery_result *squery_result_new(void) {
    struct squery_result *sqres = calloc(1, sizeof(struct squery_result));
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
    khiter_t k;
    khash_t(WID2TYPOS) *wordids = data;
    kh_set(WID2TYPOS, wordids, wid, 0);
}


static void set_wids_under_twid(struct squery *sq, struct sindex *si, termresult_t *tr) {
    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    struct bmap *b = mbmap_load_bmap(sq->txn, si->twid2widbmap_dbi, IDPRIORITY(tr->twid, 0));
    if (b) {
        bmap_iterate(b, add_wid_to_wordids, tr->wordids);
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
        kh_foreach_key(tr->wordids, wid, {
            struct bmap *b = mbmap_load_bmap(sq->txn, si->wid2bmap_dbi, IDPRIORITY(wid, 0));
            if (b) {
                oper_add(o, b);
            }
        });
        td->tbmap = oper_or(o);
        oper_total_free(o);
    }

#if 0
#ifdef TRACK_WIDS
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

void sqresult_free(struct squery_result *sqres) {
    free(sqres);
}

#ifdef TRACE_QUERY
static inline void trace_query(const char *msg, struct timeval *start) {
    struct timeval stop;
    gettimeofday(&stop, NULL);
    printf("%s %f\n", msg, timedifference_msec(*start, stop));
}
#else
#define trace_query(msg, start) ;
#endif


void execute_squery(void *w) {
    struct timeval start;
    struct squery *sq = w;

    // Start time
    gettimeofday(&start, NULL);

    int num_terms = kv_size(sq->q->terms);
    M_DBG("Performing squery for shard %d", sq->shard_idx);
    // First allocate a sq_result
    sq->sqres = squery_result_new();
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

    // TODO: Apply filters

    sq->sqres->num_hits = bmap_cardinality(sq->sqres->docid_map);
    printf("Found %d hits\n", sq->sqres->num_hits);

    // Perform ranking
    uint32_t resultcount = sq->sqres->num_hits;
    struct docrank *ranks = perform_ranking(sq, sq->sqres->docid_map, &resultcount);
    trace_query("Ranks calculated in", &start);
    //dump_bmap(sq->sqres->docid_map);

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

