/**
 * Shard query deals with querying a given sindex (shard index) and sdata (shard data)
 * to form results for a query for this shard
 */
#include "squery.h"
#include "workers.h"
#include "debug.h"

static struct squery_result *squery_result_new(void) {
    struct squery_result *sqres = calloc(1, sizeof(struct squery_result));
    kv_init(sqres->termdata);
    return sqres;
}

static struct bmap *get_twid_to_objids(struct squery *sq, struct sindex *si, uint32_t twid) {
    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    // read the query to find that out
    struct bmap *b = mbmap_load_bmap(sq->txn, si->twid2bmap_dbi, IDPRIORITY(twid, 0));
    // If we have any objids under this twid, return it
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
    // we do not have any objects matching that term
    if ((tr->twid == 0) && (kh_size(tr->wordids) == 0)) return;

    // TODO: Currently it handles matches from all fields, restrict based on requested fields
    // If we have a top level word id set, we can use the top level object matches
    // We will need to load the words under this top level id though
    if (tr->twid) {
        td->tbmap = get_twid_to_objids(sq, si, tr->twid);
        // If we have objids under this twid, let us get all the words that are under it
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
}

static void lookup_terms(struct squery *sq, struct sindex *si) {
    int num_terms = kv_size(sq->q->terms);
    struct squery_result *sqres = sq->sqres;

    for (int i = 0; i < num_terms; i++) {
        struct termdata *td = calloc(1, sizeof(struct termdata));
        td->tresult = dtrie_lookup_term(si->trie, kv_A(sq->q->terms, i));
        process_termresult(sq, si, td);
        dump_termresult(td->tresult);
        kv_push(termdata_t *, sqres->termdata, td);
    }
}

static void termdata_free(termdata_t *td) {
    termresult_free(td->tresult);
    bmap_free(td->tbmap);
    free(td);
}

void sqresult_free(struct squery_result *sqres) {
    free(sqres);
}

void execute_squery(void *w) {
    struct squery *sq = w;
    M_INFO("Performing squery for shard %d", sq->shard_idx);
    // First allocate a sq_result
    sq->sqres = squery_result_new();
    // Let the shard index handle the query now
    struct shard *s = kv_A(sq->q->in->shards, sq->shard_idx);
    struct sindex *si = s->sindex;

    // Setup a mdb txn
    mdb_txn_begin(si->env, NULL, MDB_RDONLY, &sq->txn);
    // First lookup all terms
    lookup_terms(sq, si);

    // cleanup all termdata
    for (int i = 0; i < kv_size(sq->sqres->termdata); i++) {
        termdata_free(kv_A(sq->sqres->termdata, i));
    }
    kv_destroy(sq->sqres->termdata);
    
    // Abort the read only transaction, we are done executing the query
    mdb_txn_abort(sq->txn);

    worker_done(sq->worker);
}

