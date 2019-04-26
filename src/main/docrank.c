#include "docrank.h"
#include "squery.h"
#include "ksort.h"
#include "common.h"
#include "float.h"
#include "aggs.h"

#define positions_lt(a, b) ((a) < (b))
KSORT_INIT(sort_positions, int, positions_lt)

// Fill positions in which the given word is present.  Field & position values are 
// converted to a single int 
// TODO: Fill only positions for fields requested
static int *fill_positions(wid_info_t *info, uint8_t *head, int *p, int *len) {
    int *ret;
    int plen = *len;
    if (info->is_position) {
        ret = realloc(p, (plen + 1) * sizeof(int));
        ret[plen] = (info->priority << 16) + info->offset;
        *len = plen + 1;
    } else {
        uint8_t *c = head + info->offset;
        int freq = *c;
        ret = realloc(p, (plen + freq) * sizeof(int));
        c++;
        while (freq > 0) {
            int priority = *c;
            int position;
            // Skip priority
            c++;
            int count = *c;
            freq -= count;
            // Skip count
            c++;
            while (count > 0) {
                c = read_vint(c, &position);
                ret[plen] = (priority << 16) + position;
                count--; plen++;
            }
            // Skip the separator 0xFF
            c++;
        }
        // Set length
        *len = plen;
    }
    return ret;
}

static inline void get_priority_position(wid_info_t *info, uint8_t *head, int *priority, int *position) {
    if (info->is_position) {
        *priority = info->priority;
        *position = info->offset;
    } else {
        uint8_t *p = head + info->offset;
        // Skip frequency
        p++;
        *priority = *p;
        p += 2; // skip priority and count
        read_vint(p, position);
    }
}

static inline void rank_single_term(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    khash_t(WID2TYPOS) *all_wordids = sq->sqres->all_wordids;
    uint16_t numwid = *(uint16_t *) dpos;
    uint8_t *pos = dpos + 2;
    wid_info_t *info = (wid_info_t *)pos;
    int priority, position;
    khiter_t k;
    int dist;
    int min_dist = 0xFF-1;

    // Check if we have an exact word match, set rank & typos accordingly
    if (sq->sqres->exact_docid_map[0]) {
        if (bmap_exists(sq->sqres->exact_docid_map[0], rank->docid)) {
            rank->exact = 1;
            rank->typos = 0;
            min_dist = 0;
        }
    }

    // Iterate over all words and check which word matches and set rank fields
    for (int i = 0; i < numwid; i++) {
        if ((dist = kh_get_val(WID2TYPOS, all_wordids, info[i].wid, 0xFF)) <= min_dist) {
            min_dist = dist;

            // We already have a better result, skip this one
            if (dist > rank->typos) continue;

            // If typos are lower, reset the field and position to store this one
            if (dist < rank->typos) {
                rank->typos = dist;
                rank->field = 0xFF;
                rank->position = 0xFFFF;
            }

            get_priority_position(&info[i], dpos, &priority, &position);

            if (priority > rank->field) continue;

            if (priority < rank->field) {
                rank->field = priority;
                rank->position = position;
            } else {
                // priority == rank->field, update the position
                if (position < rank->position) {
                    rank->position = position;
                }
            }
        }
    }
}

static int get_matching_term(struct squery_result *sqres, int num_terms, uint32_t wid) {
    khiter_t k;
    for (int i = 0; i < num_terms; i++) {
        if (kh_get_val(WID2TYPOS, sqres->termdata[i].tresult->wordids, 
                        wid, 0xFF) != 0xFF) {
            return i;
        }
    }
    return 0xFF;
}

static inline void rank_three_terms(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    // aaa bbb => aaa bbb aaabbb
    khash_t(WID2TYPOS) *all_wordids = sq->sqres->all_wordids;
    int num_words = 2;
    int num_terms = 3;
    int typos[2] = {0xFF, 0xFF};
    int *positions[3] = {0, 0, 0};
    int plength[3] = {0, 0, 0};
    // First set exact matches and typos
    for (int i = 0; i < num_words; i++) {
        if (sq->sqres->exact_docid_map[i]) {
            if (bmap_exists(sq->sqres->exact_docid_map[i], rank->docid)) {
                rank->exact++;
                typos[i] = 0;
            }
        }
    }

    uint16_t numwid = *(uint16_t *) dpos;
    uint8_t *pos = dpos + 2;
    wid_info_t *info = (wid_info_t *)pos;
    khiter_t k;
    int dist;

    // Iterate over all words and check which word matches which terms and set rank info
    for (int i = 0; i < numwid; i++) {
        // Ok the word matches some term
        if ((dist = kh_get_val(WID2TYPOS, all_wordids, info[i].wid, 0xFF)) != 0xFF) {
            int mterm = get_matching_term(sq->sqres, num_terms, info[i].wid);
            // If it matches the combined term / last term proximity is 1
            if (UNLIKELY(mterm == 2)) {
                rank->proximity = 1;
                typos[0] = typos[1] = 0;
            } else {
                if (dist < typos[mterm]) {
                    typos[mterm] = dist;
                }
            }
            // Now dump positions for this term / word
            positions[mterm] = fill_positions(&info[i], dpos, positions[mterm], &plength[mterm]);
        }
    }

    // Sort the position information, keep track of typos and best matching field / position
    uint32_t best_position = 0xFFFFFFFF;
    rank->typos = 0;
    for (int x = 0; x < num_terms; x++) {
        if (plength[x]) {
            ks_introsort(sort_positions, plength[x], positions[x]);
            if (positions[x][0] < best_position) {
                best_position = positions[x][0];
            }
        }
        if (x < num_words) {
            rank->typos += typos[x];
        }
    }
    // Set the field / position from best position
    rank->field = (best_position >> 16);
    rank->position = best_position & 0xFFFF;

    // Finally calculate proximity if required
    if (LIKELY(rank->proximity != 1)) {
        int i = 0, j = 0, p1len = plength[0], p2len = plength[1];
        int mindiff = 8;
        while((i < p1len) && (j < p2len)) {
            int pos1 = positions[0][i];
            int pos2 = positions[1][j];
            int diff = abs(pos1 - pos2);
            // TODO: If diff is 0, discard this result?
            // str="aaaaa test me" and search for "aaaaa aaaaa"
            if (diff != 0) {
                if (diff < mindiff) {
                    mindiff = diff;
                    if (mindiff == 1) break;
                }
            }
            pos1 > pos2 ? j++ : i++;
        }
        rank->proximity = mindiff;
    }
    for (int i = 0; i < num_terms; i++) {
        free(positions[i]);
    }
}

static inline int term_to_word_idx(int term, int num_terms) {
    if (UNLIKELY(term) == (num_terms - 1)) return 0;
    return term / 2;
}

//  0  1   2     3       0   1     2     3       4       5         6        7
// (a new hope movie) => a, anew, new, newhope, hope, hopemovie, movie, anewhopemovie
static inline void rank_many_terms(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    // aaa bbb => aaa bbb aaabbb
    khash_t(WID2TYPOS) *all_wordids = sq->sqres->all_wordids;
    int num_words = sq->q->num_words;
    int num_terms = kv_size(sq->q->terms);
    int typos[num_words];
    int *positions[num_words];
    int plength[num_words];
    int proximity[num_words];

    // First set exact matches and typos
    for (int i = 0; i < num_words; i++) {
        if (sq->sqres->exact_docid_map[i]) {
            if (bmap_exists(sq->sqres->exact_docid_map[i], rank->docid)) {
                rank->exact++;
                typos[i] = 0;
            } else {
                typos[i] = 0xFF;
            }
        } else {
            typos[i] = 0xFF;
        }
        positions[i] = NULL;
        plength[i] = 0;
        proximity[i] = 8;
    }

    rank->proximity = 0;

    uint16_t numwid = *(uint16_t *) dpos;
    uint8_t *pos = dpos + 2;
    wid_info_t *info = (wid_info_t *)pos;
    khiter_t k;
    int dist;

    // Iterate over all words and check which word matches which terms and set rank info
    for (int i = 0; i < numwid; i++) {
        // Ok the word matches some term
        if ((dist = kh_get_val(WID2TYPOS, all_wordids, info[i].wid, 0xFF)) != 0xFF) {
            int mterm = get_matching_term(sq->sqres, num_terms, info[i].wid);
            // If it matches the combined term / last term proximity is 1
            if (UNLIKELY(mterm == (num_terms-1))) {
                rank->proximity = num_words - 1;
                memset(&typos, 0, sizeof(typos));
            }
            int widx = term_to_word_idx(mterm, num_terms);
            
            if (dist < typos[widx]) {
                typos[widx] = dist;
            }
            // Now dump positions for this term / word
            positions[widx] = fill_positions(&info[i], dpos, positions[widx], &plength[widx]);
            if ((mterm % 2) == 1) {
                proximity[widx] = 1;
                // anew -> gets filled in both a and new, ignore last term match
                if (mterm != (num_terms - 1)) {
                    typos[widx + 1] = 0;
                    positions[widx + 1] = fill_positions(&info[i], dpos, 
                            positions[widx + 1], &plength[widx + 1]);
                }
            }
        }
    }

    // Sort the position information, keep track of typos and best matching field / position
    uint32_t best_position = 0xFFFFFFFF;
    rank->typos = 0;
    for (int x = 0; x < num_words; x++) {
        if (plength[x]) {
            ks_introsort(sort_positions, plength[x], positions[x]);
            if (positions[x][0] < best_position) {
                best_position = positions[x][0];
            }
        }
        rank->typos += typos[x];
    }

    // Set the field / position from best position
    rank->field = (best_position >> 16);
    rank->position = best_position & 0xFFFF;

    // Finally calculate proximity if required
    if (LIKELY(rank->proximity != num_words-1)) {
        for (int x = 0; x < num_words-1; x++) {
            if (proximity[x] == 1) goto add_prox;
            int i = 0, j = 0, p1len = plength[x], p2len = plength[x+1];
            int mindiff = 8;
            while((i < p1len) && (j < p2len)) {
                int pos1 = positions[x][i];
                int pos2 = positions[x+1][j];
                int diff = abs(pos1 - pos2);
                // TODO: If diff is 0, discard this result?
                // str="aaaaa test me" and search for "aaaaa aaaaa"
                if (diff != 0) {
                    if (diff < mindiff) {
                        mindiff = diff;
                        if (mindiff == 1) break;
                    }
                }
                pos1 > pos2 ? j++ : i++;
            }
            proximity[x] = mindiff;
add_prox:
            rank->proximity += proximity[x];
        }
    }

    for (int i = 0; i < num_words; i++) {
        free(positions[i]);
    }
}

#if 0
static void dump_rank(struct docrank *rank) {
    printf("\nDoc id is %u\n", rank->docid);
    printf("pos %d field %d prox %d exact %d typos %d compare %f\n",
            rank->position, rank->field, rank->proximity, rank->exact, rank->typos, rank->compare);
}
#endif

static inline void calculate_rank(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    int num_words = sq->q->num_words;
    if (num_words == 0) {
        rank->position = 0;
        rank->proximity = 0;
        rank->exact = 0;
        rank->field = 0;
        rank->typos = 0;
        return;
    }

    rank->field = 0xFF;
    rank->typos = 0xFF;
    rank->position = 0xFFFF;
    rank->exact = 0;

    // Single word results in 1 term
    if (num_words == 1) {
        // Proximity is always zero for a single term
        rank->proximity = 0;
        rank_single_term(rank, sq, dpos);
    }
    // 2 words results in 3 terms 'aaa bbb' gives 'aaa', 'bbb', 'aaabbb'
    else if (num_words == 2) {
        rank->proximity = 0xFFFF;
        rank_three_terms(rank, sq, dpos);
    } else {
        rank->proximity = 0xFFFF;
        rank_many_terms(rank, sq, dpos);
    }

}

static inline void process_facet_result(struct squery *sq, uint8_t *pos) {
    struct mapping *m = sq->q->in->mapping;
    // calculate facet offset
    size_t fo = m->num_numbers * sizeof(double);
    // Get the facet position
    uint8_t *fpos = pos + fo;
    for (int i = 0; i < m->num_facets; i++) {
        facets_t *f = (facets_t *)fpos;
        // If facet is enabled, count it
        if (sq->q->cfg.facet_enabled[i]) {
            for (int j = 0; j < f->count; j++) {
                struct cell *c = hashtable_insert(sq->sqres->fh[i].h, f->data[j]);
                c->value += 1;
            }
        }
        // Move to next facet
        fpos += ((f->count * sizeof(uint32_t)) + sizeof(uint32_t));
    }
}

static inline double setup_rank_compare(int rank_by, void *pos) {
    double *dpos = pos;
    return dpos[rank_by];
}

static inline void perform_doc_processing(struct squery *sq, struct docrank *rank, uint8_t *pos) {
    // If we have any facets enabled, process that
    if (sq->q->cfg.max_facet_results && sq->q->cfg.facet_enabled) {
        process_facet_result(sq, pos + sizeof(uint32_t));
    }

    // Handle sort / rankby field
    if (LIKELY(sq->q->cfg.rank_by >= 0)) {
        rank->compare = setup_rank_compare(sq->q->cfg.rank_by, pos + sizeof(uint32_t));
    }

    // Handle aggregations
    // TODO: how do we handle partial scan ?? Force full scan if we have an agg?
    if (sq->sqres->agg && !sq->fast_rank) {
        sq->sqres->agg->consume(sq->sqres->agg, sq, rank->docid, pos);
    }
}

static inline void perform_doc_rank(struct squery *sq, struct docrank *rank) {
    MDB_val key, mdata;
    key.mv_size = sizeof(uint32_t);
    int rc;
    key.mv_data = &rank->docid;
    struct sindex *si = sq->shard->sindex;
    if ((rc = mdb_get(sq->txn, si->docid2data_dbi, &key, &mdata)) != 0) {
        // Push this result down, we could not read this docdata from mdb
        M_ERR("Failed to read docdata for %u", rank->docid);
        rank->typos = 0xFF;
        rank->position = 0xFFFF;
        rank->proximity = 0xFFFF;
        rank->compare = sq->q->cfg.rank_asc ? DBL_MAX : -DBL_MAX;
        return;
    }
    if ((sq->q->cfg.hits_per_page > 0) && (sq->q->num_words > 0)) {
        uint32_t offset = *(uint32_t *)mdata.mv_data;
        uint8_t *wpos = (uint8_t *)mdata.mv_data;
        wpos += offset;
        calculate_rank(rank, sq, wpos);
    } else {
        rank->position = 0;
        rank->proximity = 0;
        rank->exact = 0;
        rank->field = 0;
        rank->typos = 0;
    }
    // Perform further processing for facets / aggregations
    perform_doc_processing(sq, rank, mdata.mv_data);
}

static void calculate_agg(uint32_t val, void *rptr) {
    struct squery *sq = rptr;
    sq->sqres->agg->consume(sq->sqres->agg, sq, val, NULL);
}

static void setup_ranks(uint32_t val, void *rptr) {
    struct rank_iter *iter = rptr;
    struct docrank *rank = &iter->ranks[iter->rankpos++];
    rank->docid = val;
    perform_doc_rank(iter->sq, rank);
}

static void setup_zero_typo_ranks(uint32_t val, void *rptr) {
    struct rank_iter *iter = rptr;
    struct docrank *rank = &iter->ranks[iter->rankpos++];
    rank->docid = val;
    // Ranking is performed using document words / positions
    perform_doc_rank(iter->sq, rank);
    bmap_add(iter->dbmap, val);
}

static void setup_skip_ranks(uint32_t val, void *rptr) {
    struct rank_iter *iter = rptr;
    if (iter->skip_counter++ % iter->skip_count == 0) {
        if (!bmap_exists(iter->dbmap, val)) {
            struct docrank *rank = &iter->ranks[iter->rankpos++];
            rank->docid = val;
            // Ranking is performed using document words / positions
            perform_doc_rank(iter->sq, rank);
        }
    }
}

static struct docrank *perform_fast_ranking(struct squery *sq, struct bmap *docid_map, 
        uint32_t *resultcount) {
    uint32_t totalcount = *resultcount;

    // First limit the number of documents we will be scanning for this ranking
    int fcount = MIN((totalcount / 100), MAX_HITS_LIMIT * 10);
    fcount = MAX(fcount, MAX_HITS_LIMIT * 5);

    M_DBG("Performing fast rank on %d documents", fcount);

    // Have a bitmap to track docs that have been ranked
    struct bmap *dbmap = bmap_new();

    // Allocate enough space with buffer for ranks
    struct docrank *ranks = malloc((MAX_HITS_LIMIT + fcount + 10) * sizeof(struct docrank));

    // Get the preferred result document ids, first prefer all documents
    struct bmap *rmap = docid_map;
    struct rank_iter riter;
    int rankpos = 0;

    riter.sq = sq;
    riter.rankpos = 0;
    riter.ranks = ranks;
    riter.dbmap = dbmap;

    // Prefer docids with zero typos if necessary
    int zt_card = bmap_cardinality(sq->sqres->zero_typo_docid_map);
    if (zt_card) {
        // If documents with zero typos is less than max possible hits, include all documents
        if (zt_card <= MAX_HITS_LIMIT) {
            bmap_iterate(sq->sqres->zero_typo_docid_map, setup_zero_typo_ranks, &riter);
            rankpos = riter.rankpos;
        } else {
            // If we have many documents with zero typos, prefer that
            rmap = sq->sqres->zero_typo_docid_map;
        }
    }

    // Now prefer items by ranking order
    MDB_cursor *cursor;
    MDB_val key, data;
    struct sindex *si = sq->shard->sindex;
    MDB_cursor_op op = sq->q->cfg.rank_asc? MDB_LAST:MDB_FIRST;
    MDB_cursor_op op2 = sq->q->cfg.rank_asc? MDB_PREV:MDB_NEXT;
    mdb_cursor_open(sq->txn, si->num_dbi[sq->q->cfg.rank_by], &cursor);

    // Iterate items by ranking order and rank them if they are matching documents
    while(mdb_cursor_get(cursor, &key, &data, op) == 0) {
        op = op2;
        uint32_t docid = *(uint32_t *)data.mv_data;
        // Make sure this doc_id is in our preferred bitmap
        if (bmap_exists(rmap, docid)) {
            // Make sure we did not already add this doc_id
            if (bmap_exists(dbmap, docid)) continue;
            // Add this document
            ranks[rankpos].docid = docid;
            perform_doc_rank(sq, &ranks[rankpos]);
            // Keep track of this document
            bmap_add(dbmap, docid);
            rankpos++;
            // If we have enough documents, break
            if (rankpos >= MAX_HITS_LIMIT) break;
        }
    }
    mdb_cursor_close(cursor);
    riter.rankpos = rankpos;


    // Rest of the documents are picked in a sequential order skipping entries
    riter.skip_count = (totalcount / fcount) + 1;
    riter.skip_counter = 0;
    bmap_iterate(rmap, setup_skip_ranks, &riter);
    bmap_free(dbmap);
    *resultcount = (riter.rankpos - 1);

    // If we have aggregates, do it on all documents
    if (sq->sqres->agg) {
        bmap_iterate(docid_map, calculate_agg, sq);
    }

    return ranks;
}


/* Perform ranking, calculates ranks for documents in the docid_map.  It may do a fullScan or
 * a partial scan based on configuration and number of matching documents */
struct docrank *perform_ranking(struct squery *sq, struct bmap *docid_map, uint32_t *resultcount) {
    uint32_t totalcount = *resultcount;
    struct query *q = sq->q;
    sq->fast_rank = !q->cfg.full_scan;

    // TODO: When aggregation is implemented and enabled, handle fastscan
    if (sq->fast_rank && totalcount > q->cfg.full_scan_threshold && q->cfg.rank_by >= 0) {
        M_INFO("Performing fast ranking");
        return perform_fast_ranking(sq, docid_map, resultcount);
    }

    sq->fast_rank = false;
    struct rank_iter riter;
    struct docrank *ranks = malloc(totalcount * sizeof(struct docrank));
    riter.sq = sq;
    riter.rankpos = 0;
    riter.ranks = ranks;
    riter.dbmap = NULL;

    bmap_iterate(docid_map, setup_ranks, &riter);

    return ranks;
}
