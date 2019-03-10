#include "docrank.h"
#include "squery.h"
#include "ksort.h"

#define positions_lt(a, b) ((a) < (b))
KSORT_INIT(sort_positions, int, positions_lt)

static inline wid_info_t *binary_search(wid_info_t *in, uint32_t wid, int numwid) {
    int low = 0, high = numwid-1, middle;
    while (low <= high) {
        middle = (low+high)/2;
        if (in[middle].wid < wid) {
            low = middle+1;
        } else if (in[middle].wid > wid) {
            high = middle-1;
        } else {
            return &in[middle];
        }
    }
    return NULL;
}

static inline bool lookup_wid(uint8_t *head, uint32_t wid, int *priority, int *position) {
    uint16_t numwid = *(uint16_t *) head;
    uint8_t *pos = head + 2;
    wid_info_t *info = binary_search((wid_info_t *)pos, wid, numwid);
    if (info) {
        if (info->is_position) {
            *priority = info->priority;
            *position = info->offset;
        } else {
            uint8_t *p = head + info->offset;
            // Skip frequency
            p++;
            uint8_t *prio = p;
            *priority = *prio;
            p += 2; // skip priority and count
            read_vint(p, position);
        }
        return true;
    }
    return false;
}

static inline void rank_single_term2(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    khash_t(WID2TYPOS) *allwordids = sq->sqres->termdata[0].tresult->wordids;
    uint32_t wid;
    int typos;
    int priority;
    int position;
    kh_foreach(allwordids, wid, typos, {
        if (lookup_wid(dpos, wid, &priority, &position)) {
            if (typos < rank->typos) {
                rank->typos = typos;
            }
            if (priority < rank->field) {
                rank->field = priority;
                rank->position = position;
            }
            return;
        }
    });

}

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
            c++;
            int count = *c;
            freq -= count;
            while (count > 0) {
                c = read_vint(c, &position);
                ret[plen] = (priority << 16) + position;
                count--; plen++;
            }
            // Skip the separator 0xFF
            c++;
        }
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

    // Check if we have an exact word match, set rank & typos accordingly
    if (sq->sqres->exact_docid_map[0]) {
        if (bmap_exists(sq->sqres->exact_docid_map[0], rank->docid)) {
            rank->exact = 1;
            rank->typos = 0;
        }
    }

    // Iterate over all words and check which word matches and set rank fields
    for (int i = 0; i < numwid; i++) {
        if ((dist = kh_get_val(WID2TYPOS, all_wordids, info[i].wid, 0xFF)) != 0xFF) {
            if (dist < rank->typos) {
                rank->typos = dist;
            }
            get_priority_position(&info[i], dpos, &priority, &position);
            if (priority < rank->field) {
                rank->field = priority;
                rank->position = position;
            }
            // TODO: is this ok ???
            if (priority == 0) break;
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
    }

}

static inline void perform_doc_rank(struct squery *sq, struct docrank *rank) {
    MDB_val key, mdata;
    key.mv_size = sizeof(uint32_t);
    int rc;
    key.mv_data = &rank->docid;
    struct sindex *si = sq->shard->sindex;
    if ((sq->q->cfg.hits_per_page > 0) && (sq->q->num_words > 0)) {
        if ((rc = mdb_get(sq->txn, si->docid2wpos_dbi, &key, &mdata)) != 0) {
            // Push this result down below
            rank->typos = 0xFF;
            rank->position = 0xFFFF;
            rank->proximity = 0xFFFF;
            return;
        }
        calculate_rank(rank, sq, mdata.mv_data);
    } else {
        rank->position = 0;
        rank->proximity = 0;
        rank->exact = 0;
        rank->field = 0;
        rank->typos = 0;
    }
}


static void setup_ranks(uint32_t val, void *rptr) {
    struct rank_iter *iter = rptr;
    struct docrank *rank = &iter->ranks[iter->rankpos++];
    rank->docid = val;
    perform_doc_rank(iter->sq, rank);
}

struct docrank *perform_ranking(struct squery *sq, struct bmap *docid_map, uint32_t *resultcount) {
    struct rank_iter riter;
    uint32_t totalcount = *resultcount;
    struct docrank *ranks = malloc(totalcount * sizeof(struct docrank));
    riter.sq = sq;
    riter.rankpos = 0;
    riter.ranks = ranks;

    bmap_iterate(docid_map, setup_ranks, &riter);

    return ranks;
}
