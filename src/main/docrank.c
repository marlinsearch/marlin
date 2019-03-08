#include "docrank.h"
#include "squery.h"

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

static inline int word_dist(khash_t(WID2TYPOS) *wordids, uint32_t wid) {
    khiter_t k = kh_get(WID2TYPOS, wordids, wid);
    if (k != kh_end(wordids)) {
        return kh_value(wordids, k);
    }
    return 0xff;
}

static inline void get_priority_position(wid_info_t *info, uint8_t *head, int *priority, int *position) {
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
}

static inline void rank_single_term(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    khash_t(WID2TYPOS) *allwordids = sq->sqres->termdata[0].tresult->wordids;
    uint16_t numwid = *(uint16_t *) dpos;
    uint8_t *pos = dpos + 2;
    wid_info_t *info = (wid_info_t *)pos;
    int priority, position;

    // Check if we have an exact word match, set rank & typos accordingly
    if (sq->sqres->exact_docid_map[0]) {
        if (bmap_exists(sq->sqres->exact_docid_map[0], rank->docid)) {
            rank->exact = 1;
            rank->typos = 0;
        }
    }

    // Iterate over all words and check which word matches and set rank fields
    for (int i = 0; i < numwid; i++) {
        int dist = word_dist(allwordids, info[i].wid);
        if (dist < rank->typos) {
            rank->typos = dist;
        }
        if (dist != 0xff) {
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


static inline void calculate_rank(struct docrank *rank, struct squery *sq, uint8_t *dpos) {
    int num_terms = kv_size(sq->q->terms);
    if (num_terms == 0) {
        rank->position = 0;
        rank->proximity = 0;
        rank->exact = 0;
        rank->field = 0;
        rank->typos = 0;
        return;
    }
    if (num_terms == 1) {
        rank->proximity = 0;
        rank->exact = 0;
        rank->field = 0xFF;
        rank->typos = 0xFF;
        rank->position = 0xFFFF;
        rank_single_term(rank, sq, dpos);
    }
}

static inline void perform_doc_rank(struct squery *sq, struct docrank *rank) {
    MDB_val key, mdata;
    key.mv_size = sizeof(uint32_t);
    int rc;
    key.mv_data = &rank->docid;
    struct sindex *si = sq->shard->sindex;
    if ((rc = mdb_get(sq->txn, si->docid2wpos_dbi, &key, &mdata)) != 0) {
        //S_ERR("Failed to get mdb data for obj data %llu %d %d", tdata->ranks[i].objid, rc, i);
        // Push this result down below
        rank->typos = 0xFF;
        rank->position = 0xFFFF;
        rank->proximity = 0xFFFF;
        return;
    }

    //if (LIKELY(sq->cfg.hits_per_page > 0)) {
        calculate_rank(rank, sq, mdata.mv_data);
    //}
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
