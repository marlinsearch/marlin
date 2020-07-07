#include "mbmap.h"
#include "mlog.h"
#include "common.h"
#include "platform.h"

#ifdef DUMP_MDB_STATS
static int whead = 0;
static int wdata = 0;
static int shead = 0;
static int sdata = 0;
static int load = 0;
static int save = 0;
static int wsize = 0;
static int rsize = 0;
static int count = 0;

void mbmap_stats_reset() {
    load = 0;
    save = 0;
    whead = 0;
    wdata = 0;
    shead = 0;
    sdata = 0;
    wsize = 0;
    rsize = 0;
}

void mbmap_stats_dump() {
    printf("ws %10d rs %10d l %10d s %10d wh %d wd %d sh %d sd %d\n", wsize, rsize, load, save, whead, wdata, shead, sdata);
    count++;
}
#endif

static inline uint16_t highbits(uint32_t i) {
    return i>>16;
}

static inline uint16_t lowbits(uint32_t i) {
    return (i & 0xFFFF);
}

static int binary_search(struct mbmap *b, uint16_t id) {
    int low = 0, high = b->num_c-1;
    // Usually we add to the end, handle that
    if (b->num_c > 0) {
        if (b->c[high].id == id) return high;
        if (b->c[high].id < id) return -b->num_c - 1;
    }
    while (low <= high) {
        int middle = (low+high)/2;
        if (b->c[middle].id < id) {
            low = middle+1;
        } else if (b->c[middle].id > id){
            high = middle-1;
        } else {
            return middle;
        }
    }
    return -(low+1);
}

void mbmap_add(struct mbmap *b, uint32_t item, MDB_txn *txn, MDB_dbi dbi) {
    uint16_t id = highbits(item);
    uint16_t val = lowbits(item);
    int pos = binary_search(b, id);
    int rc = 0;
    if (pos >= 0) {
        // If container has not been loaded, now is a good time to load it
        if (!b->c[pos].cont.buffer) {
            uint64_t bid = b->id + b->c[pos].id + 1;
            MDB_val key, data;
            key.mv_size = sizeof(bid);
            key.mv_data = &bid;
            if ((rc = mdb_get(txn, dbi, &key, &data)) == 0) {
#ifdef DUMP_MDB_STATS
                rsize += data.mv_size;
#endif
                b->c[pos].cont.buffer = malloc(data.mv_size);
                memcpy(b->c[pos].cont.buffer, data.mv_data, data.mv_size);
            } else {
                M_ERR("Failed to load mbmap container bid %"PRIu64" id %u cid %u ", bid, b->id, b->c[pos].id, mdb_strerror(rc));
                return;
            }
        }
        cont_add(&b->c[pos].cont, val);
        return;
    }
    pos = -pos-1;
    b->num_c++;
    b->c = realloc(b->c, (b->num_c)*sizeof(struct mcont));
    b->write_header = true;
    if (b->num_c-1 > pos) {
        memmove(b->c+pos+1, b->c+pos, (b->num_c-1-pos)*sizeof(struct mcont));
    }
    b->c[pos].id = id;
    b->c[pos].cont.buffer = malloc(sizeof(uint16_t) * 3);
    b->c[pos].cont.buffer[0] = id;
    b->c[pos].cont.buffer[1] = 0;
    b->c[pos].cont.buffer[2] = 0;
    cont_add(&b->c[pos].cont, val);
}

void mbmap_remove(struct mbmap *b, uint32_t item, MDB_txn *txn, MDB_dbi dbi) {
    uint16_t id = highbits(item);
    uint16_t val = lowbits(item);
    int pos = binary_search(b, id);
    if (pos >= 0) {
        // Load it if required
        if (!b->c[pos].cont.buffer) {
            uint64_t bid = b->id + b->c[pos].id + 1;
            MDB_val key, data;
            key.mv_size = sizeof(bid);
            key.mv_data = &bid;
            if (mdb_get(txn, dbi, &key, &data) == 0) {
                b->c[pos].cont.buffer = malloc(data.mv_size);
                memcpy(b->c[pos].cont.buffer, data.mv_data, data.mv_size);
            } else {
                M_ERR("Failed to load mbmap container");
                return;
            }
        }
        bool remove = cont_remove(&b->c[pos].cont, val);
        // If remove is true, the container itself has to be removed !
        if (remove) {
            // Delete from mdb
            uint64_t bid = b->id + b->c[pos].id + 1;
            MDB_val key;
            key.mv_size = sizeof(bid);
            key.mv_data = &bid;
            if (mdb_del(txn, dbi, &key, NULL) != 0) {
                M_ERR("Failed to deallocate data for mbmap");
            }
 
            b->write_header = true;
            b->num_c--;
            if (pos != b->num_c) {
                memmove(&b->c[pos], &b->c[pos+1], (b->num_c-pos)*sizeof(struct mcont));
            }
        }
    }
}

struct mbmap *mbmap_new(uint64_t id) {
    //printf("mbmap new %"PRIu64" \n", id);
    struct mbmap *b = calloc(1, sizeof(struct mbmap));
    if (!b) {
        M_ERR("Calloc failure!");
        return NULL;
    }
    b->id = id;
    return b;
}

void mbmap_load(struct mbmap *b, MDB_txn *txn, MDB_dbi dbi) {
    MDB_val key, data;
    key.mv_size = sizeof(b->id);
    key.mv_data = &b->id;
    if (mdb_get(txn, dbi, &key, &data) == 0) {
        uint16_t *buf = data.mv_data;
        b->num_c = *buf;
        b->c = calloc(b->num_c, sizeof(struct mcont));
        buf++;
#ifdef DUMP_MDB_STATS
        rsize += data.mv_size;
        load++;
#endif
        for (int i=0; i<b->num_c; i++) {
            b->c[i].id = *buf;
            buf++;
       }
    }
}

uint32_t mbmap_get_cardinality(struct mbmap *b) {
    uint32_t cardinality = 0;
    // printf("num_c is %u\n", b->num_c);
    for (int i=0; i<b->num_c; i++) {
        //cardinality += get_cont_cardinality(&b->c[i].cont);
    }
    return cardinality;
}

bool mbmap_save(struct mbmap *b, MDB_txn *txn, MDB_dbi dbi) {
    uint64_t id = 0;
    MDB_val key, data;
    key.mv_size = sizeof(id);
    key.mv_data = &id;
    // Nothing left, just delete this key
    if (b->num_c == 0) {
        id = b->id;
        if (mdb_del(txn, dbi, &key, NULL) != 0) {
            //M_DBG("Failed to delete empty mbmap");
        }
        return false;
    }
#ifdef DUMP_MDB_STATS
    save++;
#endif
    // Write header if required
    if (UNLIKELY(b->write_header)) {
        id = b->id;
        data.mv_size = sizeof(uint16_t) * (b->num_c + 1);
#ifdef DUMP_MDB_STATS
        wsize += data.mv_size;
        whead++;
#endif
        int rc = mdb_put(txn, dbi, &key, &data, MDB_RESERVE);
        if (rc != 0) {
            M_ERR("MDB reserve failure mbmap save %s", mdb_strerror(rc));
        } else {
            uint16_t *buf = data.mv_data;
            *buf = b->num_c;
            buf++;
            for (int i=0; i<b->num_c; i++) {
                *buf = b->c[i].id;
                buf++;
            }
        }
    } 
#ifdef DUMP_MDB_STATS
    else {
        shead++;
    }
#endif
    for (int i=0; i<b->num_c; i++) {
        // If a buffer is valid, dump it
        if (UNLIKELY(b->c[i].cont.buffer)) {
            // 0 is taken for header, rest are store with id + 1
            id = b->id + b->c[i].id + 1;
            int len = (cont_cardinality(&b->c[i].cont) > CUTOFF)?CUTOFF:b->c[i].cont.buffer[1];
            len += 2; // id + card
            data.mv_size = sizeof(uint16_t) * len;
#ifdef DUMP_MDB_STATS
            wsize += data.mv_size;
            wdata++;
#endif
            data.mv_data = b->c[i].cont.buffer;
            int rc = mdb_put(txn, dbi, &key, &data, 0);
            if (rc != 0) {
                M_ERR("MDB failure mbmap save %s", mdb_strerror(rc));
            }
        } 
#ifdef DUMP_MDB_STATS
        else {
            sdata++;
        }
#endif
    }
    return true;
}


struct bmap *mbmap_load_bmap(MDB_txn *txn, MDB_dbi dbi, uint64_t id) {
    MDB_val key, data;
    key.mv_size = sizeof(id);
    key.mv_data = &id;
    int rc = 0;
    if ((rc = mdb_get(txn, dbi, &key, &data)) == 0) {
        struct bmap *b = bmap_new();
        uint16_t *buf = data.mv_data;
        // TODO: Verify mv_size !
        b->num_c = *buf;
        buf++;
        b->mdb_bmap = 1;
        b->c = calloc(b->num_c, sizeof(struct cont));
        for (int i=0; i<b->num_c; i++) {
            uint16_t cid = *buf;
            buf++;
            uint64_t bcid = id + cid + 1;
            key.mv_data = &bcid;
            MDB_val data2;
            if ((rc = mdb_get(txn, dbi, &key, &data2)) == 0) {
                b->c[i].buffer = data2.mv_data;
            } else {
                M_ERR("Could not load buffer data dbi %d bcid %"PRIu64" id %"PRIu64" cid %u numc %u %s", dbi, bcid, id, cid, b->num_c, mdb_strerror(rc));
            }
        }
        return b;
    } else {
        // M_INFO("Failed to load mbmap %s\n", mdb_strerror(rc));
    }
    return NULL;
}


void mbmap_free(struct mbmap *b) {
    for (int i=0; i<b->num_c; i++) {
        if (b->c[i].cont.buffer) {
            free(b->c[i].cont.buffer);
        }
    }
    free(b->c);
    free(b);
}

