#include "mbmap.h"
#include "mlog.h"

int wrote = 0;

static inline uint16_t highbits(uint32_t i) {
    return i>>16;
}

static inline uint16_t lowbits(uint32_t i) {
    return (i & 0xFFFF);
}

static int binary_search(struct mbmap *b, uint16_t id) {
    int low = 0, high = b->num_c-1, middle;
    // Usually we add to the end, handle that
    if (b->num_c > 0) {
        if (b->c[high].id == id) return high;
        if (b->c[high].id < id) return -b->num_c - 1;
    }
    while (low <= high) {
        middle = (low+high)/2;
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
    if (pos >= 0) {
        // If container has not been loaded, now is a good time to load it
        if (!b->c[pos].cont.buffer) {
            uint64_t bid = b->id + b->c[pos].id + 1;
            MDB_val key, data;
            key.mv_size = sizeof(bid);
            key.mv_data = &bid;
            if (mdb_get(txn, dbi, &key, &data) == 0) {
                b->c[pos].cont.buffer = malloc(data.mv_size);
                memcpy(b->c[pos].cont.buffer, data.mv_data, data.mv_size);
            } else {
                M_ERR("Failed to load mbmap container bid %"PRIu64" id %u cid %u ", bid, b->id, b->c[pos].id);
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
        // TODO: data.mv_size is correct !
        for (int i=0; i<b->num_c; i++) {
            b->c[i].id = *buf;
            buf++;
            // TODO: TEMP lazy loaded on top
            /*
            uint64_t bid = b->id + (b->c[i].id+1);
            key.mv_data = &bid;
            MDB_val data2;
            if (mdb_get(txn, dbi, &key, &data2) == 0) {
                b->c[i].cont.buffer = malloc(data2.mv_size);
                memcpy(b->c[i].cont.buffer, data2.mv_data, data2.mv_size);
            } else {
                M_ERR("Could not load buffer data");
            }*/
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
            M_ERR("Failed to delete empty mbmap");
        }
        return false;
    }
    // Write header if required
    if (b->write_header) {
        wrote++;
        id = b->id;
        data.mv_size = sizeof(uint16_t) * (b->num_c + 1);
        int rc = mdb_put(txn, dbi, &key, &data, MDB_RESERVE);
        if (rc != 0) {
            M_ERR("MDB reserve failure mbmap save %d", rc);
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
    for (int i=0; i<b->num_c; i++) {
        // If a buffer is valid, dump it
        if (b->c[i].cont.buffer) {
            wrote++;
            // 0 is taken for header, rest are store with id + 1
            id = b->id + b->c[i].id + 1;
            int len = (cont_cardinality(&b->c[i].cont) > CUTOFF)?CUTOFF:b->c[i].cont.buffer[1];
            len += 2; // id + card
            data.mv_size = sizeof(uint16_t) * len;
            data.mv_data = b->c[i].cont.buffer;
            int rc = mdb_put(txn, dbi, &key, &data, 0);
            if (rc != 0) {
                M_ERR("MDB failure mbmap save %d", rc);
            }
        }
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
            if (mdb_get(txn, dbi, &key, &data2) == 0) {
                b->c[i].buffer = data2.mv_data;
            } else {
                M_ERR("Could not load buffer data dbi %d bcid %"PRIu64" id %"PRIu64" cid %u numc %u", dbi, bcid, id, cid, b->num_c);
            }
        }
        return b;
    } else {
        // S_INFO("Failed to load mbmap %d\n", rc);
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

