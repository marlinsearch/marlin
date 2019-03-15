#include "filter_apply.h"

static void (*filter_callback[F_ERROR+1]) (struct sindex *in, struct filter *, 
             MDB_txn *txn, struct bmap *docs);


static inline void num_eq_filter(struct sindex *in, struct filter *f, 
        MDB_txn *txn, struct bmap *docs) {
    f->fr_bmap = bmap_new();

    // Iterate num_dbi for the particular field and add matching items
    MDB_val key, data;
    key.mv_size = sizeof(double);
    key.mv_data = &f->numval;
    MDB_cursor *cursor;
    mdb_cursor_open(txn, in->num_dbi[f->s->i_priority], &cursor);
    MDB_cursor_op op = MDB_SET_KEY;
    while(mdb_cursor_get(cursor, &key, &data, op) == 0) {
        op = MDB_NEXT;
        double *kd = key.mv_data;
        uint64_t *dd = data.mv_data;
        if (*kd == f->numval) {
            // Iterate till we got the same key and fill up the bitmap
            bmap_add(f->fr_bmap, (uint32_t) *dd);
        } else {
            break;
        }
    }
    mdb_cursor_close(cursor);
}

/* Equal filter, based on filter field type choose appropriate filter */
static void eq_filter(struct sindex *in, struct filter *f, MDB_txn *txn, struct bmap *docs) {
    switch (f->field_type) {
        case F_NUMBER:
            num_eq_filter(in, f, txn, docs);
            break;
        default:
            M_ERR("Field type not support for EQ filter");
            break;
    }
}

static void error_filter(struct sindex *in, struct filter *f, MDB_txn *txn, struct bmap *docs) {
    M_ERR("Filters should not be applied for error filters !");
    assert(0);
}

/**
 * Applies a filter for a given sindex and a list of matching docs.
 * The response is all documents that match a filter, it may or may not be restricted to docs
 * The response from this needs to be AND with docs finally to get the filtered docs
 */
struct bmap *filter_apply(struct sindex *in, struct filter *f, MDB_txn *txn, struct bmap *docs) {
    for (int i = 0; i < kv_size(f->children); i++) {
        filter_apply(in, kv_A(f->children, i), txn, docs);
    }
    filter_callback[f->type](in, f, txn, docs);
    return f->fr_bmap;
}

void init_filter_callbacks(void) {
    filter_callback[F_EQ] = eq_filter;
    filter_callback[F_ERROR] = error_filter;
}
