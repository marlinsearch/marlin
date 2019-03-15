#include "filter_apply.h"
#include "farmhash-c.h"
#include "debug.h"

static void (*filter_callback[F_ERROR+1]) (struct sindex *in, struct filter *, 
             MDB_txn *txn, struct bmap *docs);

static inline void bool_eq_filter(struct sindex *si, struct filter *f, 
        MDB_txn *txn, struct bmap *docs) {
    uint64_t bhid = IDPRIORITY(f->numval, f->s->i_priority);
    f->fr_bmap = mbmap_load_bmap(txn, si->boolid2bmap_dbi, bhid);
}

static inline void str_eq_filter(struct sindex *si, struct filter *f, 
        MDB_txn *txn, struct bmap *docs) {
    // Make sure it is a facetted string, this should never happen though if parsing was done right
    if (!f->s->is_facet) {
        M_ERR("EQ operation on a non-facetted string");
        dump_filter(f, 0);
        return;
    }
    uint32_t facet_id = farmhash32(f->strval, strlen(f->strval));
    uint64_t fhid = IDPRIORITY(facet_id, f->s->f_priority);
    f->fr_bmap = mbmap_load_bmap(txn, si->facetid2bmap_dbi, fhid);
}

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
    if (f->fr_bmap->num_c == 0) {
        bmap_free(f->fr_bmap);
        f->fr_bmap = NULL;
    }
}

/* Equal filter, based on filter field type choose appropriate filter */
static void eq_filter(struct sindex *in, struct filter *f, MDB_txn *txn, struct bmap *docs) {
    switch (f->field_type) {
        case F_NUMBER:
            num_eq_filter(in, f, txn, docs);
            break;
        case F_STRING:
            str_eq_filter(in, f, txn, docs);
            break;
        case F_BOOLEAN:
            bool_eq_filter(in, f, txn, docs);
            break;
        default:
            M_ERR("Field type not support for EQ filter");
            break;
    }
}

/* Not Equal filter Invert results of a eq filter with matching docids */
static void ne_filter(struct sindex *in, struct filter *f, MDB_txn *txn, struct bmap *docs) {
    eq_filter(in, f, txn, docs);
    if (!f->fr_bmap) {
        f->fr_bmap = bmap_duplicate(docs);
    } else {
        struct bmap *b = bmap_invert(f->fr_bmap, docs);
        bmap_free(f->fr_bmap);
        f->fr_bmap = b;
    }
}

/* or filter applies or on all child filters */
static void or_filter(struct sindex *si, struct filter *f, MDB_txn *txn, struct bmap *docs) {
    struct oper *bso = oper_new();
    for (int i = 0; i < kv_size(f->children); i++) {
        struct filter *c = kv_A(f->children, i);
        if (c->fr_bmap) {
            oper_add(bso, c->fr_bmap);
        }
    }
    f->fr_bmap = oper_or(bso);
    oper_free(bso);
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
    filter_callback[F_NE] = ne_filter;
    filter_callback[F_OR] = or_filter;
    filter_callback[F_ERROR] = error_filter;
}
