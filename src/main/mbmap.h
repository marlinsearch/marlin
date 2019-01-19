#ifndef MBMAP_H
#define MBMAP_H

#include "bmap.h"
#include "lmdb.h"

struct mcont {
    uint16_t id;
    struct cont cont;
};

struct mbmap {
    uint64_t id; //(uint32_t id * 100000 + subid * 1000)
    struct mcont *c;
    uint16_t num_c;
    bool write_header;
};

struct mbmap *mbmap_new(uint64_t id);
void mbmap_add(struct mbmap *b, uint32_t item, MDB_txn *txn, MDB_dbi dbi);
void mbmap_remove(struct mbmap *b, uint32_t item, MDB_txn *txn, MDB_dbi dbi);
void mbmap_load(struct mbmap *b, MDB_txn *txn, MDB_dbi dbi);
bool mbmap_save(struct mbmap *b, MDB_txn *txn, MDB_dbi dbi);
struct bmap *mbmap_load_bmap(MDB_txn *txn, MDB_dbi dbi, uint64_t id);
uint32_t mbmap_get_cardinality(struct mbmap *b);
void mbmap_free(struct mbmap *b);

#endif

