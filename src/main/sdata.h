#ifndef __SDATA_H_
#define __SDATA_H_

#include <lmdb.h>
#include "shard.h"

/* sdata holds the object data for a shard */
struct sdata {
    struct shard *shard;
    uint32_t lastoid; // Last oid

    // LMDB
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi jdbi; // The object json datastore

};

struct sdata *sdata_new(struct shard *s);
void sdata_add_objects(struct sdata *sd, json_t *j);

#endif
