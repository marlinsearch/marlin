#ifndef __SDATA_H_
#define __SDATA_H_

#include <lmdb.h>
#include "shard.h"

/* sdata holds the object data for a shard */
struct sdata {
    struct shard *shard;
    uint32_t lastoid; // Last oid
    bool custom_id;

    // LMDB
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi sid2json_dbi;   // Maps shard bitmap id to json data
    MDB_dbi id2sid_dbi;     // Maps the document id to shard bitmap id
};

struct sdata *sdata_new(struct shard *s);
void sdata_add_objects(struct sdata *sd, json_t *j);
void sdata_free(struct sdata *sd);
void sdata_delete(struct sdata *sd);
void sdata_clear(struct sdata *sd);

#endif
