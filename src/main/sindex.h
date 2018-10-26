#ifndef __SINDEX_H_
#define __SINDEX_H_
#include <lmdb.h>
#include "shard.h"

/* sindex holds the search index for objects of a shard */
struct sindex {
    struct shard *shard;
    const struct mapping *map;

    // LMDB
    MDB_env *env;
    MDB_txn *txn;
};

struct sindex *sindex_new(struct shard *s);
void sindex_add_objects(struct sindex *si, json_t *j);
void sindex_free(struct sindex *si);
void sindex_delete(struct sindex *si);
void sindex_clear(struct sindex *si);

#endif

