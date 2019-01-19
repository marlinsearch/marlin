/* Shard index */
#ifndef __SINDEX_H_
#define __SINDEX_H_

#include <lmdb.h>
#include "shard.h"
#include "khash.h"
#include "kvec.h"

KHASH_MAP_INIT_INT(FACETID2STR, char *) // Facet id to string mapping
KHASH_MAP_INIT_INT64(FACETID2BMAP, struct mbmap *)

// obj_data holds per object data to be written during indexing
struct obj_data {
    uint32_t oid; // Object id of object currently being indexed
    double *num_data; // Numeric data for a given object, an array of size num_numbers
    kvec_t(uint32_t) *facet_data; // Facet ids for the object, an array of size num_facets
};

// Write cache, used to cache information during a bulk write
struct write_cache {

    // Various hashmaps maintaining current write operation data
    // for a batch of objects to be written
    khash_t(FACETID2STR) *kh_facetid2str;
    khash_t(FACETID2BMAP) *kh_facetid2bmap;

    // Per object index info
    struct obj_data od;
};

/* sindex holds the search index for objects of a shard */
struct sindex {
    struct shard *shard;
    const struct mapping *map;

    // Write cache
    struct write_cache *wc;

    // LMDB
    MDB_env *env;
    MDB_txn *txn;

    MDB_dbi facetid2str_dbi;    // Facet_id to facet string mapping
    MDB_dbi facetid2bmap_dbi;   // Facet id to bitmap of objs containing that id
    MDB_dbi num_dbi[MAX_FIELDS];
};

struct sindex *sindex_new(struct shard *s);
void sindex_add_objects(struct sindex *si, json_t *j);
void sindex_free(struct sindex *si);
void sindex_delete(struct sindex *si);
void sindex_clear(struct sindex *si);
void sindex_set_mapping(struct sindex *si, const struct mapping *map);

#endif

