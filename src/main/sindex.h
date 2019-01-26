/* Shard index */
#ifndef __SINDEX_H_
#define __SINDEX_H_

#include <lmdb.h>
#include "shard.h"
#include "khash.h"
#include "kvec.h"
#include "dtrie.h"

KHASH_MAP_INIT_INT(FACETID2STR, char *) // Facet id to string mapping
KHASH_MAP_INIT_INT64(WID2MBMAP, struct mbmap *) // Word id to mbmap

// obj_data holds per object data to be written during indexing
struct obj_data {
    uint32_t oid; // Object id of object currently being indexed
    double *num_data; // Numeric data for a given object, an array of size num_numbers
    uint32_t twid[LEVLIMIT];
    kvec_t(uint32_t) *facet_data; // Facet ids for the object, an array of size num_facets
};

// Write cache, used to cache information during a bulk write
struct write_cache {

    // Various hashmaps maintaining current write operation data
    // for a batch of objects to be written
    khash_t(FACETID2STR) *kh_facetid2str;
    khash_t(WID2MBMAP) *kh_facetid2bmap; // Facet ID to obj id bmap
    khash_t(WID2MBMAP) *kh_boolid2bmap;   // Bool id to obj id bmap
    khash_t(WID2MBMAP) *kh_wid2bmap;        // Word id to obj id bmap
    khash_t(WID2MBMAP) *kh_twid2bmap;       // Top-Level word id to obj id bmap
    khash_t(WID2MBMAP) *kh_twid2widbmap;    // Top-level word id to wid bmap

    // Per object index info
    struct obj_data od;
};

/* sindex holds the search index for objects of a shard */
struct sindex {
    struct shard *shard;
    const struct mapping *map;

    // The trie holding all words
    struct dtrie *trie;

    // Write cache
    struct write_cache *wc;

    // LMDB
    MDB_env *env;
    MDB_txn *txn;

    MDB_dbi facetid2str_dbi;    // Facet_id to facet string mapping
    MDB_dbi facetid2bmap_dbi;   // Facet id to bitmap of objs containing that id
    MDB_dbi boolid2bmap_dbi;    // Bool id to bitmap of objs containing that bool
    MDB_dbi twid2widbmap_dbi;   // Top-level wid to bitmap of wids under the twid
    MDB_dbi twid2bmap_dbi;      // Top-level wid to bitmap of objids under the twid
    MDB_dbi wid2bmap_dbi;       // Word id to bitmap of objids containing that wid
    MDB_dbi num_dbi[MAX_FIELDS];
};

struct analyzer_data {
    struct sindex *si;
    int priority;
};

struct sindex *sindex_new(struct shard *s);
void sindex_add_objects(struct sindex *si, json_t *j);
void sindex_free(struct sindex *si);
void sindex_delete(struct sindex *si);
void sindex_clear(struct sindex *si);
void sindex_set_mapping(struct sindex *si, const struct mapping *map);

#endif

