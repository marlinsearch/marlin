/* Shard index */
#ifndef __SINDEX_H_
#define __SINDEX_H_

#include <lmdb.h>
#include "shard.h"
#include "khash.h"
#include "kvec.h"
#include "dtrie.h"
#include "squery.h"

KHASH_MAP_INIT_INT(FACETID2STR, char *) // Facet id to string mapping
KHASH_MAP_INIT_INT64(WID2MBMAP, struct mbmap *) // Word id to mbmap
KHASH_MAP_INIT_INT(UNIQWID, int) // A hash set to maintain unique wordids for an document

/* Holds mapping for a word with a given priority and its position */
typedef struct wid_pos {
    uint32_t wid;
    uint32_t position;
    int priority;
} wid_pos_t;

// doc_data holds per document data to be written during indexing
struct doc_data {
    uint32_t docid; // Document id of document currently being indexed
    double *num_data; // Numeric data for a given document, an array of size num_numbers
    uint32_t twid[LEVLIMIT];
    kvec_t(uint32_t) *facet_data;   // Facet ids for the document, an array of size num_facets
    kvec_t(wid_pos_t *) kv_widpos;  // Word positions of all words for this document
    khash_t(UNIQWID)   *kh_uniqwid; // Unique word ids for this document
};

// Write cache, used to cache information during a bulk write
struct write_cache {
    // Various hashmaps maintaining current write operation data
    // for a batch of documents to be written
    khash_t(FACETID2STR) *kh_facetid2str;
    khash_t(WID2MBMAP) *kh_facetid2bmap;    // Facet ID to docid bmap
    khash_t(WID2MBMAP) *kh_boolid2bmap;     // Bool id to docid bmap
    khash_t(WID2MBMAP) *kh_wid2bmap;        // Word id to docid bmap
    khash_t(WID2MBMAP) *kh_twid2bmap;       // Top-Level word id to docid bmap
    khash_t(WID2MBMAP) *kh_twid2widbmap;    // Top-level word id to wid bmap
    khash_t(WID2MBMAP) *kh_phrasebmap;      // Adjacent word id to docid bmap

    // Per document index info
    struct doc_data od;
};

/* sindex holds the search index for documents of a shard */
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
    MDB_dbi facetid2bmap_dbi;   // Facet id to bitmap of documents containing that id
    MDB_dbi boolid2bmap_dbi;    // Bool id to bitmap of documents containing that bool
    MDB_dbi twid2widbmap_dbi;   // Top-level wid to bitmap of wids under the twid
    MDB_dbi twid2bmap_dbi;      // Top-level wid to bitmap of docids under the twid
    MDB_dbi wid2bmap_dbi;       // Word id to bitmap of docids containing that wid
    MDB_dbi docid2fndata_dbi;     // docid to facet / numeric data 
    MDB_dbi docid2wpos_dbi;       // docid to word freq, position data
    MDB_dbi phrase_dbi;         // Phrase query dbi, adjacent wids mapped to docids containing the pair
    MDB_dbi num_dbi[MAX_FIELDS];
};

struct analyzer_data {
    struct sindex *si;
    int priority;
    uint32_t prev_wid;
};

typedef struct __attribute__((__packed__)) {
    uint32_t count;
    uint32_t data[0];
} facets_t;

typedef struct __attribute__((__packed__)) wid_info {
    uint32_t wid;
    // TODO: Make it union
    uint32_t is_position:1; // If the info holds position info instead of offset
    uint32_t priority:8;    // Field priority, only used when it is positional info
    uint32_t offset:23;     // Offset to frequency and positions or the actual position
} wid_info_t;

struct sindex *sindex_new(struct shard *s);
void sindex_add_documents(struct sindex *si, json_t *j);
void sindex_free(struct sindex *si);
void sindex_delete(struct sindex *si);
void sindex_clear(struct sindex *si);
void sindex_set_mapping(struct sindex *si, const struct mapping *map);

#endif

