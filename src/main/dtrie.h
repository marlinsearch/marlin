#ifndef __DTRIE_H
#define __DTRIE_H

#include <utf8proc.h>
#include <pthread.h>
#include "bmap.h"
#include "platform.h"
#include "word.h"
#include "khash.h"
#include <lmdb.h>

#define DT_MAGIC    0xBEDEADFE
#define DT_VERSION  0x00000001
#define PSIZE       4096
#define CHMAX       0xFFFFFFFF
#define LEVLIMIT    3
#define MAPSIZE     PSIZE * PSIZE * 100

KHASH_MAP_INIT_INT(WID2TYPOS, int);

typedef enum node_type{
    NS_DEFAULT = 1, // 16
    NS_32,          // 32
    NS_64,          // 64
    NS_128,         // 128
    NS_256,         // 256
    NS_512,         // 512
    NS_1K,          // 1024
    NS_4K,          // 4096
    NS_MAX
} NTYPE;

/* Currently just the offset from start of map 
 * uniquely identifies a node */
struct PACKED dnode_id {
    uint32_t offset;
};

struct PACKED dnode_ptr {
    chr_t c;
    struct dnode_id nid;
};

// Nodes may store word ids (wid), top-level word ids (twid)
struct PACKED dnode {
    uint32_t type:4;
    uint32_t has_wid:1;
    uint32_t has_twid:1;
    uint32_t num_child:26;
    struct dnode_ptr child[0];
};

/* First page stores dt_meta */
struct dt_meta {
    uint32_t magic;
    uint32_t version;
    uint32_t num_pages;
    uint32_t word_count;
    uint32_t tword_count;
    uint32_t root_offset;
    uint32_t used_nodes[NS_MAX*10]; // Extra space, if we want to support more node sizes
    uint32_t free_nodes[NS_MAX*10];
};

struct dtrie {
    int fd;
    uint8_t *map;
    char path[PATH_MAX];
    struct dt_meta *meta;
    struct dnode *root; // The root node
    struct bmap *freemaps;
    int freemap_dirty[NS_MAX];
    pthread_rwlock_t trie_lock;
    pthread_rwlockattr_t rwlockattr;
};

struct node_iter {
    struct dtrie *trie;
    struct dnode *node;
    struct dnode *parent;
    struct dnode_id nid;
    chr_t c;
};

typedef struct search_term {
    word_t *word;
    uint8_t prefix:1;
    uint8_t typos:1;
} term_t;

/**
 * Term result holds the result of looking up a given term
 * in dtrie for a given shard index
 */
typedef struct termresult {
    uint32_t twid;
    khash_t(WID2TYPOS) *wordids;
} termresult_t;

struct dtrie *dtrie_new(const char *path, MDB_dbi dbi, MDB_txn *txn);
uint32_t dtrie_insert(struct dtrie *dt, const chr_t *str, int slen, uint32_t *twids);
uint32_t dtrie_exists(struct dtrie *dt, const chr_t *str, int slen, uint32_t *twids);
void dump_dtrie_stats(struct dtrie *dt);
void dtrie_write_start(struct dtrie *dt);
// TODO: Get rid of the lmdb here.. this should be handled outside of dtrie
void dtrie_write_end(struct dtrie *dt, MDB_dbi dbi, MDB_txn *txn);
void dtrie_free(struct dtrie *dt);
struct termresult *dtrie_lookup_term(struct dtrie *dt, term_t *t);

#endif

