/* Disk Trie 
 * This is a textbook trie implementation without any optimizations.  This 
 * will most probably be replaced by something better, but it is good enough to 
 * get started.  The trie itself contains multiple nodes of 1 char each.  
 * */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include "platform.h"
#include "dtrie.h"
#include "mlog.h"
#include "common.h"

/* Size of node for a given type */
static const int node_size[NS_MAX] = {0, 16,  32,  64, 128, 256, 512, 1024, 4096};
/* Number of nodes of a given type which will fit in a page */
static const int node_nums[NS_MAX] = {0, 256, 128, 64, 32,  16,  8,   4,    1};


static inline uint32_t *get_widpos(struct dnode *n) {
    uint8_t *pos = (uint8_t *)n;
    pos += (sizeof(struct dnode) + (n->num_child * sizeof(struct dnode_ptr)));
    return (uint32_t *)pos;
}

static inline void set_wid(struct dnode *n, uint32_t wid) {
    if (n->has_wid) {
        uint32_t *pos = get_widpos(n);
        *pos = wid;
        return;
    }
    if (n->has_twid) {
        n->has_wid = 1;
        uint32_t *pos = get_widpos(n);
        uint32_t twid = *pos;
        *pos = wid;
        pos++;
        *pos = twid;
        return;
    }
    n->has_wid = 1;
    uint32_t *pos = get_widpos(n);
    *pos = wid;
}

static inline void set_twid(struct dnode *n, uint32_t twid) {
    n->has_twid = 1;
    uint32_t *pos = get_widpos(n);
    if (n->has_wid) {
		pos++;
    }
    *pos = twid;
}

static inline uint32_t get_wid(struct dnode *n) {
    if (n->has_wid) {
        uint32_t *pos = get_widpos(n);
        return *pos;
	}
	return 0;
}

static inline uint32_t get_twid(struct dnode *n) {
    if (n->has_twid) {
        uint32_t *pos = get_widpos(n);
        if (n->has_wid) {
            pos++;
        }
        return *pos;
    }
	return 0;
}

static inline void dump_node(struct dnode *n) {
    printf("\nNode %p type %d numchild %d wid %d twid %d\n", n, n->type, n->num_child, n->has_wid, n->has_twid);
    struct dnode_ptr *np = n->child;
    for (int i=0; i<n->num_child; i++) {
        struct dnode_ptr *tnp = &np[i];
        printf("Chr %c offset %u\n", (unsigned char)tnp->c, tnp->nid.offset);
    }
    if (n->has_wid) {
        printf("wid : %d ", get_wid(n));
    }
    if (n->has_twid) {
        printf("twid : %d ", get_twid(n));
    }
    printf("\n");
}


static void init_node(struct dtrie *dt, uint8_t *ptr, NTYPE type) {
    if (LIKELY(type < NS_MAX)) {
        memset(ptr, 0, node_size[type]);
    }
    struct dnode *n = (struct dnode *)ptr;
    n->type = type;
    dt->meta->used_nodes[type]++;
    dt->meta->free_nodes[type]--;
}

static inline struct dnode *get_node_from_nodeid(struct dtrie *dt, struct dnode_id *nid) {
    uint8_t *nptr = dt->map + nid->offset;
    return (struct dnode *) nptr;
}

static struct dnode *create_new_node(struct dtrie *dt, NTYPE type, struct dnode_id *nid) {
    if (LIKELY(type < NS_MAX)) {
        // Use a new page and increment used page count
        nid->offset = (PSIZE * dt->meta->num_pages);
        dt->meta->num_pages++;
        // Initialize the page
        // TODO: Is this the way to do this?
        lseek(dt->fd, (PSIZE*(dt->meta->num_pages+1))-1, SEEK_SET);
        if (write(dt->fd, "", 1) < 0) {
            printf("!!!!!!!!!!!!!!!!!!!!! Failed to init node\n");
            return NULL;
        }
        dt->meta->free_nodes[type] = node_nums[type];

        // Add every child page other than first to the free bmap for that type
        // we know first is going to bem used right away
        uint32_t offset = nid->offset;
        for (int i = 1; i < node_nums[type]; i++) {
            offset += node_size[type];
            bmap_add(&dt->freemaps[type], offset);
        }
        dt->freemap_dirty[type] = 1;

    } else {
        int ns = type - NS_MAX;
        printf("*************** Creating big node of size %d\n", ns);
        nid->offset = (PSIZE * dt->meta->num_pages);
        dt->meta->num_pages += ns;
        lseek(dt->fd, (PSIZE*(dt->meta->num_pages+1))-1, SEEK_SET);
        if (write(dt->fd, "", 1) < 0) {
            printf("!!!!! Failed to init page\n");
            return NULL;
        }
    }

    // Reset offset to top child page
    uint8_t *nptr = dt->map;
    // Point to the last page that we just added
    nptr += nid->offset;
    // The size to initialize is either the entire page or large node size
    memset(nptr, 0, type < NS_MAX ? PSIZE : PSIZE * (type-NS_MAX));
    init_node(dt, nptr, type);
    // Add child pages to free list
    return (struct dnode *)nptr;

}


/* Gets a free node for a given type.  If first looks at the freemap for the type
 * and uses free nodes if any.  Else setups a page of nodes and returns the first 
 * node */
static struct dnode *get_free_node(struct dtrie *dt, NTYPE type, struct dnode_id *nid) {
    // Check if there are any free pages of this type
    // if so use it.  
    if (LIKELY(type < NS_MAX)) {
        uint32_t foffset = bmap_get_first(&dt->freemaps[type]);
        // We have a free page
        if (foffset != 0xFFFFFFFF) {
            nid->offset = foffset;
            // Remove the id from freemap
            bmap_remove(&dt->freemaps[type], foffset);
            dt->freemap_dirty[type] = 1;
            uint8_t *nptr = (uint8_t *)get_node_from_nodeid(dt, nid);
            init_node(dt, nptr, type);
            return (struct dnode *)nptr;
        }
    }

    // If nothing exists add a new page
    struct dnode *n = create_new_node(dt, type, nid);
    return n;
}

static int binary_search(struct dnode *n, struct dnode_ptr *np, const chr_t c) {
    int low = 0, high = n->num_child-1, middle;
    while (low <= high) {
        middle = (low+high)/2;
        if (np[middle].c < c) {
            low = middle+1;
        } else if (np[middle].c > c){
            high = middle-1;
        } else {
            return middle;
        }
    }
    return -(low+1);
}

static inline struct dnode_id *binary_search_nodeid(struct dnode *n, chr_t c) {
    struct dnode_ptr *np = n->child;
    int pos = binary_search(n, np, c);
    if (pos >= 0) {
        return &np[pos].nid;
    }
    return NULL;
}

static struct dnode *node_get_child(struct dtrie *dt, struct dnode *n, chr_t c, struct dnode_id *cnid) {
    struct dnode_id *nid = binary_search_nodeid(n, c);
    if (nid) {
        *cnid = *nid;
        return get_node_from_nodeid(dt, nid);
    }
    return NULL;
}

/* Gets the size of a node. Some nodes may have a twid or wid, handles that */ 
static inline int get_node_size(struct dnode *n) {
    int s =  sizeof(struct dnode) +  (n->num_child * sizeof(struct dnode_ptr));
    if (n->has_wid) {
        s += sizeof(uint32_t);
    }
    if (n->has_twid) {
        s += sizeof(uint32_t);
    }
    return s;
}

// Can a child ptr be added to this node?
static inline bool can_child_be_added(struct dnode *n) {
    int size = get_node_size(n) + sizeof(struct dnode_ptr);
    if (LIKELY(n->type < NS_MAX)) {
        return size <= node_size[n->type];
    } else {
        return size <= (PSIZE * (n->type - NS_MAX));
    }
}

static inline int node_wid_size(struct dnode *n) {
    int s = 0;
    if (n->has_wid) {
        s += sizeof(uint32_t);
    }
    if (n->has_twid) {
        s += sizeof(uint32_t);
    }
    return s;
}

// Given a node, insert a node_ptr to point to a child node
static void insert_nodeid_in_node(struct dnode *n, struct dnode_id *nid, chr_t c) {
    // Binary search and put chr_t in the correct place together with its nid.
    // Increase numchild ++
    struct dnode_ptr *np = n->child;
    int pos = binary_search(n, np, c);
    // It should never exist in case of a store pid in page!
    if (pos >= 0) {
        // TODO: proper warning !
        printf("\n\n\nSOMETHING IS WRONG during insert %u!! \n\n\n", c);
        //dump_page(p);
        exit(1);
        return;
    }
    pos = -pos-1;
    n->num_child++;
    if (n->num_child-1 > pos) {
        memmove(np+pos+1, np+pos, ((n->num_child-1-pos)*sizeof(struct dnode_ptr)) + node_wid_size(n));
    } else {
        memmove(np+pos+1, np+pos, node_wid_size(n));
    }
    struct dnode_ptr *tnp = &np[pos];
    tnp->c = c;
    tnp->nid.offset = nid->offset;
}

// Replace the node in a node for char c with nid
static void replace_nodeid_in_node(struct dnode *n, struct dnode_id *nid, chr_t c) {
    // Binary search and put chr in the correct place.
    struct dnode_ptr *np = n->child;
    int pos = binary_search(n, np, c);
    // It should always exist in case of a replace pid in page!
    if (pos < 0) {
        // TODO: proper warning !
        printf("\n\n\nSOMETHING IS WRONG during replace !! \n\n\n");
        return;
    }
    struct dnode_ptr *tnp = &np[pos];
    tnp->c = c;
    tnp->nid.offset = nid->offset;
}

static void upsize_node(struct node_iter *iter) {
    struct dnode *n = iter->node;
    // Nope wont fit, copy the data into a bigger page
    NTYPE type = n->type + 1;
    if (type == NS_MAX) {
        type += 2;
        printf("!!! New type is %d\n", type);
    }
    struct dnode_id new_node_id;
    struct dnode *new_node = get_free_node(iter->trie, type, &new_node_id);
    int size = get_node_size(n);
    uint32_t offset = iter->nid.offset;
    // Copy node
    memcpy(new_node, n, size);
    // n is no longer in use, just new_node
    // Add current node to free list, we don't use it anymore
    // Do not bother about mega nodes, they are very very rare
    if (LIKELY(type < NS_MAX)) {
        bmap_add(&iter->trie->freemaps[n->type], offset);
        iter->trie->freemap_dirty[n->type] = 1;
        iter->trie->meta->free_nodes[n->type]++;
        iter->trie->meta->used_nodes[n->type]--;
    }

    // Type alone is different
    new_node->type = type;
    // Now set parent to point to the new node
    if (LIKELY(iter->parent)) {
        replace_nodeid_in_node(iter->parent, &new_node_id, iter->c);
    } else {
        iter->trie->meta->root_offset = new_node_id.offset;
        iter->trie->root = new_node;
        printf("\n ******************** Root changed %u %d *********************!!\n", new_node_id.offset, new_node->type);
    }
    iter->node = new_node;
}

// This changes node size or splits nodes or adds new nodes.
static void add_nodeid_under_node(struct node_iter *iter, const chr_t c, struct dnode_id *nid) {
    struct dnode *n = iter->node;

    if (can_child_be_added(n)) {
        return insert_nodeid_in_node(n, nid, c);
    }

    // It will not fit, let us upsize it
    upsize_node(iter);

    return insert_nodeid_in_node(iter->node, nid, c);
}


static void node_add_child(struct node_iter *iter, chr_t c) {
    struct dnode_id nid;
    struct dnode *n = get_free_node(iter->trie, NS_DEFAULT, &nid);
    // Adds the chr_t c under 
    add_nodeid_under_node(iter, c, &nid);
    iter->parent = iter->node;
    iter->node = n;
    iter->nid = nid;
}

/* Adds all nodes to make the word @str.  Nodes are added only if they do not exist already */
static struct dnode *add_path_nodes(struct node_iter *iter, const chr_t *str, int slen, uint32_t *twids) {
    // Take one chr_t at a time
    for (int i=0; i<slen; i++) {
        struct dnode_id cnid;
        // Try to find the node for the chr_t
        struct dnode *next = node_get_child(iter->trie, iter->node, str[i], &cnid);
        // The node may already exist, we just update node_iter to continue traversal
        if (next) {
            iter->parent = iter->node;
            iter->node = next;
            iter->nid = cnid;
            iter->c = str[i];
        } else {
            // The node does not exist, let us add it, this updates the node_iter 
            // to continue traversal
            node_add_child(iter, str[i]);
            iter->c = str[i];
            if (i < LEVLIMIT) {
                set_twid(iter->node, ++iter->trie->meta->tword_count);
            }
        }
        if (i < LEVLIMIT) {
            twids[i] = get_twid(iter->node);
        }
    }
    return iter->node;
}

void dump_dtrie_stats(struct dtrie *dt) {
    printf("Num pages : %u Num words : %u Gwords : %u\n", dt->meta->num_pages, dt->meta->word_count, dt->meta->tword_count);
    printf("\nUsed pages : \n");
    for (int i=0; i<NS_MAX; i++) {
        printf("Type %d Count %u\n", i, dt->meta->used_nodes[i]);
    }
    printf("\nFree pages : \n");
    for (int i=0; i<NS_MAX; i++) {
        printf("Type %d Count %u\n", i, dt->meta->free_nodes[i]);
    }
    printf("Size of node %lu ptr %lu\n", sizeof(struct dnode), sizeof(struct dnode_ptr));
}

static struct dnode *lookup_word_node(struct dtrie *dt, const chr_t *str, int slen) {
    struct dnode *current = dt->root;
    struct dnode_id nid;
    for (int i=0; i<slen; i++) {
        struct dnode *next = node_get_child(dt, current, str[i], &nid);
        if (next) {
            current = next;
        } else {
            return NULL;
        }
    }
    return current;
}

static uint32_t word_exists_under_root(struct dtrie *dt, const chr_t *str, int slen, uint32_t *twids) {
    struct dnode *current = dt->root;
    struct dnode_id nid;
    for (int i=0; i<slen; i++) {
        struct dnode *next = node_get_child(dt, current, str[i], &nid);
        if (next) {
            current = next;
        } else {
            return 0;
        }
        if (i < LEVLIMIT) {
            twids[i] = get_twid(current);
        }
    }
    return get_wid(current);
}

uint32_t dtrie_exists(struct dtrie *dt, const chr_t *str, int slen, uint32_t *twids) {
    RDLOCK(&dt->trie_lock);
    uint32_t wid = word_exists_under_root(dt, str, slen, twids);
    UNLOCK(&dt->trie_lock);
    return wid;
}

// Can wid be set without going over size limits
static inline bool can_add_wid_for_node(struct dnode *n) {
    int size = get_node_size(n) + sizeof(uint32_t);
    if (LIKELY(n->type < NS_MAX)) {
        return (size <= node_size[n->type]);
    } else {
        return (size <= (PSIZE * (n->type - NS_MAX)));
    }
}


/* Inserts a word into the trie.  If word already exists, just returns the word id for the
 * existing word entry.  The first 3 top level wordids are set in twids */
uint32_t dtrie_insert(struct dtrie *dt, const chr_t *str, int slen, uint32_t *twids) {
    // First grab a write lock
    WRLOCK(&dt->trie_lock);

    /* node_iter is used while walking through a trie to store parent
     * node information */
    struct node_iter iter;
    iter.trie = dt;
    iter.node = dt->root;
    iter.parent = NULL;
    iter.nid.offset = dt->meta->root_offset;
    uint32_t wid = 0;

    /* Add all path nodes required to satisfy this word.  For eg., for the
     * word 'best' we end up adding 4 nodes b, e, s, t and return the node 
     * for the final chr 't'.  Some or all of the nodes may already exist */
    struct dnode *node = add_path_nodes(&iter, str, slen, twids);
    if (node->has_wid) {
        wid = get_wid(node);
    } else {
        // Let us see if setting wid is ok for this node
        if (!can_add_wid_for_node(node)) {
            // If we cannot, let us upsize it
            upsize_node(&iter);
            node = iter.node;
        }
        wid =  ++dt->meta->word_count;
        set_wid(node, wid);
    }
    UNLOCK(&dt->trie_lock);

    return wid;
}

void dtrie_write_start(struct dtrie *dt) {
    memset(&dt->freemap_dirty[0], 0, sizeof(dt->freemap_dirty));
}

static void store_freemap(struct bmap *b, uint64_t fid, MDB_dbi dbi, MDB_txn *txn) {
    // First find total length
    uint32_t mlen = bmap_get_dumplen(b);
    MDB_val key, data;
    key.mv_size = sizeof(uint64_t);
    key.mv_data = &fid;
    data.mv_size = mlen;
    int rc = mdb_put(txn, dbi, &key, &data, MDB_RESERVE);
    if (rc != 0) {
        M_ERR("MDB reserve failure freemap %d %u", rc, mlen);
        return;
    }
    uint16_t *buf = data.mv_data;
    bmap_dump(b, buf);
}

static void dtrie_load_freemaps(struct dtrie *dt, MDB_dbi dbi, MDB_txn *txn) {
    for (int i = 0; i < NS_MAX; i++) {
        uint64_t fid = IDPRIORITY(0xFFFFFFFF, i);
        MDB_val key, data;
        key.mv_size = sizeof(uint64_t);
        key.mv_data = &fid;
        int rc = mdb_get(txn, dbi, &key, &data);
        if (rc == 0) {
            uint16_t *buf = data.mv_data;
            bmap_load(&dt->freemaps[i], buf);
        }
    }
}

void dtrie_write_end(struct dtrie *dt, MDB_dbi dbi, MDB_txn *txn) {
    for (int i = 0; i < NS_MAX; i++) {
        if (dt->freemap_dirty[i]) {
            uint64_t fid = IDPRIORITY(0xFFFFFFFF, i);
            store_freemap(&dt->freemaps[i], fid, dbi, txn);
        }
    }
}

void dtrie_free(struct dtrie *dt) {
    WRLOCK(&dt->trie_lock);
    for (int i=1; i<NS_MAX; i++) {
        bmap_free_containers(&dt->freemaps[i]);
    }
    free(dt->freemaps);
    if (dt->map) {
        munmap(dt->map, MAPSIZE);
    }
    if (dt->fd >= 0) {
        close(dt->fd);
    }
    UNLOCK(&dt->trie_lock);
    DESTROYLOCK(&dt->trie_lock);
    free(dt);
}

/* Creates a new dtrie or loads an existing dtrie on path */
struct dtrie *dtrie_new(const char *path, MDB_dbi dbi, MDB_txn *txn) {

    struct dtrie *dt = NULL;
    int fd = open(path, O_RDWR | O_CREAT, (mode_t)0600);
    if (fd < 0) {
        goto file_error;
    }

    int size = lseek(fd, 0, SEEK_END);
    if (size < 0) {
        goto file_error;
    }

    if (size < (PSIZE * (NS_MAX + 2))) {
        if (ftruncate(fd, (PSIZE * (NS_MAX + 2))) != 0) {
            goto file_error;
        }
    }

    void *map = mmap(NULL, MAPSIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        goto file_error;
    }

    // Successfully opened and synced the file
    dt = calloc(1, sizeof(struct dtrie));

    pthread_rwlockattr_init(&dt->rwlockattr);
#ifdef __linux__
    pthread_rwlockattr_setkind_np(&dt->rwlockattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
    pthread_rwlock_init(&dt->trie_lock, &dt->rwlockattr);

    strcpy(dt->path, path);
    dt->fd = fd;
    dt->map = map;
    /* The default root is after meta page and free maps for all node sizes */
    dt->root = (struct dnode *)(dt->map + (PSIZE * (NS_MAX + 1)));
    struct dt_meta *meta = map;
    dt->meta = meta;
    /* If magic is present, it is an existing dtrie, if not setup the trie header */
    if (meta->magic != DT_MAGIC) {
        // First time the file has been created.
        memset(dt->map, 0, PSIZE);
        meta->magic = DT_MAGIC;
        meta->version = DT_VERSION;
        meta->num_pages = (NS_MAX + 2);
        meta->word_count = 0;
        meta->tword_count = 0;
        meta->root_offset = (PSIZE * (NS_MAX + 1));
        // Init root node
        dt->meta->free_nodes[NS_4K] = 1;
        for (int i = 1; i <= NS_MAX; i++) {
            uint8_t *f = dt->map + (PSIZE * i);
            memset(f, 0, PSIZE);
        }
        init_node(dt, dt->map + (PSIZE * (NS_MAX + 1)), NS_4K);
    } else {
        // Already exists, just point root to root_offset
        dt->root = (struct dnode *)(dt->map + dt->meta->root_offset);
    }
    dt->freemaps = calloc(NS_MAX, sizeof(struct bmap));
    // Load freemaps
    dtrie_load_freemaps(dt, dbi, txn);

    return dt;

file_error:
    if (fd >= 0) {
        close(fd);
    }
    printf("Failed to initialize dtrie !\n");
    return NULL;
}

/* Adds / Updates the result with a wid -> distance mapping */
static void add_wordid_to_result(termresult_t *tr, uint32_t wid, int distance) {
    khiter_t k;
    // Set a high default value, update with a better distance if we arrive at one
    int d = kh_get_val(WID2TYPOS, tr->wordids, wid, 0xFF);
    if (distance < d) {
        kh_set(WID2TYPOS, tr->wordids, wid, distance);
    }
}

static void node_walk(struct dtrie *dt, struct dnode *d, termresult_t *tr, int distance) {
    // See if we have a twid and use that if it exists
    tr->twid = get_twid(d);
    if (!tr->twid) {
        // Add wid if any in the current node
        uint32_t wid = get_wid(d);
        if (wid) {
            add_wordid_to_result(tr, wid, distance);
        }
        for (int i = 0; i < d->num_child; i++) {
            struct dnode *dc = get_node_from_nodeid(dt, &d->child[i].nid);
            node_walk(dt, dc, tr, distance);
        }
    }
}

/* Looksup toplevel word id, only to be used when word len is <= LEVLIMIT
 * If it is a prefix match, we set the twid. The words containing this twid
 * can be retrieve by the caller.  If it is not a prefix match, 
 * we add wid if we find any */
static void lookup_twid(struct dtrie *dt, term_t *t, termresult_t *tr) {
    RDLOCK(&dt->trie_lock);
    // First lookup the node for this word
    struct dnode *d = lookup_word_node(dt, t->word->chars, t->word->length);
    if (d) {
        if (t->prefix) {
            // For a prefix search, just set twid
            tr->twid = get_twid(d);
        } else {
            // It is not a prefix search, do not set the twid
            // instead add wid if any in the node
            uint32_t wid = get_wid(d);
            if (wid) {
                add_wordid_to_result(tr, wid, 0);
            }
        }
    }
    UNLOCK(&dt->trie_lock);
}

static void lookup_notypo(struct dtrie *dt, term_t *t, termresult_t *tr) {
    RDLOCK(&dt->trie_lock);
    struct dnode *d = lookup_word_node(dt, t->word->chars, t->word->length);
    if (d) {
        if (t->prefix) {
            node_walk(dt, d, tr, 0);
        } else {
            // It is not a prefix search, do not set the twid
            // instead add wid if any in the node
            uint32_t wid = get_wid(d);
            if (wid) {
                add_wordid_to_result(tr, wid, 0);
            }
        }
    }
    UNLOCK(&dt->trie_lock);
}

struct lev_data {
    struct dtrie *dt;
    word_t *word;
    term_t *t;
    termresult_t *tr;
    int maxdist;
};

static void node_lev(struct lev_data *ld, struct dnode *d, int *prev_row, int *pprev_row, 
                            chr_t c, chr_t pc, int depth) {

    int size = ld->word->length + 1;
    int *current_row = prev_row + size;
    const chr_t *str = ld->word->chars;

    current_row[0] = prev_row[0] + 1;
    memset(&current_row[1], 0x0F, (size-1)*sizeof(unsigned int));
    int lb = MAX((depth - ld->maxdist), 1);
    int rb = MIN((depth + ld->maxdist + 1), size);
    for (int i=lb; i<rb; i++) {
        int ins_del = MIN(current_row[i-1]+1, prev_row[i]+1);
        int repl = (str[i-1] == c)?prev_row[i-1]:(prev_row[i-1]+1);
        current_row[i] = MIN(ins_del, repl);
        // Damareu
        if (i > 1 && depth > 1) {
            if (str[i-1] == pc && str[i-2] == c) {
                current_row[i] = MIN(current_row[i], str[i-1]==c? pprev_row[i-2]: pprev_row[i-2]+1);
            }
        }
    }
    uint8_t dist = current_row[size-1];
    if (dist <= ld->maxdist) {
        if (ld->t->prefix) {
            if (depth >= (size-1)) {
                node_walk(ld->dt, d, ld->tr, dist);
                return;
            } else {
                if (get_wid(d)) {
                    node_walk(ld->dt, d, ld->tr, dist);
                }
            }
        } else  {
            uint32_t wid = get_wid(d);
            if (wid) {
                add_wordid_to_result(ld->tr, wid, dist);
            }
        }
    }

    // If we have reached this far, let us check the children too
    for (int i = 0; i < size; i++) {
        if (current_row[i] <= ld->maxdist) {
            for (int i = 0; i < d->num_child; i++) {
                struct dnode*nd = get_node_from_nodeid(ld->dt, &d->child[i].nid);
                node_lev(ld, nd, current_row, prev_row, d->child[i].c, c, depth+1);
            }
            break;
        }
    }
}


static void lookup_typo(struct dtrie *dt, term_t *t, termresult_t *tr) {
    RDLOCK(&dt->trie_lock);
    // Set the max distance based on word length
    // TODO: Make word distance configurable and send during lookup request
    int wlen = t->word->length;
    int maxdist = (wlen > 7)?2:1;
    // Do a Dam-Levenshtein lookup
    struct lev_data ld;
    ld.maxdist = maxdist;
    ld.dt = dt;
    ld.word = t->word;
    ld.tr = tr;
    ld.t = t;
    int *current_row = malloc(((wlen + 1) * (wlen + 2) * 2) * sizeof(int));

    for (int i = 0; i <= wlen; i++) {
        current_row[i] = i;
    }

    struct dnode *root = dt->root;
    for (int i = 0; i < root->num_child; i++) {
        struct dnode *d = get_node_from_nodeid(dt, &root->child[i].nid);
        node_lev(&ld, d, current_row, NULL, root->child[i].c, 0, 1);
    }

    free(current_row);
    UNLOCK(&dt->trie_lock);
}

struct termresult *dtrie_lookup_term(struct dtrie *dt, term_t *t) {
    struct termresult *tr = calloc(1, sizeof(struct termresult));
    tr->wordids = kh_init(WID2TYPOS);

    word_t *w = t->word;
    if (w->length <= LEVLIMIT) {
        lookup_twid(dt, t, tr);
        return tr;
    }

    // If typos are not allowed, do a notypo lookup
    if (!t->typos) {
        lookup_notypo(dt, t, tr);
        return tr;
    }

    lookup_typo(dt, t, tr);
    return tr;
}

void termresult_free(struct termresult *t) {
    if (t) {
        kh_destroy(WID2TYPOS, t->wordids);
        free(t);
    }
}
