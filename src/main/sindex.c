/* Shard Index - This is the search engine index manager, which takes care of 
 * parsing objects and building an index which lets us query these objects */
#include "sindex.h"
#include "marlin.h"
#include "common.h"
#include "dbi.h"
#include "farmhash-c.h"
#include "mbmap.h"
#include "analyzer.h"
#include "ksort.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

#define wid2bmap_add(dbi, kh, txn, keyid, vid, priority) {  \
    uint64_t hid = IDPRIORITY(keyid, priority);             \
    struct mbmap *map;                                      \
    khiter_t k = kh_get(WID2MBMAP, kh, hid);                \
    if (LIKELY(k != kh_end(kh))) {                          \
        map = kh_val(kh, k);                              \
    } else {                                                \
        map = mbmap_new(hid);                            \
        mbmap_load(map, txn, dbi);     \
        int ret = 0;                                        \
        k = kh_put(WID2MBMAP, kh, hid, &ret);              \
        kh_value(kh, k) = map;                              \
    }                                                       \
    mbmap_add(map, vid, txn, dbi);                          \
}

#define wid2bmap_remove(dbi, kh, txn, keyid, vid, priority) {  \
    uint64_t hid = IDPRIORITY(keyid, priority);             \
    struct mbmap *map;                                      \
    khiter_t k = kh_get(WID2MBMAP, kh, hid);                \
    if (LIKELY(k != kh_end(kh))) {                          \
        map = kh_val(kh, k);                              \
    } else {                                                \
        map = mbmap_new(hid);                            \
        mbmap_load(map, txn, dbi);     \
        int ret = 0;                                        \
        k = kh_put(WID2MBMAP, kh, hid, &ret);              \
        kh_value(kh, k) = map;                              \
    }                                                       \
    mbmap_remove(map, vid, txn, dbi);                       \
}


typedef struct {
    wid_pos_t *wp;
} wp_hold_t;

static inline bool compare_wpos(wp_hold_t a, wp_hold_t b) {
    if (a.wp->wid == b.wp->wid) {
        if (a.wp->priority == b.wp->priority) {
            return a.wp->position < b.wp->position;
        } else return a.wp->priority < b.wp->priority;
    } else return a.wp->wid < b.wp->wid;
}

KSORT_INIT(compwordpos, wp_hold_t, compare_wpos)

static int double_compare(const MDB_val *a, const MDB_val *b) {
    return (*(double *)b->mv_data < *(double *)a->mv_data) ? -1 :
        *(double *)b->mv_data > *(double *)a->mv_data;
}

static void si_write_start(struct sindex *si) {
    // Prepares the write cache
    si->wc = calloc(1, sizeof(struct write_cache));

    // Setup all khashes
    si->wc->kh_facetid2str = kh_init(FACETID2STR);
    si->wc->kh_boolid2bmap = kh_init(WID2MBMAP);
    si->wc->kh_facetid2bmap = kh_init(WID2MBMAP);
    si->wc->kh_wid2bmap = kh_init(WID2MBMAP);
    si->wc->kh_twid2bmap = kh_init(WID2MBMAP);
    si->wc->kh_twid2widbmap = kh_init(WID2MBMAP);
    si->wc->kh_phrasebmap = kh_init(WID2MBMAP);
 
    // Setup per obj data
    struct obj_data *od = &si->wc->od;

    // Setup num_data to store numeric information for an object
    // TODO: change it to vec like facets below
    if (si->map->num_numbers) {
        od->num_data = malloc(sizeof(double) * si->map->num_numbers);
    }

    if (si->map->num_facets) {
        od->facet_data = calloc(si->map->num_facets, sizeof(kvec_t(uint32_t)));
    }

    dtrie_write_start(si->trie);
    // Begin the write transaction
    mdb_txn_begin(si->env, NULL, 0, &si->txn);
}

/* Stores a 64bit id to bitmap mapping in lmdb */
static inline void store_id2mbmap(struct mbmap *b, MDB_dbi dbi, MDB_txn *txn) {
    mbmap_save(b, txn, dbi);
    mbmap_free(b);
}

static void store_facetid2str(struct sindex *si, uint32_t id, char *str) {
    MDB_val key, data;
    data.mv_size = strlen(str)+1;
    data.mv_data = (void *)str;

    key.mv_size = sizeof(uint32_t);
    key.mv_data = &id;
    int rc = mdb_put(si->txn, si->facetid2str_dbi, &key, &data, 0);
    if (rc != 0) {
        M_ERR("MDB put failure facetid2str %d", rc);
        M_ERR("Len %u str %s", strlen(str), str);
    } 
}


static void si_write_end(struct sindex *si) {

    struct mbmap *mbmap;

    // Store bool id to bmap mapping
    kh_foreach_value(si->wc->kh_boolid2bmap, mbmap, {
        store_id2mbmap(mbmap, si->boolid2bmap_dbi, si->txn);
    });
    kh_destroy(WID2MBMAP, si->wc->kh_boolid2bmap);

    // Store facet id to bmap mapping
    kh_foreach_value(si->wc->kh_facetid2bmap, mbmap, {
        store_id2mbmap(mbmap, si->facetid2bmap_dbi, si->txn);
    });
    kh_destroy(WID2MBMAP, si->wc->kh_facetid2bmap);

    // Store twid to wid mapping
    kh_foreach_value(si->wc->kh_twid2widbmap, mbmap, {
        store_id2mbmap(mbmap, si->twid2widbmap_dbi, si->txn);
    });
    kh_destroy(WID2MBMAP, si->wc->kh_twid2widbmap);

    // Store twid to objid mapping
    kh_foreach_value(si->wc->kh_twid2bmap, mbmap, {
        store_id2mbmap(mbmap, si->twid2bmap_dbi, si->txn);
    });
    kh_destroy(WID2MBMAP, si->wc->kh_twid2bmap);

    // Store wid to objid mapping
    kh_foreach_value(si->wc->kh_wid2bmap, mbmap, {
        store_id2mbmap(mbmap, si->wid2bmap_dbi, si->txn);
    });
    kh_destroy(WID2MBMAP, si->wc->kh_wid2bmap);
    // Store phrase to objid mapping
    kh_foreach_value(si->wc->kh_phrasebmap, mbmap, {
        store_id2mbmap(mbmap, si->phrase_dbi, si->txn);
    });
    kh_destroy(WID2MBMAP, si->wc->kh_phrasebmap);


    // Store facet id to string mappings
    uint32_t facet_id;
    char *str;
    kh_foreach(si->wc->kh_facetid2str, facet_id, str, {
        // Only store if required, we may have null values
        if (str) {
            store_facetid2str(si, facet_id, str);
            free(str);
        }
    });
    kh_destroy(FACETID2STR, si->wc->kh_facetid2str);

    dtrie_write_end(si->trie, si->boolid2bmap_dbi, si->txn);
    // Commit write transaction
    mdb_txn_commit(si->txn);

    // Free common object data
    free(si->wc->od.num_data);
    free(si->wc->od.facet_data);
    // Free write cache finally
    free(si->wc);
}

/* Intializes per object data which is in the write cache */
static inline void sindex_objdata_init(struct sindex *si) {
    struct obj_data *od = &si->wc->od;
    memset(od->num_data, 0, (si->map->num_numbers*sizeof(double)));

    kv_init(od->kv_widpos);
    od->kh_uniqwid = kh_init(UNIQWID);

    for (int i = 0; i<si->map->num_facets; i++) {
        kv_init(od->facet_data[i]);
    }
}

/* Stores the per object numeric and facet data.
 * This is split from word position information as this will
 * mostly be used during large scale aggregations
 */
static inline void store_facet_num_data(struct sindex *si, struct obj_data *od) {
    // Calculate storage size required to store the data
    // Start with size required to store numeric data
    size_t size = si->map->num_numbers * sizeof(double);
    // Add the facet data size
    size += (si->map->num_facets * sizeof(uint32_t));
    for (int i = 0; i < si->map->num_facets; i++) {
        size += (kv_size(od->facet_data[i]) * sizeof(uint32_t));
    }

    // Reserve space in lmdb to store this data
    MDB_val key,data;
    key.mv_size = sizeof(uint32_t);
    key.mv_data = &od->oid;
    data.mv_data = NULL;
    data.mv_size = size;

    if (mdb_put(si->txn, si->oid2fndata_dbi, &key, &data, MDB_RESERVE) != 0) {
        M_ERR("Failed to allocate data to store oid2fndata for oid %u", od->oid);
        return;
    }

    // First store numeric data
    double *dpos = data.mv_data;
    for (int i = 0; i < si->map->num_numbers; i++) {
        dpos[i] = od->num_data[i];
    }

    uint32_t *pos = (uint32_t *)(dpos + si->map->num_numbers);
    // For each facet, we have a count followed by facet ids
    // uint32 (count) | uint32 (facet id) | uint32 (facet id) ...
    // There is not point storing facet id as vint as these are facet hashes
    // and not really useful to be a vint
    for (int i = 0; i < si->map->num_facets; i++) {
        facets_t *f = (facets_t *)pos;
        f->count = kv_size(od->facet_data[i]);
        for (int j = 0; j < f->count; j++) {
            f->data[j] = kv_A(od->facet_data[i], j);
        }
        pos += (f->count + 1);
    }
}

static inline uint8_t *write_vint(uint8_t *buf, const int vi) {
    uint32_t i = vi;
    while ((i & ~0x7F) != 0) {
        *buf = ((uint8_t)((i & 0x7f) | 0x80));
        buf++;
        i >>= 7;
    }
    *buf = ((uint8_t)i);
    buf++;
    return buf;
}

static inline uint8_t *read_vint(uint8_t *buf, int *value) {
    uint8_t b = *buf;
    buf++;
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = *buf;
        buf++;
        i |= (b & 0x7F) << shift;
    }
    *value = i;
    return buf; 
} 

#if 0
static void dump_widpos(wp_hold_t *h, int len) {
    for (int i = 0; i < len; i++) {
        wid_pos_t *v = h[i].wp;
        printf("%u %d %u\n", v->wid, v->priority, v->position);
    }
}

static void dump_word_data(uint8_t *head) {
    uint8_t *pos = head;
    uint16_t *num_words = (uint16_t *)pos;
    pos += 2;
    wid_info_t *wi = (wid_info_t *) pos;
    printf("Num words is %d\n", *num_words);
    for (int i = 0; i < *num_words; i++) {
        printf("Word id %u is_position %u pri %u offset %u\n", wi[i].wid, wi[i].is_position, wi[i].priority, wi[i].offset );
        if (!wi[i].is_position) {
            uint8_t *p = head + wi[i].offset;
            uint8_t *freq = p;
            p++;
            printf("Freq %u\n", *freq);
            int x = 0;
            while (x < *freq) {
                uint8_t *prio = p;
                p++;
                uint8_t *count = p;
                p++;
                printf("Freq %u : Priority %u : count %u\n", *freq, *prio, *count);
                for (int j = 0; j < *count; j++) {
                    int pos = 0;
                    p = read_vint(p, &pos);
                    printf("Position : %d\n", pos);
                    x++;
                }
                if (*p != 0xFF) {
                    printf("WTF\n");
                }
                p++;
            }

        }
    }
}
#endif

/* Returns number of occurences of word id in a given object */
static inline int obj_wid_count(struct obj_data *od, uint32_t wid) {
    khiter_t k = kh_get(UNIQWID, od->kh_uniqwid, wid);
    if (LIKELY(k != kh_end(od->kh_uniqwid))) {
        return kh_value(od->kh_uniqwid, k);
    }
    return 0;
}


/* Stores word id / frequency and positions for each object */
static void store_wordpos_data(struct sindex *si, struct obj_data *od) {
    if (UNLIKELY(kv_size(od->kv_widpos) == 0)) return;
    /* First sort the position information on wid.priority.position */
    wp_hold_t *h = malloc(sizeof(wp_hold_t) * kv_size(od->kv_widpos));
    size_t wp_len = kv_size(od->kv_widpos);
    for (int i = 0; i < wp_len; i++) {
        h[i].wp = kv_A(od->kv_widpos, i);
    }
    // dump_widpos(h, wp_len);
    ks_introsort(compwordpos, wp_len, h);
    // dump_widpos(h, wp_len);
    /* Save it to dbi */
    /* First allocate the max possible memory that may be used for this data */
    // TODO: Big comment on how data is organized, explain the design picture from notes
    uint8_t *data = malloc(kv_size(od->kv_widpos) * sizeof(wid_pos_t));
    uint16_t *numwords = (uint16_t *) data;
    *numwords = kh_size(od->kh_uniqwid);
    uint8_t *pos = data + 2;  
    uint8_t *dpos = pos + (kh_size(od->kh_uniqwid) * sizeof(wid_info_t));

    // Walk through the list and fill words / freq / positions
    int x = 0;
    uint32_t last_wid = 0;
    uint8_t last_priority = 0xFF;
    uint8_t *wid_count = NULL;
    wid_info_t *wi = (wid_info_t *) pos;
    wi--;

    while (x < wp_len) {
        wid_pos_t *wp = h[x].wp;
        // printf("Setting wid %u pri %d pos %u\n", wp->wid, wp->priority, wp->position);
        // Updates the wid_info at the head
        if (wp->wid != last_wid) {
            last_wid = wp->wid;
            wi++;
            int freq = obj_wid_count(od, wp->wid);
            // printf("Frequency is %d\n", freq);
            wi->wid = wp->wid;
            if (freq == 1) {
                wi->is_position = 1;
                wi->priority = wp->priority;
                wi->offset = wp->position;
                goto next_widpos;
            } else {
                wi->is_position = 0;
                wi->priority = 0;
                wi->offset = (dpos - data);
                last_priority = 0xFF;
                *dpos = freq;
                dpos++;
            }
        }
        // Updates the data portion, which holds priority / word count
        if (last_priority != wp->priority) {
           // printf("Priority for wid %u changed from %u to %u\n", last_wid, last_priority, wp->priority);
            last_priority = wp->priority;
            *dpos = wp->priority;
            dpos++;
            wid_count = dpos;
            *wid_count = 0;
            dpos++;
        }
        // Updates the position information
        *wid_count =  *wid_count + 1;
        dpos = write_vint(dpos, wp->position);
        if (!((x + 1) < wp_len && h[x+1].wp->wid == last_wid && 
                    h[x+1].wp->priority == last_priority)) {
            *dpos = 0xFF;
            dpos++;
        }

next_widpos:
        x++;
    }
    // printf("Full word data length %lu\n", (dpos - data));
    // dump_word_data(data);

    // Reserve space in lmdb to store this data
    MDB_val key, value;
    key.mv_size = sizeof(uint32_t);
    key.mv_data = &od->oid;
    value.mv_data = data;
    value.mv_size = (dpos - data);

    if (mdb_put(si->txn, si->oid2wpos_dbi, &key, &value, 0) != 0) {
        M_ERR("Failed to store oid2wpos for oid %u", od->oid);
    }
    free(data);
    free(h);
}

/* Stores the per object data into lmdb. Per object data is
 * store in 2 different dbi's.  One for word / freq / position
 * the other for numbers & facets.
 *
 * Finally clears memory allocated to store per object index info
 */
static void sindex_store_objdata(struct sindex *si) {
    struct obj_data *od = &si->wc->od;

    store_facet_num_data(si, od);
    store_wordpos_data(si, od);

    /* Clean up per obj data */
    for (int i = 0; i < kv_size(od->kv_widpos); i++) {
        free(kv_A(od->kv_widpos, i));
    }
    kv_destroy(od->kv_widpos);
    kh_destroy(UNIQWID, od->kh_uniqwid);

    for (int i = 0; i<si->map->num_facets; i++) {
        kv_destroy(od->facet_data[i]);
    }
}


/**** Indexing *****/

/* Indexes a double into the respective num_dbi */
static inline void index_number(struct sindex *si, int priority, double d) {
    uint32_t oid = si->wc->od.oid;
    MDB_val key, data;
    key.mv_size = sizeof(d);
    key.mv_data = &d;
    data.mv_size = sizeof(oid);
    data.mv_data = &oid;
    mdb_put(si->txn, si->num_dbi[priority], &key, &data, 0);
}

static inline void index_boolean(struct sindex *si, int priority, bool b) {
    wid2bmap_add(si->boolid2bmap_dbi, si->wc->kh_boolid2bmap, si->txn, 
                         b, si->wc->od.oid, priority);
}

/**
 * Takes a facet string and generates a facetid using farmhash.  It then maps
 * the facetid to a bmap of objids which contain this facet.  It also stores this facetid
 * in the per object index data
 */
static inline void index_string_facet(struct sindex *si, const char *str, int priority) {
    // Facetid is a 32 bit farmhash of the string to be indexed
    // TODO: Collisions ? Use different seed?
    // TODO: Maintain a hashtable for a bulk write instead of hashing everytime wit farmhash?
    struct obj_data *od = &si->wc->od;
    uint32_t facet_id = farmhash32(str, strlen(str));

    // See if facetid2str is already present, else store it in a hashtable
    // This will be flushed to lmdb at the end of write
    khash_t(FACETID2STR) *kh = si->wc->kh_facetid2str;
    khiter_t k = kh_get(FACETID2STR, kh, facet_id);
    if (k == kh_end(kh)) {
        // We need to add the entry to be written
        int ret = 0;
        k = kh_put(FACETID2STR, kh, facet_id, &ret);

        // Check if it already exists in lmdb
        MDB_val key, data;
        key.mv_size = sizeof(uint32_t);
        key.mv_data = (void *)&facet_id;
        // If it already exists, we need not write so set a NULL value to 
        // avoid looking up mdb everytime we encounter this facetid
        if (mdb_get(si->txn, si->facetid2str_dbi, &key, &data) == 0) {
            kh_value(kh, k) = NULL;
        } else {
            // We need to store this value, create a copy which will 
            // be freed at the end of write
            kh_value(kh, k) = strdup(str);
        }
    }

    // Store it in per obj data
    kv_push(uint32_t, od->facet_data[priority], facet_id);
    // Update facetid -> bitmap of all documents with this facet id
    wid2bmap_add(si->facetid2bmap_dbi, si->wc->kh_facetid2bmap, si->txn, 
                         facet_id, od->oid, priority);
}

static void string_new_word_pos(word_pos_t *wp, void *data) {
    struct analyzer_data *ad = data;
    struct sindex *si = ad->si;
    struct obj_data *od = &si->wc->od;
    int p = ad->priority + 1;
    int limit = wp->word.length >= LEVLIMIT ? LEVLIMIT : wp->word.length;

    // Check if the word exists
    uint32_t wid = dtrie_exists(si->trie, wp->word.chars, wp->word.length, od->twid);
    if (wid == 0) {
        // If it does not exist, insert it and update the top-level word ids
        // mapping to include the newly added word id
        wid = dtrie_insert(si->trie, wp->word.chars, wp->word.length, od->twid);
        // Store top-level word id to word id mapping
        for (int i = 0; i < limit; i++) {
            wid2bmap_add(si->twid2widbmap_dbi, si->wc->kh_twid2widbmap, si->txn, 
                         od->twid[i], wid, 0);
            wid2bmap_add(si->twid2widbmap_dbi, si->wc->kh_twid2widbmap, si->txn, 
                         od->twid[i], wid, p);
        }
    }

    // Set the top-level wid to obj id mapping
    for (int i = 0; i < limit; i++) {
        wid2bmap_add(si->twid2bmap_dbi, si->wc->kh_twid2bmap, si->txn, 
                     od->twid[i], od->oid, 0);
        wid2bmap_add(si->twid2bmap_dbi, si->wc->kh_twid2bmap, si->txn, 
                     od->twid[i], od->oid, p);
    }

    // Set the wid to obj id mapping
    wid2bmap_add(si->wid2bmap_dbi, si->wc->kh_wid2bmap, si->txn, 
            wid, od->oid, 0);
    wid2bmap_add(si->wid2bmap_dbi, si->wc->kh_wid2bmap, si->txn, 
            wid, od->oid, p);

    // Store wid positions
    wid_pos_t *widpos = malloc(sizeof(wid_pos_t));
    widpos->wid = wid;
    widpos->priority = ad->priority;
    widpos->position = wp->position;
    // Add it to the end of the list
    kv_push(wid_pos_t *, od->kv_widpos, widpos);
    // Add the word id to the hash set
    int ret = 0;
    khiter_t k = kh_put(UNIQWID, od->kh_uniqwid, wid, &ret);
    if (ret == 1) {
        kh_value(od->kh_uniqwid, k) = 1;
    } else {
        kh_value(od->kh_uniqwid, k) = kh_value(od->kh_uniqwid, k) + 1;
    }
    // If we are not the first word, add the wid pair to phrase dbi
    if (LIKELY(ad->prev_wid)) {
        // TODO : Uncomment once you figure out how to store phrase for each priority
        // and when you really need phrase search / split word search
        //wid2bmap_add(si->phrase_dbi, si->wc->kh_phrasebmap, si->txn,
        //        ad->prev_wid, od->oid, wid);
    }
    ad->prev_wid = wid;
}

// TODO: better analyzer usage, configurable etc.,
static void index_string(struct sindex *si, const char *str, int priority) {
    struct analyzer_data ad;
    ad.si = si;
    ad.priority = priority;
    ad.prev_wid = 0;
    struct analyzer *a = get_default_analyzer();
    a->analyze_string_for_indexing(str, string_new_word_pos, &ad);
}


/**
 * Parses and indexes an object.  This uses the index schema map and updates the 
 * write cache with parsed information.
 */
static void parse_index_object(struct sindex *si, struct schema *s, json_t *j) {
    struct obj_data *od = &si->wc->od;
    // Browse the schema and retrieve the fields from the json object based on 
    // schema.  Ignore or throwaway fields which do not map to field type present
    // in schema
    while (s) {
        switch (s->type) {
            case F_STRING: {
                json_t *js = json_object_get(j, s->fname);
                if (!json_is_string(js)) break;
                const char *str = json_string_value(js);
                if (s->is_facet) {
                    index_string_facet(si, str, s->f_priority);
                }
                if (s->is_indexed) {
                    index_string(si, str, s->i_priority);
                }
            }
            break;
            case F_STRLIST: {
                json_t *jarr = json_object_get(j, s->fname);
                if (!json_is_array(jarr)) break;
                size_t jid;
                json_t *js;
                json_array_foreach(jarr, jid, js) {
                    const char *str = json_string_value(js);
                    if (s->is_facet) {
                        index_string_facet(si, str, s->f_priority);
                    }
                    if (s->is_indexed) {
                        index_string(si, str, s->i_priority);
                    }
                }
            }
            break;
            case F_NUMBER: {
                json_t *jn = json_object_get(j, s->fname);
                if (!json_is_number(jn)) break;
                double d = json_number_value(jn);
                od->num_data[s->i_priority] = d;
                // Index the number for sorting and filtering
                if (s->is_indexed) {
                    index_number(si, s->i_priority, d);
                }
            }
            break;
            case F_NUMLIST: {
                json_t *jarr = json_object_get(j, s->fname);
                if (!json_is_array(jarr)) break;
                size_t jid;
                json_t *jn;
                json_array_foreach(jarr, jid, jn) {
                    if (!json_is_number(jn)) continue;
                    double d = json_number_value(jn);
                    if (s->is_indexed) {
                        index_number(si, s->i_priority, d);
                    }
                    // TODO: Decide on how to store arrays in objdata and index
                    // Probably the same way we handle facets?
                }
            }
            break;
            case F_BOOLEAN: {
                json_t *jb = json_object_get(j, s->fname);
                if (!json_is_boolean(jb)) break;
                // True and false are set in different bitmaps
                index_boolean(si, s->i_priority, json_is_true(jb));
            }
            break;
            // For objects, recursively call with inner objects in both schema
            // and object
            case F_OBJECT: {
                json_t *jo = json_object_get(j, s->fname);
                if (!json_is_object(jo)) break;
                parse_index_object(si, s->child, jo);
            }
            break;
            case F_OBJLIST: {
                json_t *jarr = json_object_get(j, s->fname);
                if (!json_is_array(jarr)) break;
                size_t jid;
                json_t *jo;
                json_array_foreach(jarr, jid, jo) {
                    if (json_is_object(jo)) {
                        parse_index_object(si, s->child, jo);
                    }
                }
            }
            break;
            default:
                break;
        }
        s = s->next;
    }
}


/* Entry point to parse and index an object, this assumes that 
 * the write cache is ready */
static void si_add_object(struct sindex *si, json_t *j) {
    // Get the oid from the object, this was previously set when
    // adding the object to sdata
    si->wc->od.oid = json_number_value(json_object_get(j, J_OID));
    // Setup per object data
    sindex_objdata_init(si);

    // Index schema is used to parse the object
    parse_index_object(si, si->map->index_schema->child, j);

    // Store the parsed object and cleanup
    sindex_store_objdata(si);
}

/* Adds one or more objects to the shard index.  This parses the input and updates
 * the various num / string / facet dbis. */
void sindex_add_objects(struct sindex *si, json_t *j) {
    // Make sure we are ready to index these objects
    if (UNLIKELY((!j))) return;
    if (UNLIKELY(json_is_null(j))) return;
    if (UNLIKELY(si->map == NULL)) return;
    if (UNLIKELY(!si->map->ready_to_index)) return; 

    //  We are good to go now
    si_write_start(si);
    if (json_is_array(j)) {
        size_t idx;
        json_t *obj;
        json_array_foreach(j, idx, obj) {
            si_add_object(si, obj);
        }
    } else {
        si_add_object(si, j);
    }
    si_write_end(si);
}

/**
 * Updates / sets the shard index mapping.
 * This opens the necessary dynamic dbis (num / geo) as required
 */
void sindex_set_mapping(struct sindex *si, const struct mapping *map) {
    si->map = map;

    M_DBG("Index mapping set for index %s", si->shard->index->name);
    mdb_txn_begin(si->env, NULL, 0, &si->txn);
    for (int i = 0; i < map->num_numbers; i++) {
        // Form a unique dbi name for every dbi to be opened
        char dbi_name[16];
        snprintf(dbi_name, sizeof(dbi_name), "%s_%d", DBI_NUM, i);
        // Open it, allow duplicates and use a custom compare function which compares
        // doubles.  All numbers are treated as doubles for now.
        mdb_dbi_open(si->txn, dbi_name, MDB_CREATE|MDB_DUPSORT|MDB_DUPFIXED|MDB_INTEGERDUP, 
                                  &si->num_dbi[i]);
        mdb_set_compare(si->txn, si->num_dbi[i], double_compare);
    }
    mdb_txn_commit(si->txn);
}


/* Deletes the shard index.  Drops all index data and removes the index folder */
void sindex_delete(struct sindex *si) {
    // Drop all dbis
    sindex_clear(si);
    char path[PATH_MAX];
    struct shard *shard = si->shard;
    // Get the shard_data path
    snprintf(path, sizeof(path), "%s/%s", shard->base_path, shard->idx_name);
 
    sindex_free(si);

    // Remove index folder for this shard_index
    rmdir(path);
}

void sindex_clear(struct sindex *si) {
    // TODO: Drop all dbis
}

void sindex_free(struct sindex *si) {
    mdb_env_close(si->env);
    free(si);
}

struct sindex *sindex_new(struct shard *shard) {
    struct sindex *si = calloc(1, sizeof(struct sindex));
    si->shard = shard;

    // Create path necessary to store shard index
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", shard->base_path, shard->idx_name);
    mkdir(path, 0775);

    // Initialize mdb access
    mdb_env_create(&si->env);
    int rc = mdb_env_set_mapsize(si->env, MDB_ENV_SIZE);
    if (rc != 0) {
        M_ERR("Error setting mapsize on [%s/%s] %d", shard->index->app->name, shard->index->name, rc);
    }
    mdb_env_set_maxdbs(si->env, 64);
    mdb_env_open(si->env, path, MDB_NOSYNC, 0664);
    mdb_txn_begin(si->env, NULL, 0, &si->txn);

    // Open all necessary dbis here
    mdb_dbi_open(si->txn, DBI_FACETID2STR, MDB_CREATE|MDB_INTEGERKEY, &si->facetid2str_dbi);
    mdb_dbi_open(si->txn, DBI_FACETID2BMAP, MDB_CREATE|MDB_INTEGERKEY, &si->facetid2bmap_dbi);
    mdb_dbi_open(si->txn, DBI_BOOLID2BMAP, MDB_CREATE|MDB_INTEGERKEY, &si->boolid2bmap_dbi);
    mdb_dbi_open(si->txn, DBI_TWID2WIDBMAP, MDB_CREATE|MDB_INTEGERKEY, &si->twid2widbmap_dbi);
    mdb_dbi_open(si->txn, DBI_TWID2BMAP, MDB_CREATE|MDB_INTEGERKEY, &si->twid2bmap_dbi);
    mdb_dbi_open(si->txn, DBI_WID2BMAP, MDB_CREATE|MDB_INTEGERKEY, &si->wid2bmap_dbi);
    mdb_dbi_open(si->txn, DBI_OID2FNDATA, MDB_CREATE|MDB_INTEGERKEY, &si->oid2fndata_dbi);
    mdb_dbi_open(si->txn, DBI_OID2WPOS, MDB_CREATE|MDB_INTEGERKEY, &si->oid2wpos_dbi);
    mdb_dbi_open(si->txn, DBI_PHRASE, MDB_CREATE|MDB_INTEGERKEY, &si->phrase_dbi);

    strcat(path, "/dtrie.db");
    si->trie = dtrie_new(path, si->boolid2bmap_dbi, si->txn);
 
    mdb_txn_commit(si->txn);
    return si;
}
 
