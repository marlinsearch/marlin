#include "sdata.h"
#include "marlin.h"
#include "common.h"
#include "lz4.h"
#include "zlib.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

// Save last oid in mdb, assumes a txn is already in progress
static void save_last_oid(struct sdata *sd) {
    uint32_t x = 0xFFFFFFFF;
    MDB_val key, data;
    key.mv_size = sizeof(x);
    key.mv_data = &x;
    data.mv_data = &sd->lastoid;
    data.mv_size = sizeof(sd->lastoid);
    mdb_put(sd->txn, sd->sid2json_dbi, &key, &data, 0);
}

// Loads sdata info from mdb, assumes a transaction is already open
static void load_sdata_info(struct sdata *sd) {
    uint32_t x = 0xFFFFFFFF;
    MDB_val key, data;
    key.mv_size = sizeof(x);
    key.mv_data = &x;
    if (mdb_get(sd->txn, sd->sid2json_dbi, &key, &data) == 0) {
        sd->lastoid = *(uint32_t *) data.mv_data;
    } else {
        save_last_oid(sd);
    }
}

/* Gets a free object id */
// TODO: Handle deletions and maintain free objids in bitmap
static uint32_t get_free_objid(struct sdata *sd) {
    uint32_t new_oid = sd->lastoid;
    sd->lastoid++;
    return new_oid;
}

static char *jcompress(const char *src, int *compressed_len) {
#ifdef USE_LZ4
    const int src_size = (int) (strlen(src) + 1 );
    const int max_dst_size = LZ4_compressBound(src_size);
    char* compressed_data = malloc(max_dst_size);
    const int compressed_data_size = LZ4_compress_default(src, compressed_data, src_size, max_dst_size);
    *compressed_len = compressed_data_size;
    return compressed_data;
#else
    const int src_size = (int) (strlen(src) + 1 );
    size_t max_dst_size = compressBound(src_size);
    char* compressed_data = malloc(max_dst_size);
    compress((Bytef *)compressed_data, &max_dst_size, (Bytef *)src, src_size);
    *compressed_len = max_dst_size;
    return compressed_data;
#endif
}

static void sdata_add_object(struct sdata *sd, json_t *j) {
    // Make sure it is a valid object before trying to add it
    if (!j) return;
    if (json_is_null(j)) return;
    // TODO: Handle free entries from free_bm, also read below note !
    uint32_t newoid = get_free_objid(sd);
    json_object_set_new(j, J_SID, json_integer(newoid));
    char *jdata = json_dumps(j, JSON_PRESERVE_ORDER|JSON_COMPACT);
    int compressed_len;
    char *compressed_data = jcompress(jdata, &compressed_len);

    // Update SID2JSON
    MDB_val key, data;
    key.mv_size = sizeof(uint32_t);
    key.mv_data = &newoid;
    data.mv_size = compressed_len;
    data.mv_data = compressed_data;
    mdb_put(sd->txn, sd->sid2json_dbi, &key, &data, 0);

    // Update ID2SID
    const char *id = json_string_value(json_object_get(j, J_ID));
    key.mv_data = (void *)id;
    key.mv_size = sd->custom_id? strlen(id) + 1 : 22;
    data.mv_size = sizeof(uint32_t);
    data.mv_data = &newoid;
    mdb_put(sd->txn, sd->id2sid_dbi, &key, &data, 0);

    free(jdata);
    free(compressed_data);
}


/* Takes one more more objects for a shard and stores it in the shard datastore
 * A shard object id which will be the position of the object in the obj bitmap index is
 * assigned first. Any free ids due to deletions are used in advance to keep our obj
 * ids compact. The objects themselves are lz4 compressed and stored */
void sdata_add_objects(struct sdata *sd, json_t *j) {
    uint32_t oid = sd->lastoid;
    mdb_txn_begin(sd->env, NULL, 0, &sd->txn);
    if (json_is_array(j)) {
        size_t objid;
        json_t *obj;
        json_array_foreach(j, objid, obj) {
            sdata_add_object(sd, obj);
        }
    } else {
        sdata_add_object(sd, j);
    }
    // store the free_bm / used_bm objid maps
    if (oid != sd->lastoid) {
        save_last_oid(sd);
    }
    mdb_txn_commit(sd->txn);
 
}

struct sdata *sdata_new(struct shard *shard) {
    struct sdata *s = calloc(1, sizeof(struct sdata));
    s->shard = shard;
    s->lastoid = 1;
    s->custom_id = false;
    struct index *in = shard->index;

    // Create path necessary to store shard data
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s_%d/data", marlin->db_path, in->app->name, in->name, "s", shard->shard_id);
    mkdir(path, 0775);
 
    // Initialize mdb access
    mdb_env_create(&s->env);
    int rc = mdb_env_set_mapsize(s->env, MDB_ENV_SIZE);
    if (rc != 0) {
        M_ERR("Error setting mapsize on [%s] %d", in->name, rc);
    }

    mdb_env_set_maxdbs(s->env, 2);
    mdb_env_open(s->env, path, 0, 0664);
    mdb_txn_begin(s->env, NULL, 0, &s->txn);
    if (mdb_dbi_open(s->txn, DBI_SID2JSON, MDB_CREATE|MDB_INTEGERKEY, &s->sid2json_dbi) != 0) {
        M_ERR("Failed to load %s dbi", DBI_SID2JSON);
    } else {
        load_sdata_info(s);
    }
    if (mdb_dbi_open(s->txn, DBI_ID2SID, MDB_CREATE, &s->id2sid_dbi) != 0) {
        M_ERR("Failed to load %s dbi", DBI_ID2SID);
    }
    mdb_txn_commit(s->txn);
 
    return s;
}
