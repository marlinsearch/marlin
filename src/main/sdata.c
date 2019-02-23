/* Shard Data - Handles storing, retrieving and setting ID for the JSON documents.
 *
 * All documents are compressed by zlib before storing in lmdb.  An object shard id is assigned
 * to every object sequentially as long as there are no deletions.  In case of deletions,
 * deleted object ids are reused. This object SID is used to refer to the document all over
 * Shard Index (sindex.c).  In the future, during reindexing, the document ids will be 
 * reassigned for compaction in case of massive deletes */

#include "sdata.h"
#include "marlin.h"
#include "common.h"
#include "lz4.h"
#include "zlib.h"
#include "dbi.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

// Save last docid in mdb, assumes a txn is already in progress
static void save_last_docid(struct sdata *sd) {
    uint32_t x = 0xFFFFFFFF;
    MDB_val key, data;
    key.mv_size = sizeof(x);
    key.mv_data = &x;
    data.mv_data = &sd->last_docid;
    data.mv_size = sizeof(sd->last_docid);
    mdb_put(sd->txn, sd->sid2json_dbi, &key, &data, 0);
}

// Loads sdata info from mdb, assumes a transaction is already open
static void load_sdata_info(struct sdata *sd) {
    uint32_t x = 0xFFFFFFFF;
    MDB_val key, data;
    key.mv_size = sizeof(x);
    key.mv_data = &x;
    if (mdb_get(sd->txn, sd->sid2json_dbi, &key, &data) == 0) {
        sd->last_docid = *(uint32_t *) data.mv_data;
    } else {
        save_last_docid(sd);
    }
}

/* Gets a free object id */
// TODO: Handle deletions and maintain free docids in bitmap
static uint32_t get_free_docid(struct sdata *sd) {
    uint32_t new_docid = sd->last_docid;
    sd->last_docid++;
    return new_docid;
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
    uint32_t new_docid = get_free_docid(sd);
    json_object_set_new(j, J_DOCID, json_integer(new_docid));
    char *jdata = json_dumps(j, JSON_PRESERVE_ORDER|JSON_COMPACT);
    int compressed_len;
    char *compressed_data = jcompress(jdata, &compressed_len);

    // Update SID2JSON
    MDB_val key, data;
    key.mv_size = sizeof(uint32_t);
    key.mv_data = &new_docid;
    data.mv_size = compressed_len;
    data.mv_data = compressed_data;
    mdb_put(sd->txn, sd->sid2json_dbi, &key, &data, 0);

    // Update ID2SID - Id to ShardId
    const char *id = json_string_value(json_object_get(j, J_ID));
    key.mv_data = (void *)id;
    key.mv_size = sd->custom_id? strlen(id) + 1 : 22;
    data.mv_size = sizeof(uint32_t);
    data.mv_data = &new_docid;
    mdb_put(sd->txn, sd->id2sid_dbi, &key, &data, 0);

    free(jdata);
    free(compressed_data);
}


/* Takes one more more documents for a shard and stores it in the shard datastore
 * A shard object id which will be the position of the object in the obj bitmap index is
 * assigned first. Any free ids due to deletions are used in advance to keep our obj
 * ids compact. The documents themselves are zlib/lz4 compressed and stored */
void sdata_add_documents(struct sdata *sd, json_t *j) {
    uint32_t docid = sd->last_docid;
    mdb_txn_begin(sd->env, NULL, 0, &sd->txn);
    if (json_is_array(j)) {
        size_t docid;
        json_t *obj;
        json_array_foreach(j, docid, obj) {
            sdata_add_object(sd, obj);
        }
    } else {
        sdata_add_object(sd, j);
    }
    // store the free_bm / used_bm docid maps
    if (docid != sd->last_docid) {
        save_last_docid(sd);
    }
    mdb_txn_commit(sd->txn);
 
}

void sdata_free(struct sdata *sd) {
    mdb_dbi_close(sd->env, sd->id2sid_dbi);
    mdb_dbi_close(sd->env, sd->sid2json_dbi);
    mdb_env_close(sd->env);
    free(sd);
}

void sdata_clear(struct sdata *sd) {
    mdb_txn_begin(sd->env, NULL, 0, &sd->txn);
    int rc = 0;
    if ((rc = mdb_drop(sd->txn, sd->id2sid_dbi, 0)) != 0) {
        M_ERR("Failed to drop id2sid dbi %d %s", rc, mdb_strerror(rc));
    }
    if ((rc = mdb_drop(sd->txn, sd->sid2json_dbi, 0)) != 0) {
        M_ERR("Failed to drop sid2json dbi %d %s", rc, mdb_strerror(rc));
    }
    mdb_txn_commit(sd->txn);
    sd->last_docid = 1;
}

/* Deletes the shard data.  Drops all data and removes the data folder */
void sdata_delete(struct sdata *sd) {
    // Drop all dbis
    sdata_clear(sd);
    char path[PATH_MAX];
    struct shard *shard = sd->shard;
    // Get the shard_data path
    snprintf(path, sizeof(path), "%s/data", shard->base_path);
    // Free shard data which closes the environment and dbis
    sdata_free(sd);
    char fpath[PATH_MAX];
    // Now delete the data and lock files
    snprintf(fpath, PATH_MAX, "%s/%s", path, MDB_DATA_FILE);
    unlink(fpath);
    snprintf(fpath, PATH_MAX, "%s/%s", path, MDB_LOCK_FILE);
    unlink(fpath);
    // Remove data folder for this shard_data
    rmdir(path);
}

struct sdata *sdata_new(struct shard *shard) {
    struct sdata *s = calloc(1, sizeof(struct sdata));
    s->shard = shard;
    s->last_docid = 1;
    s->custom_id = false;
    struct index *in = shard->index;

    // Create path necessary to store shard data
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data", shard->base_path);
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

