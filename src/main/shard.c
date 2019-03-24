#include "shard.h"
#include "common.h"
#include "marlin.h"
#include "utils.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

static void shard_save_info(struct shard *s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", s->base_path, SHARD_FILE);
    FILE *f = fopen(path, "w");
    if (!f) {
        M_ERR("Failed to save datastore index name");
        return;
    }
    fprintf(f, "{\"%s\":\"%s\"}", J_NAME, s->idx_name);
    fclose(f);
}

// Loads shard information, this gives the actual shard index file path
static void shard_load_info(struct shard *s) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", s->base_path, SHARD_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_object(json)) {
        const char *name = json_string_value(json_object_get(json, J_NAME));
        snprintf(s->idx_name, sizeof(s->idx_name), "%s", name);
        json_decref(json);
    }
}

void shard_set_mapping(struct shard *s, const struct mapping *m) {
    M_DBG("Shard mapping set");
    // TODO: This should be a function in sindex, 
    // Set current index for removal and create a new sindex and
    // set mapping.
    sindex_set_mapping(s->sindex, m);
}

/* Adds one or more documents to a shard */
void shard_add_documents(struct shard *s, json_t *j) {
    // First add it to the shard datastore, this updates
    // the shard document id
    sdata_add_documents(s->sdata, j);
    // Then index the document data
    sindex_add_documents(s->sindex, j);
}

struct bmap *shard_get_all_docids(struct shard *s) {
    return bmap_duplicate(s->sdata->used_bmap);
}

void shard_free(struct shard *s) {
    // Free shard data
    // Note: Pointer check if it valid is done as during
    // a delete, this would already be freed
    if (s->sdata) {
        sdata_free(s->sdata);
    }
    if (s->sindex) {
        sindex_free(s->sindex);
    }
    free(s);
}

void shard_clear(struct shard *s) {
    // Clear the shard data
    sdata_clear(s->sdata);
    // Now clear the search index
    sindex_clear(s->sindex);
}

void shard_delete(struct shard *s) {
    // NOTE: Delete and set the pointers for sdata and sindex to NULL
    // so a shard_free which comes later on does not try to free it again
    // FIrst delete the shard data
    sdata_delete(s->sdata);
    s->sdata = NULL;
    // Now take care of the search index
    sindex_delete(s->sindex);
    s->sindex = NULL;
    
    // Collect path names
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s", s->base_path);
    char fpath[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s/%s", s->base_path, SHARD_FILE);
    
    // Free the shard
    shard_free(s);

    // Delete from disk
    unlink(fpath);
    rmdir(path);
}

/* Get the document with id from shard data */
char *shard_get_document(const struct shard *s, const char *id) {
    return sdata_get_document(s->sdata, id);
}

/* Delete the given document from a shard */
bool shard_delete_document(struct shard *s, const json_t *j) {
    const char *id = json_string_value(json_object_get(j, J_ID));
    if (!id) return false;
    uint32_t docid = sdata_delete_document(s->sdata, id);
    if (docid) {
        sindex_delete_document(s->sindex, j, docid);
    }
    return !!docid;
}

bool shard_replace_document(struct shard *s, struct json_t *newj, const struct json_t *oldj) {
    // If we have an existing document to replace, delete that
    if (oldj) {
        const char *id = json_string_value(json_object_get(oldj, J_ID));
        if (!id) return false;
        uint32_t docid = sdata_delete_document(s->sdata, id);
        if (docid) {
            sindex_delete_document(s->sindex, oldj, docid);
        }
    }
    shard_add_documents(s, newj);
    return true;
}

bool shard_update_document(struct shard *s, struct json_t *newj, struct json_t *oldj) {
    const char *id = json_string_value(json_object_get(oldj, J_ID));
    if (!id) return false;
    uint32_t docid = sdata_delete_document(s->sdata, id);
    if (docid) {
        sindex_delete_document(s->sindex, oldj, docid);
    }
    json_object_update(oldj, newj);
    shard_add_documents(s, oldj);
    return true;
}

struct shard *shard_new(struct index *in, uint16_t shard_id) {
    struct shard *s = calloc(1, sizeof(struct shard));
    s->index = in;
    s->shard_id = shard_id;
    // Create shard path if required
    snprintf(s->base_path, sizeof(s->base_path), "%s/%s/%s/data/%s_%d", marlin->db_path, 
             in->app->name, in->name, "s", shard_id);
    mkdir(s->base_path, 0775);
    shard_load_info(s);
    if (strlen(s->idx_name) == 0) {
        char idx[16];
        random_str(idx, 8);
        snprintf(s->idx_name, sizeof(s->idx_name), "index_%s", idx);
        // Save the newly assigned name
        shard_save_info(s);
    }
 
    // create / load shard data for this shard
    s->sdata = sdata_new(s);
    // Create / load shard search index for this shard
    s->sindex = sindex_new(s);
    return s;
}

