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

/* Adds one or more objects to a shard */
void shard_add_objects(struct shard *s, json_t *j) {
    // First add it to the shard datastore, this updates
    // the shard object id
    sdata_add_objects(s->sdata, j);
    // Then index the object data
    sindex_add_objects(s->sindex, j);
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
    // Now take car of the search index
    sindex_delete(s->sindex);
    s->sindex = NULL;
    // Free the shard
    shard_free(s);
    rmdir(s->base_path);
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

