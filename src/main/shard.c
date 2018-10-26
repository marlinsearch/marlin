#include "shard.h"
#include "common.h"
#include "marlin.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

void shard_set_mapping(struct shard *s, const struct mapping *m) {
    s->sindex->map = m;
}

/* Adds one or more objects to a shard */
void shard_add_objects(struct shard *s, json_t *j) {
    // First add it to the shard datastore, this updates
    // the shard object id
    sdata_add_objects(s->sdata, j);
    // Then index the object data
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

    // Form the shard path, we need this to delete
    struct index *in = s->index;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s_%d", marlin->db_path, 
             in->app->name, in->name, "s", s->shard_id);
    // Free the shard
    shard_free(s);
    rmdir(path);
}

struct shard *shard_new(struct index *in, uint16_t shard_id) {
    struct shard *s = calloc(1, sizeof(struct shard));
    s->index = in;
    s->shard_id = shard_id;
    // Create shard path if required
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s_%d", marlin->db_path, 
             in->app->name, in->name, "s", shard_id);
    mkdir(path, 0775);
 
    // create / load shard data for this shard
    s->sdata = sdata_new(s);
    // Create / load shard search index for this shard
    s->sindex = sindex_new(s);

    return s;
}

