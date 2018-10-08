#include "shard.h"
#include "common.h"
#include "marlin.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

/* Adds one or more objects to a shard */
void shard_add_objects(struct shard *s, json_t *j) {
    // First add it to the shard datastore
    sdata_add_objects(s->sdata, j);
    // Then index the object data
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

    return s;
}


void shard_free(struct shard *s) {
    // Free shard data
    if (s->sdata) {
        sdata_free(s->sdata);
    }
    free(s);
}

void shard_clear(struct shard *s) {
    // Clear the shard data
    sdata_clear(s->sdata);
}

void shard_delete(struct shard *s) {
    // FIrst delete the shard data
    sdata_delete(s->sdata);
    s->sdata = NULL;
    // Form the shard path, we need this to delete
    struct index *in = s->index;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s_%d", marlin->db_path, 
             in->app->name, in->name, "s", s->shard_id);
    // Free the shard
    shard_free(s);
    rmdir(path);
}

