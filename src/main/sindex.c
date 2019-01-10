/* Shard Index - This is the search engine index manager, which takes care of 
 * parsing objects and building an index which lets us query these objects */
#include "sindex.h"
#include "marlin.h"
#include "common.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="


static void si_write_start(struct sindex *si) {
}

static void si_write_end(struct sindex *si) {
}

static void si_add_object(struct sindex *si, json_t *j) {
}

/**
 * Adds one or more objects to the shard index.  This parses the input and updates
 * the various num / string / facet dbis.
 */
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

/* Deletes the shard index.  Drops all index data and removes the index folder */
void sindex_delete(struct sindex *si) {
    // Drop all dbis
    sindex_clear(si);
    char path[PATH_MAX];
    struct shard *shard = si->shard;
    // Get the shard_data path
    snprintf(path, sizeof(path), "%s/%s/%s/%s_%d/index", marlin->db_path, shard->index->app->name, 
             shard->index->name, "s", shard->shard_id);
 
    sindex_free(si);

    // Remove index folder for this shard_index
    rmdir(path);
}

void sindex_clear(struct sindex *si) {
}

void sindex_free(struct sindex *si) {
    free(si);
}

struct sindex *sindex_new(struct shard *shard) {
    struct sindex *si = calloc(1, sizeof(struct sindex));
    si->shard = shard;
    struct index *in = shard->index;

    // Create path necessary to store shard index
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s_%d/index", marlin->db_path, in->app->name, 
             in->name, "s", shard->shard_id);
    mkdir(path, 0775);
 
    return si;
}
 
