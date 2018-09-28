#include <h2o.h>
#include "index.h"
#include "utils.h"
#include "keys.h"
#include "url.h"
#include "api.h"
#include "platform.h"
#include "marlin.h"
#include "workers.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="
#define USE_INDEX_THREAD_POOL 1

struct api_path {
    const char *method;
    const char *path;
    KEY_ACCESS access;
    char *(*api_cb)(h2o_req_t *req, void *data);
};

struct add_obj_tdata {
    struct worker *tdata;
    struct index *index;
    json_t *sh_j;
    int shard_idx;
};

void worker_add_process(void *w) {
    struct add_obj_tdata *add = w;
    shard_add_objects(kv_A(add->index->shards, add->shard_idx), add->sh_j);
    printf("Shard worker %d len %lu\n", add->shard_idx, json_array_size(add->sh_j));
    json_decref(add->sh_j);
    worker_done(add->tdata);
}

void index_worker_add_objects(struct index *in, json_t **sh_j) {
    struct worker worker;
    worker_init(&worker, in->num_shards);
    struct add_obj_tdata *sh_add = malloc(in->num_shards * sizeof(struct add_obj_tdata));
    for (int i = 0; i < in->num_shards; i++) {
        sh_add[i].tdata = &worker;
        sh_add[i].sh_j = sh_j[i];
        sh_add[i].index = in;
        sh_add[i].shard_idx = i;
        threadpool_add(index_pool, worker_add_process, &sh_add[i], 0);
    }

    wait_for_workers(&worker);
    worker_destroy(&worker);
}

/* This is where new objects are added to the index.
 * First we generate a objid for every object.  use the objid
 * to determine which shard it belongs to and send it over */
static void index_add_objects(struct index *in, json_t *j) {
    if (json_is_array(j)) {
        size_t index;
        json_t *jo;
        // First create shard specific array of objects
        json_t **sh_j = malloc(sizeof(json_t *) * in->num_shards);
        for (int i = 0; i < in->num_shards; i++) {
            sh_j[i] = json_array();
        }

        // set the object id for every object and hash the object id
        // to arrive at the shard the object belongs.  Add it to the
        // shard array
        json_array_foreach(j, index, jo) {
            char *id = generate_objid(in->fctx);
            json_object_set_new(jo, J_ID, json_string(id));
            int sid = get_shard_routing_id(id, in->num_shards);
            json_array_append(sh_j[sid], jo);
            free(id);
        }

#ifndef USE_INDEX_THREAD_POOL
        // Finally add objects to the shards
        // When not using thread pool it is simple, just iterate and
        // get things done
        for (int i=0; i < in->num_shards; i++) {
            // send to shard
            shard_add_objects(kv_A(in->shards, i), sh_j[i]);
            printf("Shard %d len %lu\n", i, json_array_size(sh_j[i]));
            json_decref(sh_j[i]);
        }
#else
        // When we use a thread pool we need to handle some additional
        // stuff 
        index_worker_add_objects(in, sh_j);
#endif
        free(sh_j);
    }
}

/* This is where any modifications to the index happen.  It happens one at a time
 * no two index jobs can ever run simultaneously and is controlled by the threadpool */
static void index_work_job(void *data) {
    struct in_job *job = data;
    struct index *in = job->index;
    if (in) {
        switch (job->type) {
            case JOB_ADD:
                index_add_objects(in, job->j);
                break;
            default:
                break;
        }
        ATOMIC_DEC(&in->job_count);
    }
    if (job->j) json_decref(job->j);
    if (job->j2) json_decref(job->j2);
    free(job);
}

/* This is how a job gets scheduled for an index.  It basically
 * adds the job to a threadpool, which invokes one job a time. */
static int index_add_job(struct index *in, struct in_job *job) {
    // No jobs, the wpool may need to be initialized
    if (in->job_count == 0) {
        RDLOCK(&in->wpool_lock);
        if (!in->wpool) {
            UNLOCK(&in->wpool_lock);
            WRLOCK(&in->wpool_lock);
            in->wpool = threadpool_create(1, JOB_QUEUE_LEN, 0);
            UNLOCK(&in->wpool_lock);
        } else {
            UNLOCK(&in->wpool_lock);
        }
    }
    int r = threadpool_add(in->wpool, index_work_job, job, 0);
    // Job added successfully
    if (r == 0) {
        ATOMIC_INC(&in->job_count);
    }
    return r;
}

/* Creates a new index job for a given index and job type */
static struct in_job *in_job_new(struct index *in, JOB_TYPE type) {
    struct in_job *job = calloc(1, sizeof(struct in_job));
    job->index = in;
    job->type = type;
    return job;
}

static void index_save_info(struct index *in) {
    json_t *j = json_object();
    json_object_set_new(j, J_NUM_SHARDS, json_integer(in->num_shards));
    json_object_set_new(j, J_CREATED, json_integer(in->time_created));
    json_object_set_new(j, J_UPDATED, json_integer(in->time_updated));
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, INDEX_FILE);
 
    FILE *fp = fopen(path, "w");
    if (fp) {
        char *jstr = json_dumps(j, JSON_PRESERVE_ORDER|JSON_INDENT(4));
        fprintf(fp, "%s", jstr);
        free(jstr);
        fclose(fp);
    } else {
        M_ERR("Failed to store index info for %s/%s", in->app->name, in->name);
    }
}

/* This tries to load index information regarding shards and creation / update times
 * If none exists, dumps the current information in a index into the file.  The latter
 * happens when this is a newly created index */
static void index_update_info(struct index *in) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, INDEX_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_object(json)) {
        in->num_shards = json_number_value(json_object_get(json, J_NUM_SHARDS));
        in->time_created = json_number_value(json_object_get(json, J_CREATED));
        in->time_updated = json_number_value(json_object_get(json, J_UPDATED));
        json_decref(json);
        return;
    } else {
        index_save_info(in);
    }
}

/* Object / Objects are being added to the index.  The request can either
 * be a single object or an array of objects.  We never try to update the
 * index right away.  A job is created and added to the job queue.  The
 * jobqueue gets processed serially one at at time */
static char *index_data_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, JSON_ALLOW_NUL, &error);
    if (!j) return strdup(J_FAILURE);
    struct in_job *job = in_job_new(in, JOB_ADD);
    job->j = j;
    index_add_job(in, job);
    M_DBG("Index job added type %d count %u", job->type, in->job_count);
    return strdup(J_SUCCESS);
}

static json_t *index_get_info_json(struct index *in) {
    json_t *j = json_object();
    json_object_set_new(j, J_NAME, json_string(in->name));
    json_object_set_new(j, J_NUM_SHARDS, json_integer(in->num_shards));
    json_object_set_new(j, J_NUM_JOBS, json_integer(in->job_count));
    return j;
}

static char *index_get_settings_callback(h2o_req_t *req, void *data) {
    return strdup(J_SUCCESS);
}

static char *index_save_settings_callback(h2o_req_t *req, void *data) {
    return strdup(J_SUCCESS);
}

static char *index_info_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    json_t *j = index_get_info_json(in);
    char *response = json_dumps(j, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(j);
    return response;
}

const struct api_path apipaths[] = {
    // Set Settings
    {"POST", URL_SETTINGS, KA_S_CONFIG, index_save_settings_callback},
    // Get current settings for this index
    {"GET", URL_SETTINGS, KA_G_CONFIG, index_get_settings_callback},
    // Lets you add new objects to this index
    {"POST", NULL, KA_ADD, index_data_callback},
    // Query get
    {"GET", URL_INFO, KA_QUERY|KA_BROWSE, index_info_callback},
    // Done here
    {"", "", KA_NONE, NULL}
    /*
    // Browse
    {"GET", NULL, KA_BROWSE, index_browse_callback},
    // Post data
    {"POST", NULL, KA_ADD, index_data_callback},
    // Get Settings
    {"GET", URL_SETTINGS, KA_G_CONFIG, index_get_settings_callback},
    // Set Settings
    {"POST", URL_SETTINGS, KA_S_CONFIG, index_save_settings_callback},
    // Reindex
    {"POST", URL_REINDEX, KA_S_CONFIG, index_reindex_callback},
    // Mapping
    {"GET", URL_MAPPING, KA_G_CONFIG, index_mapping_callback},
    // Index Mapping
    {"GET", URL_INDEX_MAPPING, KA_G_CONFIG, index_index_mapping_callback},
    // Query get
    // {"GET", URL_QUERY, KA_QUERY, index_query_callback},
    // Query post
    {"POST", URL_QUERY, KA_QUERY, index_json_query_callback},
    // Clear datastore
    {"POST", URL_CLEAR, KA_DELETE, index_clear_callback},
    // Bulk
    {"POST", URL_BULK, KA_DELETE|KA_ADD|KA_UPDATE, index_bulk_callback},
    // Object manipulation
    // get object
    {"GET", URL_MULTI, KA_BROWSE, index_get_object_callback},
    // replace object
    {"PUT", URL_MULTI, KA_UPDATE, index_replace_object_callback},
    // update object
    {"PATCH", URL_MULTI, KA_UPDATE, index_update_object_callback},
    // delete object
    {"DELETE", URL_MULTI, KA_DELETE, index_delete_object_callback},
    {"", "", KA_NONE, NULL}
    */
};

/* Applies a given key to the index, this registers all necessary callbacks for this key
 * and handles permissions */
void index_apply_key(struct index *in, struct key *k, KEY_ACCESS access) {
    char path[PATH_MAX];

    int idx = 0;
    while(1) {
        const struct api_path *ap = &apipaths[idx];
        if (ap->api_cb == NULL) break;
        if (ap->path) {
            sprintf(path, "%s/%s/%s", URL_INDEXES, in->name, ap->path);
        } else {
            sprintf(path, "%s/%s", URL_INDEXES, in->name);
        }
        register_api_callback(in->app->appid, k->apikey, ap->method,
             path, url_cbdata_new((access & ap->access)?ap->api_cb:api_forbidden, in));
        idx++;
    }
}

/* Revokes a key for an index */
// NOTE: index_apply_key, index_delete_key, index_register_handlers all 3 need to be updated
void index_delete_key(struct index *in, struct key *k) {
    char path[PATH_MAX];
    int idx = 0;
    while(1) {
        const struct api_path *ap = &apipaths[idx];
        if (ap->api_cb == NULL) break;
        if (ap->path) {
            sprintf(path, "%s/%s/%s", URL_INDEXES, in->name, ap->path);
        } else {
            sprintf(path, "%s/%s", URL_INDEXES, in->name);
        }
        deregister_api_callback(in->app->appid, k->apikey, ap->method, path);
        idx++;
    }
}

/* Handles registering all handlers for this index, this is only to be used for app keys */
// NOTE: index_apply_key, index_delete_key, index_register_handlers all 3 need to be updated
static void setup_index_handlers(struct index *in, const char *appid, const char *apikey, bool reg) {
    char path[PATH_MAX];
    int idx = 0;
    while(1) {
        const struct api_path *ap = &apipaths[idx];
        if (ap->api_cb == NULL) break;
        if (ap->path) {
            sprintf(path, "%s/%s/%s", URL_INDEXES, in->name, ap->path);
        } else {
            sprintf(path, "%s/%s", URL_INDEXES, in->name);
        }
        if (reg) {
            register_api_callback(appid, apikey, ap->method, path, url_cbdata_new(ap->api_cb, in));
        } else {
            deregister_api_callback(appid, apikey, ap->method, path);
        }
        idx++;
    }
}


/* Creates a new index or loads existing index */
struct index *index_new(const char *name, struct app *a, int num_shards) {
    struct index *in = calloc(1, sizeof(struct index));
    snprintf(in->name, sizeof(in->name), "%s", name);
    in->app = a;
    in->num_shards = num_shards;
    in->time_created = get_utc_seconds();
    in->fctx = flakeid_ctx_create_with_spoof(NULL);
    kv_init(in->shards);


    // Create index path if not present
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, in->name);
    mkdir(path, 0775);
    // Update stored index information if any
    index_update_info(in);

    // NOTE : Only after updating information do we have the correct number of shards
    // TODO: Properly handle shards when more than 1 node is present and all shards
    // of an index may not be on the same node
    // For now it is just shards by id.. so load it
    for (int i = 0; i < in->num_shards; i++) {
        struct shard *s = shard_new(in, i);
        kv_push(struct shard*, in->shards, s);
    }

    // Register the handlers for the app
    setup_index_handlers(in, a->appid, a->apikey, true);

    return in;
}

void index_free(struct index *in) {
    setup_index_handlers(in, in->app->appid, in->app->apikey, false);
    flakeid_ctx_destroy(in->fctx);
}

