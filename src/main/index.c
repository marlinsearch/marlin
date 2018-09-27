#include <h2o.h>
#include "index.h"
#include "utils.h"
#include "keys.h"
#include "url.h"
#include "api.h"
#include "platform.h"

// ALL API HANDLERS COME BELOW THIS

struct api_path {
    const char *method;
    const char *path;
    KEY_ACCESS access;
    char *(*api_cb)(h2o_req_t *req, void *data);
};

static void index_work_job(void *data) {
    struct in_job *job = data;
    struct index *in = job->index;
    if (in) {
        switch (job->type) {
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

static char *index_get_settings_callback(h2o_req_t *req, void *data) {
    return strdup(J_SUCCESS);
}

static char *index_save_settings_callback(h2o_req_t *req, void *data) {
    return strdup(J_SUCCESS);
}

const struct api_path apipaths[] = {
    // Set Settings
    {"POST", URL_SETTINGS, KA_S_CONFIG, index_save_settings_callback},
    // Get current settings for this index
    {"GET", URL_SETTINGS, KA_G_CONFIG, index_get_settings_callback},
    // Lets you add new objects to this index
    {"POST", NULL, KA_ADD, index_data_callback},
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
static void index_register_handlers(struct index *in, const char *appid, const char *apikey) {
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
        register_api_callback(appid, apikey, ap->method, path, url_cbdata_new(ap->api_cb, in));
        idx++;
    }
}

/* Creates a new index or loads existing index */
struct index *index_new(const char *name, struct app *a) {
    struct index *in = calloc(1, sizeof(struct index));
    snprintf(in->name, sizeof(in->name), "%s", name);
    in->app = a;
    in->time_created = get_utc_seconds();
    index_register_handlers(in, a->appid, a->apikey);

    return in;
}
