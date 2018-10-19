#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <jansson.h>

#include "app.h"
#include "marlin.h"
#include "url.h"
#include "api.h"
#include "utils.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

/* NOTE: This gets called every 5 seconds */
static void app_timer_callback(h2o_timeout_entry_t *entry) {
    M_DBG("App timeout!");
    struct app_timeout *at = (struct app_timeout *)entry;
    struct app *a = at->app;
    // restart the timer
    h2o_timeout_link(g_h2o_ctx->loop, &a->timeout, &a->timeout_entry.te);
}

/* sets up a periodic timer which fires every 5 seconds */
static void app_setup_timeouts(struct app *a) {
    h2o_timeout_init(g_h2o_ctx->loop, &a->timeout, APP_TIMER_SECS*1000);
    a->timeout_entry.te.cb = app_timer_callback;
    a->timeout_entry.app = a;
    h2o_timeout_link(g_h2o_ctx->loop, &a->timeout, &a->timeout_entry.te);
}

/* Converts the list of indexes to json format, both for api usage and for saving index info*/
static char *index_list_to_json(struct app *a) {
    json_t *ja = json_array();
    for (int i = 0; i < kv_size(a->indexes); i++) {
        struct index *in = kv_A(a->indexes, i);
        json_t *jo = json_object();
        json_object_set_new(jo, J_NAME, json_string(in->name));
        json_object_set_new(jo, J_CREATED, json_integer(in->time_created));
        json_object_set_new(jo, J_UPDATED, json_integer(in->time_updated));
        json_object_set_new(jo, J_NUM_SHARDS, json_integer(in->num_shards));
        json_array_append_new(ja, jo);
    }
    char *resp = json_dumps(ja, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(ja);
    return resp;
}

static char *list_indexes_handler(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    return index_list_to_json(a);
}

/* Creates an index after validations.  Returns NULL on failure to create */
// TODO: proper error handling, let the user know what went wrong while creating index
static struct index *create_index_from_json(struct app *a, struct json_t *j) {
    const char *name = json_string_value(json_object_get(j, J_NAME));

    // Make sure name exists and it is valid
    // TODO: validate name.. no spaces weird chars etc..,
    if (!name) return NULL;
    if (strlen(name) > MAX_INDEX_NAME) return NULL;

    // Make sure index does not exist already
    for (int i = 0; i < kv_size(a->indexes); i++) {
        struct index *in = kv_A(a->indexes, i);
        if (strcmp(in->name, name) == 0) return NULL;
    }

    // It does not matter what the value is for existing indices as the value
    // is stored in the index json file
    int num_shards = json_number_value(json_object_get(j, J_NUM_SHARDS));
    if (!num_shards) {
        num_shards = DEFAULT_NUM_SHARDS;
    }

    // We are good to go, create index now
    struct index *in = index_new(name, a, num_shards);
    uint64_t created = json_number_value(json_object_get(j, J_CREATED));
    if (!created) created = get_utc_seconds();
    in->time_created = created;
    kv_push(struct index *, a->indexes, in);
    return in;
}

void app_save_indexes(struct app *a) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, INDEXES_FILE);
    FILE *f = fopen(path, "w");
    if (!f) {
        M_ERR("Failed to save indexes file");
        return;
    }
    char *s = index_list_to_json(a);
    fprintf(f, "%s", s);
    free(s);
    fclose(f);
}

static char *create_index_handler(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);
    if (j) {
        struct index *in = create_index_from_json(a, j);
        if (!in) goto jerror;
        M_INFO("Created index %s", in->name);
        json_decref(j);
        app_save_indexes(a);
        return strdup(J_SUCCESS);
    }
jerror:
    json_decref(j);
    return api_bad_request(req);
}

/* Loads the indexes file and loads each index one by one
 * Just ignores what it cannot load for now */
static void app_load_indexes(struct app *c) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, c->name, INDEXES_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_array(json)) {
        size_t objid;
        json_t *obj;
        json_array_foreach(json, objid, obj) {
            create_index_from_json(c, obj);
        }
        json_decref(json);
    }
}

/* Deletes an index within an app and updates the indexes file */
bool app_delete_index(struct app *a, struct index *in) {
    for (int i=0; i < kv_size(a->indexes); i++) {
        if (kv_A(a->indexes, i) == in) {
            kv_del(struct index *, a->indexes, i);
            index_delete(in);
            app_save_indexes(a);
            return true;
        }
    }
    return false;
}

/* Creates a new app or loads an existing app */
struct app *app_new(const char *name, const char *appid, const char *apikey) {
    struct app *a = calloc(1, sizeof(struct app));
    snprintf(a->name, sizeof(a->name), "%s", name);
    snprintf(a->appid, sizeof(a->appid), "%s", appid);
    snprintf(a->apikey, sizeof(a->apikey), "%s", apikey);

    // Create app path if not present
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", marlin->db_path, a->name);
    mkdir(path, 0775);
 
    // Register app api handlers
    register_api_callback(a->appid, a->apikey, "POST", 
                          URL_INDEXES, url_cbdata_new(create_index_handler, a));
    register_api_callback(a->appid, a->apikey, "GET", 
                          URL_INDEXES, url_cbdata_new(list_indexes_handler, a));
 
    kv_init(a->indexes);
    app_setup_timeouts(a);

    // Load all indexes managed by the app after everything else is complete
    app_load_indexes(a);
    return a;
}

void app_free(struct app *a) {
    // Deregister app api handlers
    deregister_api_callback(a->appid, a->apikey, "POST", URL_INDEXES);
    deregister_api_callback(a->appid, a->apikey, "GET", URL_INDEXES);
    // free the indices
    for (int i=0; i<kv_size(a->indexes); i++) {
        index_free(kv_A(a->indexes, i));
    }
    kv_destroy(a->indexes);
    // remove linked timeouts
    h2o_timeout_unlink(&a->timeout_entry.te);
    h2o_timeout_dispose(g_h2o_ctx->loop, &a->timeout);
    // finally free the app
    free(a);
}

void app_delete(struct app *a) {
    // Save the app path
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", marlin->db_path, a->name);
    // and indexes file path
    char indexes_path[PATH_MAX];
    snprintf(indexes_path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, INDEXES_FILE);
    // Delete all indexes
    for (int i=0; i<kv_size(a->indexes); i++) {
        index_delete(kv_A(a->indexes, i));
    }
    kv_size(a->indexes) = 0;
    // Free the app, which destroys the indexes kvec and handles the rest of the free
    app_free(a);
    unlink(indexes_path);
    rmdir(path);
}

