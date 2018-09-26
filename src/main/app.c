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

/* Converts the list of indexes to json format, both for api usage and for saving index info*/
static char *index_list_to_json(struct app *a) {
    json_t *ja = json_array();
    for (int i = 0; i < kv_size(a->indexes); i++) {
        struct index *in = kv_A(a->indexes, i);
        json_t *jo = json_object();
        json_object_set_new(jo, J_NAME, json_string(in->name));
        json_object_set_new(jo, J_CREATED, json_integer(in->time_created));
        json_object_set_new(jo, J_UPDATED, json_integer(in->time_updated));
        json_array_append_new(ja, jo);
    }
    char *resp = json_dumps(ja, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(ja);
    return resp;
}

static char *list_indexes(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    return index_list_to_json(a);
}

/* Creates an index after validations.  Returns NULL on failure to create */
// TODO: proper error handling, let the user know what went wrong while creating index
static struct index *create_index_from_json(struct app *a, struct json_t *j) {
    const char *name = json_string_value(json_object_get(j, J_NAME));

    // Make sure name exists and it is valid
    if (!name) return NULL;
    if (strlen(name) > MAX_INDEX_NAME) return NULL;

    // Make sure index does not exist already
    for (int i = 0; i < kv_size(a->indexes); i++) {
        struct index *in = kv_A(a->indexes, i);
        if (strcmp(in->name, name) == 0) return NULL;
    }

    // We are good to go, create index now
    struct index *in = index_new(name, a);
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

static char *create_index(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);
    M_DBG("create index %s %s", req->entity.base, error.text);
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

/* Creates a new app or loads an existing app */
struct app *app_new(const char *name, const char *appid, const char *apikey) {
    struct app *a = malloc(sizeof(struct app));
    snprintf(a->name, sizeof(a->name), "%s", name);
    snprintf(a->appid, sizeof(a->appid), "%s", appid);
    snprintf(a->apikey, sizeof(a->apikey), "%s", apikey);

    // Create app path if not present
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", marlin->db_path, a->name);
    mkdir(path, 0775);
 
    // Register app api handlers
    register_api_callback(a->appid, a->apikey, "POST", 
                          URL_INDEXES, url_cbdata_new(create_index, a));
    register_api_callback(a->appid, a->apikey, "GET", 
                          URL_INDEXES, url_cbdata_new(list_indexes, a));
 
    kv_init(a->indexes);
    app_load_indexes(a);
    return a;
}

void app_free(struct app *a) {
    free(a);
}
