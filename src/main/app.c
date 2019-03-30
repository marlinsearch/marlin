#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <jansson.h>

#include "app.h"
#include "marlin.h"
#include "url.h"
#include "api.h"
#include "utils.h"
#include "keys.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

const KEY_ACCESS permission[] = {
    KA_NONE,
    KA_ADD,
    KA_DELETE,
    KA_UPDATE,
    KA_ADD_INDEX,
    KA_DEL_INDEX,
    KA_BROWSE,
    KA_G_CONFIG,
    KA_S_CONFIG,
    KA_ANALYTICS,
    KA_LIST_INDEX,
    KA_QUERY,
};

const char *permission_str[] = {
    "none",
    "addDoc",
    "delDoc",
    "updateDoc",
    "addIndex",
    "delIndex",
    "browseDoc",
    "getConfig",
    "setConfig",
    "analytics",
    "listIndex",
    "queryIndex",
};


static void app_free_job(struct app *c) {
    // job has been added
    // Process only one free job every timeout.  We are not in a hurry to
    // free
    // TODO: make sure APP_TIMER_SECS has passed before free
    WRLOCK(&c->free_lock);
    if (!c->fjob_head) goto unlock;
    struct free_job *fj = c->fjob_head;
    // Incase this is the last job..
    if (c->fjob_head == c->fjob_tail) {
        c->fjob_head = c->fjob_tail = NULL;
    } else {
        // Else, move the head
        c->fjob_head = fj->next;
    }
    // Do the actual free
    switch(fj->type) {
        case FREE_TRIE:
            dtrie_free(fj->ptr_to_free);
            break;
        case FREE_BMAP:
            bmap_free(fj->ptr_to_free);
            break;
    }
    free(fj);
unlock:
    UNLOCK(&c->free_lock);
}

/* NOTE: This gets called every 5 seconds */
static void app_timer_callback(h2o_timeout_entry_t *entry) {
    M_DBG("App timeout!");
    struct app_timeout *at = (struct app_timeout *)entry;
    struct app *a = at->app;
    app_free_job(a);
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

static int lookup_permission(const char *p) {
    if (!p) return -1;
    for (int i=0; i<=NUM_PERMS; i++) {
        if (strcmp(permission_str[i], p) == 0) return i;
    }
    return -1;
}

static void key_free_indexes(struct key *k) {
    for (int i=0; i<kv_size(k->indexes); i++) {
        char *dsname = kv_A(k->indexes, i);
        free(dsname);
    }
    kv_destroy(k->indexes);
}

static void key_free(struct key *k) {
    key_free_indexes(k);
    free(k);
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
        app_index_apply_allkeys(a, in);
        app_save_indexes(a);
        return api_success(req);
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
        size_t id;
        json_t *j;
        json_array_foreach(json, id, j) {
            create_index_from_json(c, j);
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

static json_t *key_to_json(const struct key *k) {
    json_t *j = json_object();
    json_object_set_new(j, J_APIKEY, json_string(k->apikey));
    json_object_set_new(j, J_DESCRIPTION, json_string(k->description));
    json_t *ja = json_array();
    for (int i=0; i<=NUM_PERMS; i++) {
        if (k->access & permission[i]) {
            json_array_append_new(ja, json_string(permission_str[i]));
        }
    }
    json_object_set_new(j, J_PERMISSIONS, ja);
    json_t *jd = json_array();
    for (int i=0; i<kv_size(k->indexes); i++) {
        json_array_append_new(jd, json_string(kv_A(k->indexes, i)));
    }
    json_object_set_new(j, J_INDEXES, jd);
    return j;
}

static char *app_keys_to_json(const struct app *a) {
    json_t *ja = json_array();
    for (int i=0; i<kv_size(a->keys); i++) {
        json_array_append_new(ja, key_to_json(kv_A(a->keys, i)));
    }
    char *resp = json_dumps(ja, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(ja);
    return resp;
}

static void app_save_keys(const struct app *a) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, KEYS_FILE);
    FILE *f = fopen(path, "w");
    if (!f) {
        M_ERR("Failed to save application keys for %s\n", a->name);
        return;
    }
    char *s = app_keys_to_json(a);
    fprintf(f, "%s", s);
    free(s);
    fclose(f);
}

static void app_index_apply_key(struct app *a, struct key *k, struct index *in) {
    // If indexes are configured in the key, only indexes mapped to the key get access
    if (kv_size(k->indexes)) {
        bool applied = false;
        for (int i=0; i<kv_size(k->indexes); i++) {
            const char *index_name = kv_A(k->indexes, i);
            if ((strcmp(in->name, index_name) == 0)) {
                index_apply_key(in, k, k->access);
                applied = true;
                break;
            }
        }
        // The index was not in key list, map to forbidden
        if (!applied) {
            index_apply_key(in, k, KA_NONE);
        }
    } else {
        // If no indexes are configured for the key, it applies for everybody
        index_apply_key(in, k, k->access);
    }
}

void app_index_apply_allkeys(struct app *a, struct index *in) {
    for (int i=0; i<kv_size(a->keys); i++) {
        struct key *k = kv_A(a->keys, i);
        app_index_apply_key(a, k, in);
    }
}

static void app_apply_key(struct app *a, struct key *k) {
    // Register app related api callbacks
    register_api_callback(a->appid, k->apikey, "POST", 
            URL_INDEXES, url_cbdata_new((k->access & KA_ADD_INDEX)?create_index_handler:api_forbidden, a));
    register_api_callback(a->appid, k->apikey, "GET", 
            URL_INDEXES, url_cbdata_new((k->access & KA_LIST_INDEX)?list_indexes_handler:api_forbidden, a));
    // Else apply to only those that match
    for (int i=0; i<kv_size(a->indexes); i++) {
        struct index *in= kv_A(a->indexes, i);
        app_index_apply_key(a, k, in);
    }
}

static struct key *load_key_from_json(struct app *a, json_t *j) {
    const char *apikey = json_string_value(json_object_get(j, J_APIKEY));
    KEY_ACCESS perm = KA_NONE;
    // API key is a must.. bail out if not present
    if (!apikey) return NULL;

    // Get permissions
    json_t *jp = json_object_get(j, J_PERMISSIONS);
    if (jp) {
        // If permissions is present, make sure it is an array
        if (!json_is_array(jp)) {
            return NULL;
        }
        size_t objid;
        json_t *obj;
        json_array_foreach(jp, objid, obj) {
            int pos = lookup_permission(json_string_value(obj));
            // We failed to lookup this permission
            if (pos < 0) return NULL;
            perm |= permission[pos];
        }
    }
    struct key *k = calloc(1, sizeof(struct key));
    snprintf(k->apikey, sizeof(k->apikey), "%s", apikey);
    k->access = perm;

    // Read description for key
    const char *desc = json_string_value(json_object_get(j, J_DESCRIPTION));
    if (desc) {
        snprintf(k->description, sizeof(k->description), "%s", desc);
    }

    // Is the key restricted to some indices?
    kv_init(k->indexes);
    json_t *jd = json_object_get(j, J_INDEXES);
    if (jd) {
        // If indexes is present, make sure it is an array
        if (!json_is_array(jd)) {
            key_free(k);
            return NULL;
        }
        size_t objid;
        json_t *obj;
        json_array_foreach(jd, objid, obj) {
            if (!json_is_string(obj)) {
                key_free(k);
                return NULL;
            }
            kv_push(char *, k->indexes, strdup(json_string_value(obj)));
        }
    }
    kv_push(struct key *, a->keys, k);
    app_apply_key(a, k);
    return k;
}

static char *create_key(h2o_req_t *req, void *data) {
    struct app *a = (struct app*) data;
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);
    if (j) {
        char apikey[APIKEY_SIZE+1];
        random_str(apikey, APIKEY_SIZE);
        json_object_set_new(j, J_APIKEY, json_string(apikey));
        struct key *k = load_key_from_json(a, j);
        if (!k) goto jerror;
        char *ret = malloc(PATH_MAX);
        snprintf(ret, PATH_MAX, "{\"%s\":\"%s\"}", J_APIKEY, apikey);
        json_decref(j);
        app_save_keys(a);
        return ret;
    }
jerror:
    json_decref(j);
    return api_bad_request(req);
}

static struct key *parse_req_key(h2o_req_t *req, struct app *a) {
    char key[APIKEY_SIZE+1];
    key[0] = '\0';
    int path_len = req->query_at < req->path.len ? req->query_at: req->path.len;
    int start = path_len;
    while (start > 3) {
        start--;
        if (req->path.base[start] == '/') {
            if ((path_len - start) != (APIKEY_SIZE+1)) {
                key[0] = '\0';
            } else {
                memcpy(key, &req->path.base[start+1], path_len-1-start);
                key[path_len-start-1] = '\0';
            }
            break;
        }
    }
    if (key[0] == '\0') return NULL;
    for (int i=0; i<kv_size(a->keys); i++) {
        struct key *k = kv_A(a->keys, i);
        if (strcmp(k->apikey, key) == 0) {
            return k;
        }
    }
    return NULL;
}

static char *dump_key(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    struct key *k = parse_req_key(req, a);
    if (!k) {
        return api_not_found(req);
    } else {
        json_t *jk = key_to_json(k);
        char *resp = json_dumps(jk, JSON_PRESERVE_ORDER|JSON_INDENT(4));
        json_decref(jk);
        return resp;
    }
}

static char *update_key(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    struct key *k = parse_req_key(req, a);
    if (!k) {
        return api_not_found(req);
    } else {
        json_error_t error;
        json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);
        if (j) {
            KEY_ACCESS perm = KA_NONE;
            // Get permissions
            json_t *jp = json_object_get(j, J_PERMISSIONS);
            if (jp) {
                // If permissions is present, make sure it is an array
                if (!json_is_array(jp)) {
                    return api_bad_request(req);
                }
                size_t objid;
                json_t *obj;
                json_array_foreach(jp, objid, obj) {
                    int pos = lookup_permission(json_string_value(obj));
                    // We failed to lookup this permission
                    if (pos < 0) return api_bad_request(req);
                    perm |= permission[pos];
                }
            }
            // Validate indexes is ok
            json_t *jd = json_object_get(j, J_INDEXES);
            if (jd) {
                // If indexes is present, make sure it is an array
                if (!json_is_array(jd)) {
                    return api_bad_request(req);
                }
                size_t objid;
                json_t *obj;
                json_array_foreach(jd, objid, obj) {
                    if (!json_is_string(obj)) {
                        return api_bad_request(req);
                    }
                }
            }
            // Free datastores if any
            jd = json_object_get(j, J_INDEXES);
            if (jd) {
                key_free_indexes(k);
                kv_init(k->indexes);
                size_t objid;
                json_t *obj;
                json_array_foreach(jd, objid, obj) {
                    kv_push(char *, k->indexes, strdup(json_string_value(obj)));
                }
            }
            k->access = perm;
            const char *desc = json_string_value(json_object_get(j, J_DESCRIPTION));
            if (desc) {
                snprintf(k->description, sizeof(k->description), "%s", desc);
            }
            app_apply_key(a, k);
            app_save_keys(a);
            json_decref(j);
            return api_success(req);
        } else {
            return api_bad_request(req);
        }
    }
}

static void app_delete_key(struct app *a, struct key *k) {
    // Deregister app related api callbacks
    deregister_api_callback(a->appid, k->apikey, "POST", URL_INDEXES);
    deregister_api_callback(a->appid, k->apikey, "GET", URL_INDEXES);
    // Else apply to only those that match
    for (int i=0; i<kv_size(a->indexes); i++) {
        struct index *in = kv_A(a->indexes, i);
        index_delete_key(in, k);
    }
    for (int i=0; i<kv_size(a->keys); i++) {
        if (kv_A(a->keys, i) == k) {
            kv_del(struct key *, a->keys, i);
            break;
        }
    }
}

static char *delete_key(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    struct key *k = parse_req_key(req, a);
    if (!k) {
        return api_not_found(req);
    } else {
        app_delete_key(a, k);
        key_free(k);
        app_save_keys(a);
        return api_success(req);
    }
}

static char *list_keys(h2o_req_t *req, void *data) {
    struct app *a = (struct app *) data;
    return app_keys_to_json(a);
}

static void app_load_keys(struct app *a) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, KEYS_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_array(json)) {
        size_t objid;
        json_t *obj;
        json_array_foreach(json, objid, obj) {
            load_key_from_json(a, obj);
        }
        json_decref(json);
    }
    // Register key specific urls to update / dump / delete
    sprintf(path, "%s/%s", URL_KEYS, URL_MULTI);
    register_api_callback(a->appid, a->apikey, "GET",
             path, url_cbdata_new(dump_key, a));
    register_api_callback(a->appid, a->apikey, "POST",
             path, url_cbdata_new(update_key, a));
    register_api_callback(a->appid, a->apikey, "DELETE",
             path, url_cbdata_new(delete_key, a));
}

void app_add_freejob(struct app *a, FREE_JOB_TYPE type, void *ptr) {
    struct free_job *fj = calloc(1, sizeof(struct free_job));
    fj->type = type;
    fj->ptr_to_free = ptr;
    // Start time
    gettimeofday(&fj->time_added, NULL);
    WRLOCK(&a->free_lock);
    // If nothing exists, add it and set head and tail to fj
    if (!a->fjob_head) {
        a->fjob_head = fj;
        a->fjob_tail = fj;
    } else if (a->fjob_tail) {
        a->fjob_tail->next = fj;
        a->fjob_tail = fj;
    }
    UNLOCK(&a->free_lock);
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
    register_api_callback(a->appid, a->apikey, "POST", 
                          URL_KEYS, url_cbdata_new(create_key, a));
    register_api_callback(a->appid, a->apikey, "GET", 
                          URL_KEYS, url_cbdata_new(list_keys, a));

    kv_init(a->indexes);
    INITLOCK(&a->free_lock);
    app_setup_timeouts(a);

    // Load all indexes managed by the app after everything else is complete
    app_load_indexes(a);
    // Load all keys for this app
    app_load_keys(a);
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

    // Free the keys
    for (int i=0; i<kv_size(a->keys); i++) {
        free(kv_A(a->keys, i));
    }
    kv_destroy(a->keys);
 
    // remove linked timeouts
    h2o_timeout_unlink(&a->timeout_entry.te);
    h2o_timeout_dispose(g_h2o_ctx->loop, &a->timeout);

    // Handle free jobs
    while (a->fjob_head) {
        app_free_job(a);
    }
    DESTROYLOCK(&a->free_lock);
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
    char keys_path[PATH_MAX];
    snprintf(keys_path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, KEYS_FILE);
    // Delete all indexes
    for (int i=0; i<kv_size(a->indexes); i++) {
        index_delete(kv_A(a->indexes, i));
    }
    kv_size(a->indexes) = 0;
    // Free the app, which destroys the indexes kvec and handles the rest of the free
    app_free(a);
    unlink(indexes_path);
    unlink(keys_path);
    rmdir(path);
}

