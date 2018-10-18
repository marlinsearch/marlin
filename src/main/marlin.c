#include <jansson.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "marlin.h"
#include "utils.h"
#include "url.h"
#include "api.h"
#include "analyzer.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

struct marlin *marlin;
threadpool_t *index_pool;


// Load marlin settings
void load_settings(const char *settings_path) {
    if (!marlin) {
        marlin = calloc(1, sizeof(struct marlin));
    }
    json_error_t error;
    json_t *js = json_load_file(settings_path, 0, &error);
    if (!js) {
        M_ERR("Failed to load settings, exiting !");
        exit(EXIT_FAILURE);
    }
    const char *appid = json_string_value(json_object_get(js, MASTER_APPID));
    const char *apikey = json_string_value(json_object_get(js, MASTER_APIKEY));
    const char *db_path = json_string_value(json_object_get(js, DB_LOCATION));
    const char *ssl_cert = json_string_value(json_object_get(js, SSL_CERT));
    const char *ssl_key = json_string_value(json_object_get(js, SSL_KEY));
    if (appid && apikey && db_path && ssl_cert && ssl_key) {
        snprintf(marlin->appid, sizeof(marlin->appid), "%s", appid);
        snprintf(marlin->apikey, sizeof(marlin->apikey), "%s", apikey);
        snprintf(marlin->db_path, sizeof(marlin->db_path), "%s", db_path);
        snprintf(marlin->ssl_cert, sizeof(marlin->ssl_cert), "%s", ssl_cert);
        snprintf(marlin->ssl_key, sizeof(marlin->ssl_key), "%s", ssl_key);
    } else {
        M_ERR("Missing settings, exiting !");
        exit(EXIT_FAILURE);
    }
    marlin->port = 9002;
    marlin->https = true;
    if (json_object_get(js, PORT)) {
        marlin->port = json_integer_value(json_object_get(js, PORT));
    }
    if (json_object_get(js, HTTPS)) {
        marlin->https = json_boolean_value(json_object_get(js, HTTPS));
    }
    if (json_object_get(js, NUMTHREADS)) {
        marlin->num_threads = json_integer_value(json_object_get(js, NUMTHREADS));
    } else {
        marlin->num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    }
    index_pool = threadpool_create(8, 64, 0);
    json_decref(js);
}

static char *marlin_handler(h2o_req_t *req, void *data) {
    // TODO: add and use version from version.h
    return strdup("{\"success\":true, \"version\":\"0.1\"}");
}

static char *app_list_to_json(void) {
    json_t *ja = json_array();
    for (int i=0; i<kv_size(marlin->apps); i++) {
        struct app *a = kv_A(marlin->apps, i);
        json_t *jo = json_object();
        json_object_set_new(jo, J_NAME, json_string(a->name));
        json_object_set_new(jo, J_APPID, json_string(a->appid));
        json_object_set_new(jo, J_APIKEY, json_string(a->apikey));
        json_object_set_new(jo, J_NUM_INDEXES, json_integer(kv_size(a->indexes)));
        json_array_append_new(ja, jo);
    }
    char *resp = json_dumps(ja, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(ja);
    return resp;
}

/* Creates / updates the app list file */
static void save_apps(void) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", marlin->db_path, APPS_FILE);
    FILE *f = fopen(path, "w");
    if (!f) {
        M_ERR("Failed to save app info");
        return;
    }
    char *app_str = app_list_to_json();
    fprintf(f, "%s", app_str);
    free(app_str);
    fclose(f);
}

/* API handler to delete an application */
static char *delete_app_handler(h2o_req_t *req, void *data) {
    struct app *a = (struct app *)data;
    for (int i=0; i < kv_size(marlin->apps); i++) {
        if (kv_A(marlin->apps, i) == data) {
            kv_del(struct app *, marlin->apps, i);
            // De-register callback
            char urlapp[PATH_MAX];
            sprintf(urlapp, "%s/%s", URL_APPS, a->name);
            // Remove handler to get app
            deregister_api_callback(marlin->appid, marlin->apikey, "GET", urlapp);
            // remove the handler for deletion
            deregister_api_callback(marlin->appid, marlin->apikey, "DELETE", urlapp);
            // Delete app
            app_delete(a);
            break;
        }
    }
    save_apps();
    // TODO: Return app information and when it was deleted
    return strdup(J_SUCCESS);
}

/* API handler to get an application */
static char *get_app_handler(h2o_req_t *req, void *data) {
    struct app *a = (struct app *)data;
    json_t *jo = json_object();
    json_object_set_new(jo, J_NAME, json_string(a->name));
    json_object_set_new(jo, J_APPID, json_string(a->appid));
    json_object_set_new(jo, J_APIKEY, json_string(a->apikey));
    char *resp = json_dumps(jo, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(jo);
    return resp;
}

/* Called to create an app either when called by api or when Marlin is restarted
 * and the apps are loaded from applications file */
static int create_app_from_json(struct json_t *j) {
    const char *name = json_string_value(json_object_get(j, J_NAME));
    const char *appid = json_string_value(json_object_get(j, J_APPID));
    const char *apikey = json_string_value(json_object_get(j, J_APIKEY));

    // TODO: Return proper error codes and let the user know what the problem is
    if (!(name && appid && apikey)) return -1;
    if (strlen(appid) != APPID_SIZE) return -1;
    if (strlen(apikey) != APIKEY_SIZE) return -1;
    M_DBG("Creating application %s %s %s", name, appid, apikey);

    // Make sure app does not exist already
    for (int i = 0; i < kv_size(marlin->apps); i++) {
        struct app *a = kv_A(marlin->apps, i);
        if (strcmp(a->name, name) == 0) return -1;
        if (strcmp(a->appid, appid) == 0) return -1;
    }

    // Create / load the application and add it to our list of apps
    struct app *a = app_new(name, appid, apikey);
    kv_push(struct app *, marlin->apps, a);

    // Register URL to delete application
    char urlapp[PATH_MAX];
    sprintf(urlapp, "%s/%s", URL_APPS, a->name);
    // Setup delete handler, only master can delete this
    register_api_callback(marlin->appid, marlin->apikey, "DELETE", 
            urlapp, url_cbdata_new(delete_app_handler, a));
    // Setup get handler, only master can get this
    register_api_callback(marlin->appid, marlin->apikey, "GET", 
            urlapp, url_cbdata_new(get_app_handler, a));
    return 0;
}


/**
 * Loads applications from the applications.json file in db
 * Creates a default application if nothing exists */
static void load_apps(void) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", marlin->db_path, APPS_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_array(json)) {
        size_t objid;
        json_t *obj;
        json_array_foreach(json, objid, obj) {
            create_app_from_json(obj);
        }
        json_decref(json);
    }
}

/* Handler which lists all applications */
static char *list_apps_handler(h2o_req_t *req, void *data) {
    return app_list_to_json();
}

/* API Handler to create a new application.  An appId and apiKey
 * can be specified or they will be automatically generated if required.
 * At the minimum an app name is required.  If not results in a badrequest */
static char *create_app_handler(h2o_req_t *req, void *data) {
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);

    //If an appid and apikey were part of the request, use that else generate
    if (!json_object_get(j, J_APPID)) {
        char appid[APPID_SIZE];
        random_str(appid, APPID_SIZE);
        json_object_set_new(j, J_APPID, json_string(appid));
    }

    if (!json_object_get(j, J_APIKEY)) {
        char apikey[APIKEY_SIZE];
        random_str(apikey, APIKEY_SIZE);
        json_object_set_new(j, J_APIKEY, json_string(apikey));
    }

    if (j) {
        int r = create_app_from_json(j);
        if (r < 0) goto jerror;
        save_apps();
        char *resp = json_dumps(j, JSON_PRESERVE_ORDER|JSON_INDENT(4));
        json_decref(j);
        return resp;
    }
jerror:
    // TODO: Better error message
    json_decref(j);
    req->res.status = 400;
    req->res.reason = "Bad Request";
    return strdup(J_FAILURE);
}

void init_marlin(void) {
    // Make sure setting have been loaded before initializing
    assert(marlin != NULL);

    // Create the db path if not present
    mkdir(marlin->db_path, 0775);

    // Initializations
    init_analyzers();

    // Setup applications, creating a default one if required
    kv_init(marlin->apps);
    load_apps();

    // Setup API handlers
    register_api_callback(marlin->appid, marlin->apikey, "GET",
                          URL_MARLIN, url_cbdata_new(marlin_handler, NULL));
    register_api_callback(marlin->appid, marlin->apikey, "GET", 
                          URL_APPS, url_cbdata_new(list_apps_handler, NULL));
    register_api_callback(marlin->appid, marlin->apikey, "POST", 
                          URL_APPS, url_cbdata_new(create_app_handler, NULL));

    M_INFO("Initialized %s on port %d", marlin->https?"https":"http", marlin->port);
}

void shutdown_marlin(void) {
    M_INFO("Shutting down !");
    threadpool_destroy(index_pool, 0);
    // Deregister callbacks
    deregister_api_callback(marlin->appid, marlin->apikey, "GET", URL_MARLIN);
    deregister_api_callback(marlin->appid, marlin->apikey, "GET", URL_APPS);
    deregister_api_callback(marlin->appid, marlin->apikey, "POST", URL_APPS);
    // Free apps
    for (int i = 0; i < kv_size(marlin->apps); i++) {
        struct app *a = kv_A(marlin->apps, i);
        app_free(a);
    }
    kv_destroy(marlin->apps);
    free(marlin);
}

