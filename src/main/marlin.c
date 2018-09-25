#include <jansson.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "marlin.h"
#include "url.h"
#include "api.h"

struct marlin *marlin;

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
    json_decref(js);
}

static char *marlin_handler(h2o_req_t *req, void *data) {
    // TODO: add and use version from version.h
    return strdup("{\"success\":true, \"version\":\"0.1\"}");
}

void init_marlin(void) {
    // Make sure setting have been loaded before initializing
    assert(marlin != NULL);

    // Create the db path if not present
    mkdir(marlin->db_path, 0644);

    // Setup API handlers
    register_api_callback(marlin->appid, marlin->apikey, "GET",
                          URL_MARLIN, url_cbdata_new(marlin_handler, NULL));
    /*
    register_api_callback(diya->appid, diya->apikey, "GET", 
                          URL_INT_CUSTOMER, url_cbdata_new(list_customers_handler, NULL));
    register_api_callback(diya->appid, diya->apikey, "POST", 
                          URL_INT_CUSTOMER, url_cbdata_new(create_customer_handler, NULL));
                          */

    M_INFO("Initialized %s on port %d", marlin->https?"https":"http", marlin->port);
}

void shutdown_marlin(void) {
    M_INFO("Shutting down !");
    // Deregister callbacks
    deregister_api_callback(marlin->appid, marlin->apikey, "GET", URL_MARLIN);
}

