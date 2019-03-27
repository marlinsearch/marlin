#ifndef _API_H
#define _API_H
#include <h2o.h>
#include "khash.h"

#define M_APP_ID "x-marlin-application-id"
#define M_APP_ID_LEN 23
#define M_API_KEY "x-marlin-rest-api-key"
#define M_API_KEY_LEN 21

extern h2o_context_t *g_h2o_ctx;

struct url_cbdata {
    char *(*cb)(h2o_req_t *req, void *);
    void *data;
};

KHASH_MAP_INIT_STR(URL_CBDATA, struct url_cbdata *)

void init_api(void);
void *run_loop(void *thread_index);


struct url_cbdata *url_cbdata_new(char *(*cb)(h2o_req_t *, void *), void *data);
void register_api_callback(const char *appid, const char *apikey, const char *method, 
        const char *url, struct url_cbdata *cbdata);
void deregister_api_callback(const char *appid, const char *apikey, const char *method, 
        const char *url);
char *api_forbidden(h2o_req_t *req, void *data);
char *api_bad_request(h2o_req_t *req);
char *api_not_found(h2o_req_t *req);
char *api_success(h2o_req_t *req);

#endif

