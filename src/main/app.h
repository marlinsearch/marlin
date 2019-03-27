#ifndef __APP_H__
#define __APP_H__

#include <h2o.h>
#include <h2o/timeout.h>
#include "common.h"
#include "index.h"
#include "kvec.h"

#define MAX_APP_NAME    32
#define APP_TIMER_SECS  5

/* Used for periodic h2o timer */
struct app_timeout {
    h2o_timeout_entry_t te;
    void *app;
};

/* An app is a group of indices with its own appid / key and set of child keys */
struct app {
    char name[MAX_APP_NAME];
    char appid[APPID_SIZE+1];
    char apikey[APIKEY_SIZE+1];

    // Timeout handling, we need a period timer
    h2o_timeout_t timeout;
    struct app_timeout timeout_entry;
    kvec_t(struct key *) keys;

    // The indices this app manages
    kvec_t(struct index *) indexes;
};

struct app *app_new(const char *name, const char *appid, const char *apikey);
void app_free(struct app *a);
void app_delete(struct app *a);
bool app_delete_index(struct app *a, struct index *in);
void app_index_apply_allkeys(struct app *a, struct index *in);

#endif
