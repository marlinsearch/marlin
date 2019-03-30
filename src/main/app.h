#ifndef __APP_H__
#define __APP_H__

#include <h2o.h>
#include <h2o/timeout.h>
#include "common.h"
#include "index.h"
#include "kvec.h"

#define MAX_APP_NAME    32
#define APP_TIMER_SECS  5

typedef enum free_job_type {
    FREE_TRIE,
    FREE_BMAP,
} FREE_JOB_TYPE;


/* Used for periodic h2o timer */
struct app_timeout {
    h2o_timeout_entry_t te;
    void *app;
};

// A free job takes care of freeing stuff
// in a delayed manner.
struct free_job {
    FREE_JOB_TYPE type;
    void *ptr_to_free;
    struct timeval time_added;
    struct free_job *next;
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

    // Free job handling
    struct free_job *fjob_head;
    struct free_job *fjob_tail;
    pthread_rwlock_t free_lock;

    // The indices this app manages
    kvec_t(struct index *) indexes;
};

struct app *app_new(const char *name, const char *appid, const char *apikey);
void app_free(struct app *a);
void app_delete(struct app *a);
bool app_delete_index(struct app *a, struct index *in);
void app_index_apply_allkeys(struct app *a, struct index *in);
void app_add_freejob(struct app *a, FREE_JOB_TYPE type, void *ptr);

#endif
