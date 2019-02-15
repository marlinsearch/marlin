#ifndef _MARLIN_H
#define _MARLIN_H
#include "common.h"
#include "platform.h"
#include "kvec.h"
#include "app.h"
#include "threadpool.h"

struct marlin {
    char appid[APPID_SIZE+1];
    char apikey[APIKEY_SIZE+1];
    char db_path[PATH_MAX];
    char ssl_cert[PATH_MAX];
    char ssl_key[PATH_MAX];
    int  num_threads;
    int  num_processors;
    int  port;
    bool https;
    kvec_t(struct app *) apps;
};

extern struct marlin *marlin;
extern threadpool_t *index_pool;

void load_settings(const char *settings_path);
void init_marlin(void);
void shutdown_marlin(void);

#endif
