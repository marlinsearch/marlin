#ifndef __APP_H__
#define __APP_H__

#include "common.h"
#include "index.h"
#include "kvec.h"

#define MAX_APP_NAME 32

struct app {
    char name[MAX_APP_NAME];
    char appid[APPID_SIZE+1];
    char apikey[APIKEY_SIZE+1];

    kvec_t(struct index *) indexes; // The indices this app manages
};

struct app *app_new(const char *name, const char *appid, const char *apikey);
void app_free(struct app *a);

#endif
