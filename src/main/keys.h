#ifndef __KEYS_H_
#define __KEYS_H_

#include "common.h"
#include "kvec.h"

// Permissions handled by key
typedef enum key_access {
    KA_NONE      = 0,
    KA_ADD       = 1 << 0,
    KA_DELETE    = 1 << 1,
    KA_UPDATE    = 1 << 2,
    KA_ADD_INDEX = 1 << 3,
    KA_DEL_INDEX = 1 << 4,
    KA_BROWSE    = 1 << 5,
    KA_G_CONFIG  = 1 << 6,
    KA_S_CONFIG  = 1 << 7,
    KA_ANALYTICS = 1 << 8,
    KA_LIST_INDEX= 1 << 9,
    KA_QUERY     = 1 << 10,
} KEY_ACCESS;
#define NUM_PERMS 11

// A key
struct key {
    char apikey[APIKEY_SIZE+1];
    char description[PATH_MAX];
    KEY_ACCESS access;
    kvec_t(char *) indexes;
};

#endif
