#ifndef COMMON_H
#define COMMON_H
#include <inttypes.h>
#include <sys/types.h>
#include <stdbool.h>

#include "platform.h"
#include "mlog.h"

// Settings
#define APPID_SIZE      8
#define APIKEY_SIZE     32
#define SETTINGS_PATH   "./settings.json"
#define MASTER_APPID    "masterAppId"
#define MASTER_APIKEY   "masterApiKey"
#define DB_LOCATION     "dbLocation"
#define SSL_CERT        "certificate"
#define SSL_KEY         "privateKey"
#define PORT            "port"
#define HTTPS           "https"
#define NUMCORES        "numCores"

#endif

