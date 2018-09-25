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
#define MASTER_APPID    "master_app_id"
#define MASTER_APIKEY   "master_api_key"
#define DB_LOCATION     "db_location"
#define SSL_CERT        "certificate"
#define SSL_KEY         "private_key"
#define PORT            "port"
#define HTTPS           "https"
#define NUMTHREADS      "num_threads"

#endif

