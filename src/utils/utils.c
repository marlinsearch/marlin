#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "utils.h"
#include "common.h"
#include "farmhash-c.h"

/* Generates a random string */
// TODO: Rewrite using libcrypto
void random_str(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    // Try using urandom else switch to superweak rand
    FILE *f = fopen("/dev/urandom", "r");
    if (f) {
        unsigned char *rnd = malloc(len + 1);
        if (fread(rnd, len+1, 1, f)) {
            for (int i = 0; i < len; ++i) {
                s[i] = alphanum[rnd[i] % (sizeof(alphanum) - 1)];
            }
            s[len] = 0;
            free(rnd);
            fclose(f);
            return;
        } else {
            free(rnd);
        }
        fclose(f);
    }

    time_t t;
    srand((unsigned) (time(&t)*7879));
    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    s[len] = 0;
}

/* Returns seconds from epoch */
uint64_t get_utc_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec;
}

char *generate_docid(flakeid_ctx_t *fctx) {
    unsigned char id[16];
    flakeid_get(fctx, id);
    size_t outlen;
    unsigned char *b = base64_encode(&id[1], 15, &outlen);
    return (char *)b;
}

int get_shard_routing_id(const char *key, int num_shards) {
    uint64_t hash = farmhash(key, strlen(key));
    return hash % num_shards;
}

bool is_json_string_array(const json_t *j) {
    if (!json_is_array(j)) return false;
    size_t id;
    json_t *js;
    json_array_foreach(j, id, js) {
        if (!json_is_string(js)) return false;
    }
    return true;
}

float timedifference_msec(struct timeval t0, struct timeval t1) {
    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
}

char *http_error(h2o_req_t *req, HTTP_CODE code) {
    req->res.status = code;
    switch (code) {
        case HTTP_NOT_FOUND:
            req->res.reason = "Not Found";
            break;
        case HTTP_BAD_REQUEST:
            req->res.reason = "Bad Request";
            break;
        case HTTP_SERVER_ERROR:
            req->res.reason = "Internal Error";
            break;
        case HTTP_TOO_MANY:
            req->res.reason = "Too Many Requests";
        default:
            break;
    }
    return strdup(J_FAILURE);
}

char *failure_message(const char *msg) {
    char *tmp = malloc(PATH_MAX);
    snprintf(tmp, PATH_MAX, "{\"success\": false, \"message\": \"%s\"}", msg);
    return tmp;
}

