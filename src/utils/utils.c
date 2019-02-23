#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "utils.h"
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

