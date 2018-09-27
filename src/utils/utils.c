#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "utils.h"
#include "cfarmhash.h"

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
            return;
        } else {
            free(rnd);
        }
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

char *generate_objid(flakeid_ctx_t *fctx) {
    unsigned char id[16];
    flakeid_get(fctx, id);
    size_t outlen;
    unsigned char *b = base64_encode(&id[1], 15, &outlen);
    return (char *)b;
}

int get_shard_routing_id(const char *key, int num_shards) {
    uint64_t hash = cfarmhash(key, strlen(key));
    return hash % num_shards;
}

