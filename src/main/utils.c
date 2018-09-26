#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <sys/time.h>
#include "utils.h"

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
