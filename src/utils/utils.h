#ifndef __UTIL_H_
#define __UTIL_H_
#include <inttypes.h>
#include <jansson.h>
#include <stdbool.h>
#include "flakeid.h"
#include "base64.h"

void random_str(char *s, const int len);
uint64_t get_utc_seconds(void);
char *generate_docid(flakeid_ctx_t *fctx);
int get_shard_routing_id(const char *key, int num_shards);
bool is_json_string_array(const json_t *j);
float timedifference_msec(struct timeval t0, struct timeval t1);

#endif
