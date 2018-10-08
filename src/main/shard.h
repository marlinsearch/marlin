#ifndef __SHARD_H_
#define __SHARD_H_

#include "index.h"
#include "sdata.h"

struct shard {
    uint16_t shard_id;
    struct index *index;
    struct sdata *sdata;
};

struct shard *shard_new(struct index *in, uint16_t shard_id);
void shard_add_objects(struct shard *s, json_t *j);
void shard_free(struct shard *s);
void shard_delete(struct shard *s);
void shard_clear(struct shard *s);

#endif

