#ifndef __SHARD_H_
#define __SHARD_H_

#include "index.h"
#include "sdata.h"
#include "sindex.h"

struct shard {
    uint16_t shard_id;
    char idx_name[PATH_MAX];
    char base_path[PATH_MAX];
    struct index *index;
    struct sdata *sdata;
    struct sindex *sindex;
};

struct shard *shard_new(struct index *in, uint16_t shard_id);
void shard_add_documents(struct shard *s, json_t *j);
void shard_free(struct shard *s);
void shard_delete(struct shard *s);
void shard_clear(struct shard *s);
void shard_set_mapping(struct shard *s, const struct mapping *m);
struct bmap *shard_get_all_docids(struct shard *s);

#endif

