#ifndef __AGGS_H__
#define __AGGS_H__
#include "common.h"
#include "index.h"

typedef enum agg_type {
    AGG_ROOT,
    AGG_MAX,
    AGG_MIN,
    AGG_AVG,
    AGG_STATS,
    AGG_ERROR
} AGG_TYPE;

typedef enum agg_kind {
    AGGK_ROOT,
    AGGK_METRIC,
    AGGK_BUCKET,
    AGGK_PIPE,
} AGG_KIND;

struct agg;

typedef void (*parse_agg_f) (json_t *j);
typedef void (*consume_f) (struct agg *a, struct index *in, uint32_t docid, void *data);
typedef json_t *(*as_json_f) (struct agg *a);

/* Holds an aggregation and its nested aggregations */
typedef struct agg {
    char name[MAX_FIELD_NAME];
    AGG_TYPE type;
    AGG_KIND kind;
    kvec_t(struct agg *) children; // children applicable for root aggregations
    struct agg *naggs; // Nested aggregations for bucket aggregations
    consume_f consume;
    as_json_f as_json;
} agg_t;

struct agg *parse_aggs(const char *agg_key, const char *name, json_t *j, struct index *in);
struct agg *detect_and_parse(const char *key, json_t *j, struct index *in);

#endif
