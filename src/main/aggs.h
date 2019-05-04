#ifndef __AGGS_H__
#define __AGGS_H__

#include "common.h"
#include "squery.h"

#define JA_FIELD    "field"
#define JA_VALUE    "value"
#define JA_SUM      "sum"
#define JA_AVG      "avg"
#define JA_MIN      "min"
#define JA_MAX      "max"
#define JA_COUNT    "count"
#define JA_RANGE    "range"
#define JA_RANGES   "ranges"

typedef enum agg_type {
    AGG_ROOT,
    // Metrics
    AGG_MAX,
    AGG_MIN,
    AGG_AVG,
    AGG_STATS,
    AGG_CARDINALITY,
    // Bucket
    AGG_RANGE,
    AGG_ERROR
} AGG_TYPE;

typedef enum agg_kind {
    AGGK_ROOT,
    AGGK_METRIC,
    AGGK_BUCKET,
    AGGK_PIPE,
} AGG_KIND;

/* Buckets for bucket aggregations */
struct bkt;

typedef void (*consume_b) (struct bkt *b, struct squery *sq, uint32_t docid, void *data, double v);
typedef json_t *(*as_json_b) (struct bkt *b);

struct bkt {
    char key[MAX_FIELD_NAME];
    uint32_t count;
    int field;
    consume_b consume;
    as_json_b as_json;
    struct agg *aggs;
};


/* Aggregations */
struct agg;

typedef void (*parse_agg_f) (json_t *j);
typedef void (*consume_f) (struct agg *a, struct squery *sq, uint32_t docid, void *data);
typedef json_t *(*as_json_f) (struct agg *a);
typedef struct agg *(*agg_dup_f) (const struct agg *a);
typedef void (*merge_agg_f) (struct agg *info, const struct agg *from);
typedef void (*agg_free_f) (struct agg *a);

/* Holds an aggregation and its nested aggregations */
typedef struct agg {
    char name[MAX_FIELD_NAME];
    AGG_TYPE type;
    AGG_KIND kind;
    kvec_t(struct agg *) children; // children applicable for root aggregations
    struct agg *naggs; // Nested aggregations for bucket aggregations
    consume_f consume;
    as_json_f as_json;
    agg_dup_f dup;
    merge_agg_f merge;
    agg_free_f free;
    kvec_t(struct bkt *) bkts; // Buckets for bucket aggregations
    int field; // Field  the aggregation deals with
} agg_t;

struct agg *parse_aggs(json_t *j, struct index *in);
struct agg *detect_and_parse(const char *key, json_t *j, struct index *in);
struct agg *aggs_dup(const struct agg *a);

#endif

