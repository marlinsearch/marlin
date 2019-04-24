#ifndef __METRICS_AGGS_H__
#define __METRICS_AGGS_H__
#include "common.h"
#include "index.h"
#include "aggs.h"

#define JA_FIELD    "field"
#define JA_VALUE    "value"
#define JA_SUM      "sum"
#define JA_AVG      "avg"
#define JA_MIN      "min"
#define JA_MAX      "max"
#define JA_COUNT    "count"

struct agg_max {
    struct agg a;
    int max_field;
    double value;
};

struct agg_min{
    struct agg a;
    int min_field;
    double value;
};

struct agg_avg {
    struct agg a;
    int field;
    double sum;
    int count;
};

struct agg_stats {
    struct agg a;
    int field;
    double sum;
    double min;
    double max;
    int count;
};

struct agg_card {
    struct agg a;
    int field;
    struct bmap *bmap;
    struct oper *oper;
    uint32_t count;
};

struct agg *parse_max_agg(const char *name, json_t *j, struct index *in);
struct agg *parse_min_agg(const char *name, json_t *j, struct index *in);
struct agg *parse_avg_agg(const char *name, json_t *j, struct index *in);
struct agg *parse_stats_agg(const char *name, json_t *j, struct index *in);
struct agg *parse_card_agg(const char *name, json_t *j, struct index *in);

#endif
