#ifndef __METRICS_AGGS_H__
#define __METRICS_AGGS_H__

#include "common.h"
#include "index.h"
#include "aggs.h"

struct agg_max {
    struct agg a;
    double value;
};

struct agg_min {
    struct agg a;
    double value;
};

struct agg_avg {
    struct agg a;
    double sum;
    int count;
};

struct agg_stats {
    struct agg a;
    double sum;
    double min;
    double max;
    int count;
};

struct agg_card {
    struct agg a;
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
