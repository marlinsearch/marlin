#ifndef __METRICS_AGGS_H__
#define __METRICS_AGGS_H__
#include "common.h"
#include "index.h"
#include "aggs.h"

#define JA_FIELD    "field"
#define JA_VALUE    "value"

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
    double value;
    int count;
};

struct agg *parse_max_agg(const char *name, json_t *j, struct index *in);
struct agg *parse_min_agg(const char *name, json_t *j, struct index *in);
struct agg *parse_avg_agg(const char *name, json_t *j, struct index *in);

#endif
