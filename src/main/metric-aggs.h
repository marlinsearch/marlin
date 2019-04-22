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

struct agg *parse_max_agg(const char *name, json_t *j, struct index *in);

#endif
