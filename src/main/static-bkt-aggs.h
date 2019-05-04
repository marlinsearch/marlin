#ifndef __SBKT_AGGS_H__
#define __SBKT_AGGS_H__
#include "common.h"
#include "index.h"
#include "aggs.h"

struct range_bkt {
    struct bkt b;
    double from;
    double to;
};

struct agg *parse_range_agg(const char *name, json_t *j, struct index *in);

#endif
