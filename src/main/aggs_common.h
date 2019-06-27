#ifndef __AGGS_COMMON_H__
#define __AGGS_COMMON_H__

#include "common.h"
#include "squery.h"

double get_agg_num_value(struct squery *sq, uint32_t docid, void *data, int priority);

#endif

