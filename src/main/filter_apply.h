#ifndef __FILTER_APPLY_H__
#define __FILTER_APPLY_H__

#include "filter.h"

struct bmap *filter_apply(struct sindex *in, struct filter *f, MDB_txn *txn, struct bmap *docs);

#endif
