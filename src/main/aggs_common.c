#include "aggs_common.h"
#include <float.h>

double get_agg_num_value(struct squery *sq, uint32_t docid, void *data, int priority) {
    double val = -DBL_MAX;
    if (data) {
        uint8_t *pos = data;
        double *dpos = (double *)(pos + sizeof(uint32_t));
        // TODO: Handle NULL values
        val = dpos[priority];
    } else {
        int docpos = docid % 1000;
        uint64_t docgrp_id = IDNUM((docid - docpos), priority);
        khash_t(IDNUM2DBL) *kh = sq->kh_idnum2dbl;
        khiter_t k = kh_get(IDNUM2DBL, kh, docgrp_id);
        double *grpd = NULL;

        if (k == kh_end(kh)) {
            // We need to add the entry to be written
            int ret = 0;
            k = kh_put(IDNUM2DBL, kh, docgrp_id, &ret);

            // Check if it already exists in lmdb
            MDB_val key, mdata;
            key.mv_size = sizeof(uint64_t);
            key.mv_data = (void *)&docgrp_id;
            // If it already exists, we need not write so set a NULL value to 
            // avoid looking up mdb everytime we encounter this facetid
            if (mdb_get(sq->txn, sq->shard->sindex->idnum2dbl_dbi, &key, &mdata) == 0) {
                grpd = mdata.mv_data;
            }
            kh_value(kh, k) = grpd;
        } else {
            grpd = kh_value(kh, k);
        }
        if (grpd) {
            return grpd[docpos];
        }
    }
    return val;
}



