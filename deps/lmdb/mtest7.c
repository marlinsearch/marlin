/* mtest.c - memory-mapped database tester/toy */
/*
 * Copyright 2011-2020 Howard Chu, Symas Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include <sys/types.h>
#include "lmdb.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

float timedifference_msec(struct timeval t0, struct timeval t1) {
    return (t1.tv_sec - t0.tv_sec) * 1000.0f + (t1.tv_usec - t0.tv_usec) / 1000.0f;
}

int main(int argc,char * argv[])
{
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi[10];
	MDB_val key, data;
	MDB_txn *txn;
	int count;
	double tsize = 0;
	int *values;
	struct timeval start, stop, tstart;

	srand(time(NULL));

	    count = (rand()%384) + 64;
	    values = (int *)malloc(count*sizeof(int));

	    for(i = 0;i<count;i++) {
			values[i] = rand()%1024;
	    }
    
		E(mdb_env_create(&env));
		mdb_env_set_maxdbs(env, 64);
		E(mdb_env_set_mapsize(env, 65536000000));

        // 200k write
		// 135 E(mdb_env_open(env, "./testdb", MDB_NORDAHEAD, 0664));
		// 88 E(mdb_env_open(env, "./testdb", MDB_NORDAHEAD|MDB_NOSYNC|MDB_NOMETASYNC, 0664));
        // 86 E(mdb_env_open(env, "./testdb", MDB_NORDAHEAD|MDB_WRITEMAP|MDB_MAPASYNC, 0664));
        // 500k writes : 526.791000s  : dbi[key%10]
		E(mdb_env_open(env, "./testdb", MDB_NORDAHEAD|MDB_NOSYNC|MDB_NOMETASYNC, 0664));

		E(mdb_txn_begin(env, NULL, 0, &txn));
		E(mdb_dbi_open(txn, "a0", MDB_CREATE, &dbi[0]));
		E(mdb_dbi_open(txn, "a1", MDB_CREATE, &dbi[1]));
		E(mdb_dbi_open(txn, "a2", MDB_CREATE, &dbi[2]));
		E(mdb_dbi_open(txn, "a3", MDB_CREATE, &dbi[3]));
		E(mdb_dbi_open(txn, "a4", MDB_CREATE, &dbi[4]));
		E(mdb_dbi_open(txn, "a5", MDB_CREATE, &dbi[5]));
		E(mdb_dbi_open(txn, "a6", MDB_CREATE, &dbi[6]));
		E(mdb_dbi_open(txn, "a7", MDB_CREATE, &dbi[7]));
		E(mdb_dbi_open(txn, "a8", MDB_CREATE, &dbi[8]));
		E(mdb_dbi_open(txn, "a9", MDB_CREATE, &dbi[9]));
		E(mdb_txn_commit(txn));
   
		key.mv_size = sizeof(uint64_t);

	    gettimeofday(&tstart, NULL);
        int maxcount = 500;
	    for (i=1;i<maxcount;i++) {	
	        gettimeofday(&start, NULL);
		    E(mdb_txn_begin(env, NULL, 0, &txn));
		    int size = 0;
	        for (j = 1; j < 10000; j++) {
                uint64_t keyd = (i * j) + rand();
                key.mv_data = &keyd;
                data.mv_size = (rand() % 1000) + 4000;
                data.mv_data = malloc(data.mv_size);
                size += data.mv_size;
                tsize += (data.mv_size / 1000);
                //rc = mdb_put(txn, dbi[0], &key, &data, 0);
                rc = mdb_put(txn, dbi[keyd%10], &key, &data, 0);
                free(data.mv_data);
            }
		    E(mdb_txn_commit(txn));
	        gettimeofday(&stop, NULL);
            float took = timedifference_msec(start, stop);
            float ttook = timedifference_msec(tstart, stop);
            printf("Wrote total %10d : %10d took %5f tw %5fMB ttook %5fs\n", i * j, size, took, 
                    tsize, ttook/1000.0);
	    }
	    for (int x = 0; x<10; x++) {
		    mdb_dbi_close(env, dbi[x]);
        }
		mdb_env_close(env);
	return 0;
}
