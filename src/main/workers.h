#ifndef __WORKERS_H_
#define __WORKERS_H_
#include <pthread.h>
#include "threadpool.h"

struct worker {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int pending;
};

void worker_init(struct worker *worker, int num_workers);
void worker_destroy(struct worker *worker);
void worker_done(struct worker *worker);
void wait_for_workers(struct worker *worker);

#endif
