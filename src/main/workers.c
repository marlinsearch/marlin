#include "workers.h"


void worker_init(struct worker *tdata, int num_workers) {
    tdata->pending = num_workers;
    pthread_mutex_init(&tdata->lock, NULL);
    pthread_cond_init(&tdata->cond, NULL);
}

void worker_done(struct worker *worker) {
    pthread_mutex_lock(&worker->lock);
    worker->pending--;
    pthread_cond_signal(&worker->cond);
    pthread_mutex_unlock(&worker->lock);
}

void wait_for_workers(struct worker *worker) {
    // Wait till workers are done
    pthread_mutex_lock(&worker->lock);
    while (worker->pending > 0) {
        pthread_cond_wait(&worker->cond, &worker->lock);
    }
    pthread_mutex_unlock(&worker->lock);
}

void worker_destroy(struct worker *tdata) {
    pthread_mutex_destroy(&tdata->lock);
    pthread_cond_destroy(&tdata->cond);
}

