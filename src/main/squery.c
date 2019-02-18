#include "squery.h"
#include "workers.h"

void execute_squery(void *w) {
    struct squery *sq = w;
    M_INFO("Performing squery for shard %d", sq->shard_idx);
    worker_done(sq->worker);
}

