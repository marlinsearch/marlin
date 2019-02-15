#ifndef __QUERY_H__
#define __QUERY_H__

#include "kvec.h"
#include "analyzer.h"

struct query {
    struct index *in;
    kvec_t(word_t *) words;
    int num_words;
};

struct query *query_new(struct index *in);
void query_free(struct query *q);

#endif
