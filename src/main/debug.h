#ifndef __DEBUG_H_
#define __DEBUG_H_
#include "word.h"
#include "query.h"
#include "dtrie.h"


void worddump(const word_t *a);
void dump_query(struct query *q);
void dump_termresult(termresult_t *tr);

#endif
