#ifndef __DEBUG_H_
#define __DEBUG_H_
#include "word.h"
#include "query.h"
#include "dtrie.h"

// #define DUMP_ENABLE 1
#define TRACE_QUERY 1

#ifdef DUMP_ENABLE
void worddump(const word_t *a);
void dump_query(struct query *q);
void dump_termresult(termresult_t *tr);
void dump_bmap(struct bmap *b);
#else
#define worddump(a) ;
#define dump_query(q) ;
#define dump_termresult(tr) ;
#define dump_bmap(b) ;
#endif

#ifdef TRACE_QUERY
void trace_query(const char *msg, struct timeval *start);
#else
#define trace_query(msg, start) ;
#endif


#endif
