#include <stdio.h>
#include "debug.h"

#ifdef DUMP_ENABLE
void worddump(const word_t *a) {
    printf("Length : %d [ ", a->length);
    for (int i = 0; i < a->length; i++) {
        printf("%u ", a->chars[i]);
    }
    printf("]\n");
}

static void dump_term(term_t *t) {
    worddump(t->word);
    printf("Prefix %d Typos %d\n", t->prefix, t->typos);
}

void dump_query(struct query *q) {
    printf("Query text %s\n", q->text);
    printf("\nQuery words %d\n", q->num_words);
    for (int i = 0; i < q->num_words; i++) {
        worddump(kv_A(q->words, i));
    }
    printf("\nQuery terms %lu\n", kv_size(q->terms));
    for (int i = 0; i < kv_size(q->terms); i++) {
        dump_term(kv_A(q->terms, i));
    }
    printf("\n");
}

void dump_termresult(termresult_t *tr) {
    printf("Term twid : %u\n", tr->twid);
    printf("Num wids  : %u\n", kh_size(tr->wordids));
    uint32_t wid;
    int dist;
    kh_foreach(tr->wordids, wid, dist, {
        printf("wid : %u, dist %d\n", wid, dist);
    });
}

static void bmap_iter(uint32_t x, void *d) {
    uint32_t *v = d;
    *v += 1;
    printf("%8u", x);
    if ((*v % 8) == 0) printf("\n");
}

void dump_bmap(struct bmap *b) {
    if (!b) {
        printf("bmap is null\n");
        return;
    }
    printf("\nBitmap length : %u\n", bmap_cardinality(b));
    uint32_t x = 0;
    bmap_iterate(b, bmap_iter, &x);
    printf("\n");
}
#endif
