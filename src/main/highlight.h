#ifndef __HIGHLIGHT_H_
#define __HIGHLIGHT_H_

#include "query.h"

/* Used to track start and end position of char * for every unicode code point */
typedef struct char_se {
    uint32_t start;
    uint32_t end;
} char_se_t;

struct token {
    word_t *word;
    kvec_t(struct char_se*) se;
    bool is_match;
    uint8_t match_len;
};

char *highlight(const char *str, struct query *q, int snip_num_words);

#endif
