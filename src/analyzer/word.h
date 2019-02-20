#ifndef _WORD_H_
#define _WORD_H_

#include <utf8proc.h>

typedef utf8proc_int32_t chr_t; 

typedef struct word {
    chr_t *chars;
    int length;
} word_t;

word_t *wordnew(void);
word_t *worddup(const word_t *a);
word_t *wordadd(const word_t *a, const word_t *b);
word_t *wordcat(word_t *dest, const word_t *src);
void worddump(const word_t *a);

#endif

