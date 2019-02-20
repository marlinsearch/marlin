#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "word.h"

/* Create a new empty word of length 0 */
word_t *wordnew(void) {
    word_t *w = calloc(1, sizeof(word_t));
    return w;
}

/* Creates a copy of a word */
word_t *worddup(const word_t *a) {
    word_t *nw = wordnew();
    nw->length = a->length;
    nw->chars = malloc(sizeof(chr_t) * nw->length);
    memcpy(nw->chars, a->chars, sizeof(chr_t) * nw->length);
    return nw;
}

/* Creates a new word which is a concatenation of 2 words */
word_t *wordadd(const word_t *a, const word_t *b) {
    word_t *w = wordnew();
    w->length = a->length + b->length;
    w->chars = malloc(sizeof(chr_t) * w->length);
    memcpy(w->chars, a->chars, sizeof(chr_t) * a->length);
    memcpy(&w->chars[a->length], b->chars, sizeof(chr_t) * a->length);
    return w;
}

word_t *wordcat(word_t *dest, const word_t *src) {
    int len = dest->length;
    dest->length += src->length;
    dest->chars = realloc(dest->chars, sizeof(chr_t) * dest->length);
    memcpy(&dest->chars[len], src->chars, sizeof(chr_t) * src->length);
    return dest;
}

void worddump(const word_t *a) {
    printf("Length : %d [ ", a->length);
    for (int i = 0; i < a->length; i++) {
        printf("%u ", a->chars[i]);
    }
    printf("]\n");
}

