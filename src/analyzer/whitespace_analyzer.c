#include <stdio.h>
#include <string.h>
#include "analyzer.h"

#define MAX_WHITESPACE_TOKEN_LEN  256


static void analyze_string_for_indexing(const char *str, new_word_pos_f cb, void *data) {
    const char *pos = (const char *)str;
    const char *epos = (const char *)str + strlen(str);
    chr_t token[MAX_WHITESPACE_TOKEN_LEN];
    word_pos_t word_pos;
    chr_t cp;
    int len = 0;
    int position = 0;

    while (pos < epos) {
        int size = utf8proc_iterate((unsigned char *)pos, -1, &cp);
        // If we reached the end we need to add the word
        int add_word = (pos == (epos - size)) ? 2 : 0;
        if (size > 0) {
            if (cp == ' ') {
                add_word = 1;
            } else {
                token[len++] = cp;
            }
            pos += size;
        } else {
            pos++;
        }
        // if word needs to be added, do it now
        if (add_word && len > 0) {
            word_pos.word.chars = token;
            word_pos.word.length = len;
            word_pos.position = position++;
            cb(&word_pos, data);
            len = 0;
        }
        if (len == MAX_WHITESPACE_TOKEN_LEN-1) {
            len = 0;
        }
    }
}

void init_whitespace_analyzer(void) {
    struct analyzer *a = calloc(1, sizeof(struct analyzer));
    snprintf(a->name, sizeof(a->name), "%s", "whitespace");
    a->analyze_string_for_indexing = &analyze_string_for_indexing;
    register_analyzer(a);
}

