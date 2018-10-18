#include <stdio.h>
#include <string.h>
#include "analyzer.h"


static void analyze_string_for_indexing(const char *str, new_word_pos_f cb, void *data) {
    utf8proc_uint8_t *map_out;
    // Strip accents, invalid chars and lower case everything
    size_t len_out = utf8proc_map((uint8_t*)str, 0, &map_out, 
                           UTF8PROC_NULLTERM | UTF8PROC_STABLE |
                           UTF8PROC_STRIPMARK | UTF8PROC_COMPOSE |
                           UTF8PROC_COMPAT | UTF8PROC_LUMP | UTF8PROC_STRIPCC | 
                           UTF8PROC_STRIPNA | UTF8PROC_IGNORE |UTF8PROC_CASEFOLD);
    const char *pos = (const char *)map_out;
    const char *epos = (const char *)map_out + len_out;
    chr_t token[128];
    chr_t cp;
    int len = 0;
    int is_abbrev = 0;
    int is_hyphen = 0;
    word_pos_t word_pos;
    int position = 0;
    int hs = 0;

    while (pos < epos) {
        int size = utf8proc_iterate((unsigned char *)pos, -1, &cp);
        // If we reached the end we need to add the word
        int add_word = (pos == (epos - size)) ? 2 : 0;
        if (size > 0) {
            // printf("%c %d %d %d\n", pos[0], cp,utf8proc_tolower(cp), utf8proc_category(cp));
            utf8proc_category_t cat = utf8proc_category(cp);
            switch (cat) {
                case UTF8PROC_CATEGORY_LL:
                case UTF8PROC_CATEGORY_LO:
                case UTF8PROC_CATEGORY_PC:
                case UTF8PROC_CATEGORY_MC:
                case UTF8PROC_CATEGORY_MN:
                case UTF8PROC_CATEGORY_ND:
                case UTF8PROC_CATEGORY_NL:
                case UTF8PROC_CATEGORY_NO:
                    token[len++] = cp;
                    break;
                default:
                    // u.s.a. => usa
                    if (((char)cp) == '.') {
                        if ((len == 1) || is_abbrev) {
                            is_abbrev = 1;
                            break;
                        }
                    }
                    // don't => dont
                    if (((char)cp) == '\'') {
                        is_abbrev = 1;
                        break;
                    }
                    // Handle hyphenated words
                    if (((char)cp) == '-' && (len > 0)) {
                        is_hyphen = 1;
                        word_pos.word.chars = &token[hs];
                        word_pos.word.length = len - hs;
                        word_pos.position = position++;
                        cb(&word_pos, data);
                        hs = len;
                        break;
                    }
                    add_word = 1;
                    break;
            }
            pos += size;
        } else {
            pos++;
        }
        // if word needs to be added, do it now
        if (add_word && len > 0) {
            if (is_hyphen) {
                if ((len - hs) > 0) {
                    word_pos.word.chars = &token[hs];
                    word_pos.word.length = len - hs;
                    word_pos.position = position;
                    cb(&word_pos, data);
                    position--;
                } else {
                    // Word has already been added.
                    goto reset_word;
                }
            }
            word_pos.word.chars = token;
            word_pos.word.length = len;
            word_pos.position = position++;
            cb(&word_pos, data);
reset_word:
            is_hyphen = 0;
            is_abbrev = 0;
            len = 0;
            hs = 0;
        }
        if (len >= 127) {
            len = 0;
        }
    }
    free(map_out);
}

void init_default_analyzer(void) {
    struct analyzer *a = calloc(1, sizeof(struct analyzer));
    snprintf(a->name, sizeof(a->name), "%s", "default");
    a->analyze_string_for_indexing = &analyze_string_for_indexing;
    register_analyzer(a);
}

