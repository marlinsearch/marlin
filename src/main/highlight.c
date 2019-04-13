/* Highlighting and snippeting.  Some of the code actually belongs in analyzer?  Move things 
 * around when the mess in analyzer is cleanedup.  Will currently work only with default analyzer*/

#include "highlight.h"
#include "analyzer.h"

#define MIN3(a, b, c) ((a) < (b) ? ((a) < (c) ? (a) : (c)) : ((b) < (c) ? (b) : (c)))

static struct char_se *char_se_dup(const struct char_se *cse) {
    struct char_se *ret = malloc(sizeof(struct char_se));
    memcpy(ret, cse, sizeof(struct char_se));
    return ret;
}

static void new_token(word_t *word, void *sep, void *data) {
    kvec_t(struct token *) *tokens = data;
    kvec_t(struct char_se *) *se = sep;
    struct token *t = calloc(1, sizeof(struct token));
    t->word = worddup(word);
    int e = kv_size(*se);
    for (int i = e-word->length; i < e; i++) {
        struct char_se *dup = char_se_dup(kv_A(*se, i));
        kv_push(struct char_se *, t->se, dup);
    }
    kv_push(struct token *, *tokens, t);
}

static void tokenize(const char *str, void *data) {
    utf8proc_uint8_t *map_out;
    // Strip accents, invalid chars and lower case everything
    size_t len_out = utf8proc_map((uint8_t*)str, 0, &map_out, 
                           UTF8PROC_NULLTERM | UTF8PROC_STABLE |
                           UTF8PROC_STRIPMARK | UTF8PROC_COMPOSE |
                           UTF8PROC_COMPAT | UTF8PROC_LUMP | UTF8PROC_STRIPCC | 
                           UTF8PROC_STRIPNA | UTF8PROC_IGNORE |UTF8PROC_CASEFOLD);
    const char *pos = (const char *)map_out;
    const char *epos = (const char *)map_out + len_out;

    const char *pos2 = str;
    kvec_t(struct char_se *) se;
    kv_init(se);

    chr_t token[128];
    chr_t cp;
    chr_t cp2;
    int len = 0;
    int is_abbrev = 0;
    int is_hyphen = 0;
    word_pos_t word_pos;
    int position = 0;
    int hs = 0;
    uint32_t start = 0;

    while (pos < epos) {
        int size = utf8proc_iterate((unsigned char *)pos, -1, &cp);
        int size2 = utf8proc_iterate((unsigned char *)pos2, -1, &cp2);

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
                case UTF8PROC_CATEGORY_NO: {
                    token[len++] = cp;
                    struct char_se *cse = malloc(sizeof(struct char_se));
                    cse->start = start;
                    cse->end = start + size2;
                    kv_push(struct char_se *, se, cse);
                    }
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
                        word_pos.position = ++position;
                        new_token(&word_pos.word, &se, data);
                        hs = len;
                        break;
                    }
                    add_word = 1;
                    break;
            }
            pos += size;
            start += size2;
            pos2 += size2;
        } else {
            pos++;
            start++;
            pos2++;
        }

        // if word needs to be added, do it now
        if (add_word && len > 0) {
            if (is_hyphen) {
                if ((len - hs) > 0) {
                    word_pos.word.chars = &token[hs];
                    word_pos.word.length = len - hs;
                    word_pos.position = position;
                    new_token(&word_pos.word, &se, data);
                    position--;
                } else {
                    // Word has already been added.
                    goto reset_word;
                }
            }
            word_pos.word.chars = token;
            word_pos.word.length = len;
            word_pos.position = ++position;
            new_token(&word_pos.word, &se, data);
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

    // Free char_se we have been tracking
    for (int i = 0; i < kv_size(se); i++) {
        free(kv_A(se, i));
    }
    kv_destroy(se);

    free(map_out);
}

#if 0
static void dump_token(struct token *t) {
    printf("Word len is %d\n", t->word->length);
    for (int i = 0; i < t->word->length; i++) {
        struct char_se *c = kv_A(t->se, i);
        printf("%u %c s %u e %u\n", t->word->chars[i], t->word->chars[i], c->start, c->end);
    }
    printf("Match len %d\n", t->match_len);
    printf("\n");
}
#endif

int levenshtein(const chr_t *s1, int s1len, const chr_t *s2, int s2len, int max_dist) {
    int x, y;
    unsigned int matrix[s2len+1][s1len+1];
    matrix[0][0] = 0;
    for (x = 1; x <= s2len; x++)
        matrix[x][0] = matrix[x-1][0] + 1;
    for (y = 1; y <= s1len; y++)
        matrix[0][y] = matrix[0][y-1] + 1;
    int best_pos = 0xFF;
    for (x = 1; x <= s2len; x++) {
        int best = 0xFF;
        for (y = 1; y <= s1len; y++) {
            int cost = (s1[y-1] == s2[x-1] ? 0 : 1);
            matrix[x][y] = MIN3(matrix[x-1][y] + 1, matrix[x][y-1] + 1, matrix[x-1][y-1] + cost);
            if (x > 1 && y > 1 ) {
                if ((s1[y-1] == s2[x-2]) && (s1[y-2] == s2[x-1])) {
                    matrix[x][y] = MIN(matrix[x][y], matrix[x-2][y-2] + cost);
                }
            }
            if (matrix[x][y] < best) {
                best = matrix[x][y];
                best_pos = y;
            }
        }
        if (best > max_dist) return -1;
    }
    return best_pos;
}


bool is_match(struct token *token, struct search_term *t) {
    int max_dist = 0;
    if (t->typos) {
        max_dist = t->word->length > 7 ? 2 : (t->word->length > 3 ? 1 : 0);
    }
    // If its not a prefix match check if length is between +max_dist and -max_dist
    if (!t->prefix) {
        if ((token->word->length < t->word->length - max_dist) || 
                (token->word->length > t->word->length + max_dist)) {
            return false;
        }
    }

    int len = levenshtein(token->word->chars, token->word->length, t->word->chars, t->word->length, max_dist);
    if (len >= 0) {
        token->is_match = true;
        token->match_len = len;
    }
    return token->is_match;
}


/* Highlights str with the terms in query q and snips response to snip_num_words.  If nothing
 * is highlighted returns NULL */
char *highlight(const char *str, struct query *q, int snip_num_words) {
    kvec_t(struct token *) tokens;
    kv_init(tokens);
    tokenize(str, &tokens);

#if 0
    printf("\n\nNum Tokens %lu len %lu\n", kv_size(tokens), strlen(str));
    for (int i = 0; i < kv_size(tokens); i++) {
        dump_token(kv_A(tokens, i));
    }
#endif

    // Take every token and see if it matches a term
    int num_terms = kv_size(q->terms);
    int num_tokens = kv_size(tokens);
    int num_matches = 0;
    int last_match_end = 0;

    int snip_end= 0; 
    int best_start = 0;
    int best_end = 0;
    int best_matches = 0;

    for (int i = 0; i < num_tokens; i++) {

        struct token *t = kv_A(tokens, i);
        // Make sure we are not overlapping
        if (kv_A(t->se, 0)->start < last_match_end) continue;

        // Find a match by looking at all terms
        for (int j = 0; j < num_terms; j++) {
            if (is_match(t, kv_A(q->terms, j))) {
                num_matches++;
#if 0
                printf("Matched %d\n", i);
                dump_token(t);
#endif
                last_match_end = kv_A(t->se, t->match_len-1)->end;
                break;
            }
        }

        // Handle snippets.. choose the snippet with most matches
        if (snip_num_words) {
            if (snip_end < snip_num_words) {
                best_matches = num_matches;
                best_end++;
                snip_end++;
            } else {
                num_matches -= kv_A(tokens, snip_end - snip_num_words)->is_match;
                if (num_matches > best_matches) {
                    best_start = snip_end - snip_num_words;
                    best_end = snip_end;
                    best_matches = num_matches;
                }
                snip_end++;
            }
        }
    }

    char *resp = NULL;
    if (last_match_end == 0) goto free_tokens;

    int tlen = strlen(str) + (num_matches * strlen("<em> </em>"));

    resp = malloc(tlen);
    int pos = 0;
    int start = 0;
    int end = num_tokens;
    int i = 0;

    if (snip_num_words) {
        // TODO: Center best_start and best_end
        start = kv_A(kv_A(tokens, best_start)->se, 0)->start;
        i = best_start;
        end = best_end;
    }

    for (; i < end; i++) {
        struct token *t = kv_A(tokens, i);
        // We have a match
        if (t->is_match) {
            // Copy until start
            struct char_se *c = kv_A(t->se, 0);
            // We can end up in this state due to split words
            if (c->start < start) start = c->start;

            memcpy(&resp[pos], &str[start], c->start - start);
            pos += (c->start - start);
            // Copy start
            memcpy(&resp[pos], "<em>", strlen("<em>"));
            pos += strlen("<em>");
            struct char_se *c2 = kv_A(t->se, t->match_len - 1);
            // Copy matching content
            memcpy(&resp[pos], &str[c->start], c2->end - c->start);
            pos += c2->end - c->start;
            start = c2->end;
            // copy end
            memcpy(&resp[pos], "</em>", strlen("</em>"));
            pos += strlen("</em>");
        }

        if (i == num_tokens - 1) {
            struct char_se *c = kv_A(t->se, t->word->length - 1);
            memcpy(&resp[pos], &str[start], c->end - start);
            pos += c->end - start;
        }
    }
    resp[pos] = '\0';

free_tokens:
    // Free the tokens and char_se in it
    for (int i = 0; i < kv_size(tokens); i++) {
        struct token *t = kv_A(tokens, i);
        for (int j = 0; j < kv_size(t->se); j++) {
            free(kv_A(t->se, j));
        }
        kv_destroy(t->se);
        wordfree(t->word);
        free(t);
    }
    kv_destroy(tokens);
    return resp;
}
