#ifndef __ANALYZER_H__
#define __ANALYZER_H__
#include <utf8proc.h>

#define MAX_ANALYZER_NAME  256

typedef utf8proc_int32_t chr_t; 

typedef struct word {
    chr_t *chars;
    int length;
} word_t;

typedef struct word_pos {
    word_t word;
    int position;
} word_pos_t;

struct analyzer;

typedef void (*new_word_pos_f) (word_pos_t *wordpos, void *data);

typedef void (*index_string_f) (const char *str, new_word_pos_f cb, void *data);

typedef void (*search_string_f) (const char *str, new_word_pos_f cb, void *data);

typedef void (*free_analyzer_f) (struct analyzer *a);


struct analyzer {
    char name[MAX_ANALYZER_NAME];
    index_string_f analyze_string_for_indexing; 
    search_string_f analyze_string_for_search; 
    free_analyzer_f free_analyzer;
    void *cfg;
    struct analyzer *next;
};


void init_analyzers(void);
void register_analyzer(struct analyzer *a);
struct analyzer *get_analyzer(const char *name);
struct analyzer *get_default_analyzer(void);

#endif

