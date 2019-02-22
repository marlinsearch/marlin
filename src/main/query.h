#ifndef __QUERY_H__
#define __QUERY_H__

#include "kvec.h"
#include "index.h"
#include "word.h"

typedef enum prefix_type {
    PREFIX_LAST,
    PREFIX_ALL,
    PREFIX_NONE,
    PREFIX_MAX
} PREFIX_TYPE;

typedef enum match_type {
    MATCH_ALL,
    MATCH_ANY,
    IGNORE_LAST,
    IGNORE_FIRST,
    MATCH_MAX
} MATCH_TYPE;

typedef enum query_type {
    QUERY_DEFAULT,
    QUERY_PHRASE,
    QUERY_MAX
} QUERY_TYPE;

typedef enum typos_type {
    TYPO_OK,
    TYPO_NONE,
    TYPO_MAX
} TYPO_TYPE;

struct query_cfg {
    PREFIX_TYPE prefix;
    TYPO_TYPE typos;
};

struct query {
    struct index *in;
    char *text;
    kvec_t(word_t *) words;
    int num_words;
    kvec_t(term_t *) terms;

    // Query config
    struct query_cfg cfg;
};

struct query *query_new(struct index *in);
void query_free(struct query *q);
char *execute_query(struct query *q);
void generate_query_terms(struct query *q);

#endif
