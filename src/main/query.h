#ifndef __QUERY_H__
#define __QUERY_H__

#include "kvec.h"
#include "index.h"
#include "word.h"
#include "dtrie.h"
#include "sort.h"
#include "ksort.h"

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

/* Query configuration for an index */
struct query_cfg {
    PREFIX_TYPE prefix;
    TYPO_TYPE typos;
    MATCH_TYPE match;
    uint16_t hits_per_page;
    uint16_t max_hits;
    uint16_t max_facet_results;
    int num_rules;
    char rank_by_field[MAX_FIELD_NAME];
    int rank_by;        // Field on which ranking has to be performed
    bool rank_sort;     // Sort by the field or use it to rank finally
    bool rank_asc;      // Rank by ascending or descending order
    bool full_scan;     // Do we want to scan all documents before a result?
    uint32_t full_scan_threshold; // Threshold under which a full scan is performed
    SORT_RULE  rank_algo[R_MAX + 1]; // Ranking algorithm

    int *facet_enabled;
};

struct query {
    struct index *in;
    char *text;
    kvec_t(word_t *) words;
    int num_words;
    kvec_t(term_t *) terms;
    struct filter *filter;

    // Query config
    struct query_cfg cfg;
    uint16_t page_num;
    bool explain;
    SORT_RULE  rank_rule[R_MAX + 1]; // Ranking algorithm + rank_by 
};

struct query *query_new(struct index *in);
void query_free(struct query *q);
char *execute_query(struct query *q);
void generate_query_terms(struct query *q);

#endif
