#ifndef __SORT_H_
#define __SORT_H_
#include "docrank.h"

#define PARTIAL_SORT_LIMIT 100

typedef enum {
    R_TYPO,
    R_PROX,
    R_POS,
    R_EXACT,
    R_FIELD,
    R_GEO,
    R_DONE,
    R_COMP,
    R_COMP_ASC,
    R_MAX,
} SORT_RULE;

extern SORT_RULE default_rule[];
extern int default_num_rules;
SORT_RULE rulestr_to_rule(const char *str);
const char *rule_to_rulestr(SORT_RULE rule);

void rank_partial_sort(docrank_t *a, size_t l, size_t r, size_t m, SORT_RULE *rules);
void rank_sort(size_t n, docrank_t *a, SORT_RULE *rules);

#endif
