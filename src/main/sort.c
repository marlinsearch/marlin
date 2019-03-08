#include <stdlib.h>
#include <float.h>
#include "ksort.h"
#include "sort.h"
#include <stdbool.h>

SORT_RULE default_rule[] = { R_TYPO, R_PROX, R_POS, R_EXACT };
int default_num_rules = 4;

const char *sort_rule_str[] = {
    "typos",
    "proximity",
    "position",
    "exact",
    "field",
    "geo"
};

SORT_RULE rulestr_to_rule(const char *str) {
    for (int i = 0; i < R_DONE; i++) {
        if (strcmp(str, sort_rule_str[i]) == 0) {
            return i;
        }
    }
    return R_MAX;
}

const char *rule_to_rulestr(SORT_RULE rule) {
    if (rule >= R_DONE) return NULL;
    return sort_rule_str[rule];
}

static inline bool __nsort_lt(docrank_t a, docrank_t b, SORT_RULE *rules) {
    if (a.typos == b.typos) {
        if (a.proximity == b.proximity) {
            if (a.position == b.position) {
                if (a.exact == b.exact) {
                    return a.compare > b.compare;
                } else return (a.exact > b.exact);
            } else return (a.position < b.position);
        } else return (a.proximity < b.proximity);
    } else return (a.typos < b.typos);
}

static inline bool __sort_lt(docrank_t a, docrank_t b, SORT_RULE *rules) {
    int c = 0;
    while (1) {
        switch (rules[c]) {
           case R_TYPO:
                if (a.typos != b.typos) {
                    return a.typos < b.typos;
                }
                break;
            case R_PROX:
                if (a.proximity != b.proximity) {
                    return a.proximity < b.proximity;
                }
                break;
            case R_POS:
                if (a.position != b.position) {
                    return a.position < b.position;
                }
                break;
            case R_EXACT:
                if (a.exact != b.exact) {
                    return a.exact > b.exact;
                }
                break;
            case R_COMP:
                if (a.compare != b.compare) {
                    return a.compare > b.compare;
                }
                break;
            case R_COMP_ASC:
                if (a.compare != b.compare) {
                    return a.compare < b.compare;
                }
                break;
            case R_FIELD:
                if (a.field != b.field) {
                    return a.field < b.field;
                }
                break;
            case R_GEO:
                if (a.distance != b.distance) {
                    return a.distance < b.distance;
                }
                break;
            case R_DONE:
                return false;
                break;
            case R_MAX:
                return false;
                break;
        }
        c++;
    }
    return false;
}


static inline void ks_insertsort_d(docrank_t *s, docrank_t *t, SORT_RULE *rules)			
{																	
    docrank_t *i, *j, swap_tmp;									
    for (i = s + 1; i < t; ++i)									
        for (j = i; j > s && __sort_lt(*j, *(j-1), rules); --j) {	
            swap_tmp = *j; *j = *(j-1); *(j-1) = swap_tmp;	
        }												
}	

static void ks_combsort_d(size_t n, docrank_t *a, SORT_RULE *rules) {
    const double shrink_factor = 1.2473309501039786540366528676643; 
    int do_swap;												
    size_t gap = n;											
    docrank_t tmp, *i, *j;									
    do {											
        if (gap > 2) {							
            gap = (size_t)(gap / shrink_factor);					
            if (gap == 9 || gap == 10) gap = 11;					
        }															
        do_swap = 0;												
        for (i = a; i < a + n - gap; ++i) {							
            j = i + gap;											
            if (__sort_lt(*j, *i, rules)) {								
                tmp = *i; *i = *j; *j = tmp;						
                do_swap = 1;										
            }														
        }														
    } while (do_swap || gap > 2);							
    if (gap != 1) ks_insertsort_d(a, a + n, rules);		
}

void rank_sort(size_t n, docrank_t *a, SORT_RULE *rules) {
    int d;															
    ks_isort_stack_t *top, *stack;									
    docrank_t rp, swap_tmp;											
    docrank_t *s, *t, *i, *j, *k;										

    if (n < 1) return;												
    else if (n == 2) {											
        if (__sort_lt(a[1], a[0], rules)) { swap_tmp = a[0]; a[0] = a[1]; a[1] = swap_tmp; } 
        return;														
    }																
    for (d = 2; 1ul<<d < n; ++d);									
    stack = (ks_isort_stack_t*)malloc(sizeof(ks_isort_stack_t) * ((sizeof(size_t)*d)+2)); 
    top = stack; s = a; t = a + (n-1); d <<= 1;						
    while (1) {													
        if (s < t) {										
            if (--d == 0) {								
                ks_combsort_d(t - s + 1, s, rules);	
                t = s;						
                continue;				
            }							
            i = s; j = t; k = i + ((j-i)>>1) + 1;					
            if (__sort_lt(*k, *i, rules)) {						
                if (__sort_lt(*k, *j, rules)) k = j;			
            } else k = __sort_lt(*j, *i, rules)? i : j;	
            rp = *k;						
            if (k != t) { swap_tmp = *k; *k = *t; *t = swap_tmp; }	
            for (;;) {										
                do ++i; while (__sort_lt(*i, rp, rules));		
                do --j; while (i <= j && __sort_lt(rp, *j, rules));		
                if (j <= i) break;							
                swap_tmp = *i; *i = *j; *j = swap_tmp;				
            }														
            swap_tmp = *i; *i = *t; *t = swap_tmp;					
            if (i-s > t-i) {										
                if (i-s > 16) { top->left = s; top->right = i-1; top->depth = d; ++top; } 
                s = t-i > 16? i+1 : t;								
            } else {												
                if (t-i > 16) { top->left = i+1; top->right = t; top->depth = d; ++top; } 
                t = i-s > 16? i-1 : s;								
            }														
        } else {													
            if (top == stack) {									
                free(stack);								
                ks_insertsort_d(a, a+n, rules);						
                return;											
            } else { --top; s = (docrank_t*)top->left; t = (docrank_t*)top->right; d = top->depth; } 
        }															
    }															
}																	

static inline size_t qspartition_d(docrank_t *a, size_t s, size_t e, size_t p, SORT_RULE *rules) {
    docrank_t pv = a[p];
    size_t i=s;
    size_t fi=s;
    KSORT_SWAP(docrank_t, a[e], a[p]);
    for (;i<e; ++i) { 
        if (__sort_lt(a[i], pv, rules)) { 
            KSORT_SWAP(docrank_t, a[i], a[fi]);
            ++fi;
        }
    }
    KSORT_SWAP(docrank_t, a[fi], a[e]);
    return fi;
}

static inline size_t medianofthree_d(docrank_t *a, size_t s, size_t e, SORT_RULE *rules) {
    const size_t m = (s+e)/2;
    if (!__sort_lt(a[s], a[m], rules)) KSORT_SWAP(docrank_t, a[m], a[s]);
    if (!__sort_lt(a[m], a[e], rules)) KSORT_SWAP(docrank_t, a[m], a[e]);
    if (!__sort_lt(a[s], a[e], rules)) KSORT_SWAP(docrank_t, a[s], a[e]);
    return m;
}

void rank_partial_sort(docrank_t *a, size_t l, size_t r, size_t m, SORT_RULE *rules){
    if (l == r) return; 
    const size_t p = qspartition_d(a, l, r, medianofthree_d(a, l, r, rules), rules); 
    if (p == m) return; 
    else if (m < p) return rank_partial_sort(a, l, p-1, m, rules); 
    else return rank_partial_sort(a, p+1, r, m, rules);
}

