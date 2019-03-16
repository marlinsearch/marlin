#ifndef __FILTER_H_
#define __FILTER_H_

#include "common.h"
#include "index.h"


typedef enum filter_type {
    F_AND,// and
    F_OR, // OR or IN
    F_NIN,// Not in
    F_EQ, // Equals
    F_NE, // Not equal
    F_GT, // Greater than
    F_GTE, // Greater than equals
    F_LT, // Lesser than
    F_LTE, // Lesser than equals
    F_RANGE, // Numeric range comparison
    F_ERROR // Error occured while parsing
} FILTER_TYPE;

KHASH_MAP_INIT_STR(foper, FILTER_TYPE) // Filter operator to filtertype mapping

struct filter {
    FILTER_TYPE type;
    F_TYPE field_type;
    const struct schema *s;
    struct bmap *fr_bmap;               // Filter result bmap
    kvec_t(struct filter *) children;   // Children for this filter
    char error[128];                    // A error string to hold filter parse errors

    const char *strval;
    double numval;
    // Below fields are only applicable for numeric comparisons
    double numval2;
    FILTER_TYPE numcmp1;
    FILTER_TYPE numcmp2;
};


struct filter *parse_filter(struct index *in, json_t *j);
void filter_free(struct filter *f);
void init_filters(void);
void dump_filter(struct filter *f, int indent);
void init_filter_callbacks(void);
struct filter *filter_dup(const struct filter *f);

#endif

