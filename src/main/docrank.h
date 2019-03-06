#ifndef __DOCRANK_H_
#define __DOCRANK_H_
#include <stdint.h>
#include "squery.h"

typedef struct docrank {
    double compare;
    uint32_t docid;
    uint16_t attributes;
    uint16_t proximity;
    uint8_t typos;
    uint8_t exact;
    uint8_t field;
    uint8_t pad;
} docrank_t;

struct rank_iter {
    struct bmap *dbmap;
    struct docrank *ranks;
    struct squery *sq;
    uint32_t rankpos;
};

struct docrank *perform_ranking(struct squery *sq, struct bmap *docid_map, uint32_t *resultcount);
#endif
