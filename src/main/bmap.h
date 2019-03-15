#ifndef __BMAP_H
#define __BMAP_H

#include "cont.h"

struct bmap {
    struct cont *c;
    uint16_t num_c;
    uint8_t needs_free:1; // Does this bitset need to be freed at the end of a query?
    uint8_t mdb_bmap:1; // Is this bitmap a lmdb memory bitmap
};

struct bmap *bmap_new();
void bmap_add(struct bmap *b, uint32_t item);
void bmap_remove(struct bmap *b, uint32_t item);
void bmap_iterate(const struct bmap *b, bmap_iterator iter, void *ptr);
void bmap_free(struct bmap *b);
void bmap_free_containers(struct bmap *b);
uint32_t bmap_cardinality(const struct bmap *b);
struct bmap *bmap_and(const struct bmap *a, const struct bmap *b);
uint32_t bmap_and_cardinality(const struct bmap *a, const struct bmap *b);
struct bmap *convert_to_bitset_bmap(const struct bmap *f);
struct bmap *bmap_duplicate(const struct bmap *b);
uint32_t bmap_get_first(struct bmap *b);
uint32_t bmap_get_dumplen(const struct bmap *b);
void bmap_dump(const struct bmap *b, uint16_t *buf); // Dump to buf
void bmap_load(struct bmap *b, const uint16_t *buf); // Load from buf
bool bmap_exists(const struct bmap *b, uint32_t item);
struct bmap *bmap_invert(const struct bmap *b, const struct bmap *input);

struct oper {
    int count;
    struct bmap **b;
};

struct oper *oper_new();
void oper_add(struct oper *o, struct bmap *b);
struct bmap *oper_and(const struct oper *o);
struct bmap *oper_or(const struct oper *o);
void oper_free(struct oper *o);
void oper_total_free(struct oper *o);

#endif
