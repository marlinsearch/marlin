#ifndef __CONT_H
#define __CONT_H
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>

#define CUTOFF (1<<12)
#define BCUTOFF (1<<12)/4

#define ID 0
#define CARDINALITY 1

#define BITSET_CONT_TYPE 1
#define ARRAY_CONT_TYPE  2

// macro for pairing container type codes
#define CONT_PAIR(c1, c2) (4 * (c1) + (c2))


typedef void (*bmap_iterator)(uint32_t value, void *param);

struct cont {
    uint16_t *buffer;
};

struct cont *cont_new(uint16_t id);
bool cont_init(struct cont *c, const uint16_t id);
void cont_add(struct cont *c, const uint16_t id);
uint32_t cont_cardinality(const struct cont *c);
bool cont_remove(struct cont *c, const uint16_t item);
void cont_iterate(const struct cont *c, bmap_iterator iter, void *param);
void cont_free(struct cont *);
struct cont *cont_and(const struct cont *a, const struct cont *b);
struct cont *cont_andnot(const struct cont *a, const struct cont *b);
void cont_inplace_and(struct cont *a, const struct cont *b);
void cont_inplace_union(struct cont *a, const struct cont *b);
void bitset_cont_inplace_union(struct cont *a, const struct cont *b);
uint32_t cont_and_cardinality(const struct cont *a, const struct cont *b);
uint16_t *cont_duplicate(const struct cont *c);
bool cont_exists(const struct cont *c, uint16_t item);
void cont_invert(struct cont *c, const struct cont *input);
void bitset_cont_to_array(struct cont *c);

void bitset_cont_cardinality(struct cont *c);
uint16_t cont_get_first(const struct cont *c);


#endif
