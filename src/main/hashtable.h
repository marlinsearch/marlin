#ifndef __HASHTABLE_H__
#define __HASHTABLE_H__

#include <inttypes.h>
#include <pthread.h>

struct cell {
    uint32_t key;
    uint32_t value;
};

struct hashtable {
    struct cell* m_cells;
    uint32_t m_arraySize;
    uint32_t m_population;
};

struct hashtable *hashtable_new(uint32_t initial_size);
struct hashtable *hashtable_dup(struct hashtable *h);
void hashtable_free(struct hashtable *h);

// Basic operations
struct cell* hashtable_lookup(struct hashtable *h, uint32_t key);
struct cell* hashtable_insert(struct hashtable *h, uint32_t key);

#endif
