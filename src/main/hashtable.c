// Got this from somewhere, not sure where :( 
// This is copyrighted to whomsoever who wrote this code.
#include <stdlib.h>
#include <string.h>
#include "hashtable.h"
#include "utils.h"

#define FIRST_CELL(h, hash) (h->m_cells + ((hash) & (h->m_arraySize - 1)))
#define CIRCULAR_NEXT(h, c) ((c) + 1 != h->m_cells + h->m_arraySize ? (c) + 1 : h->m_cells)

// from code.google.com/p/smhasher/wiki/MurmurHash3
static inline uint32_t integerHash(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

//----------------------------------------------
//  HashTable::HashTable
//----------------------------------------------
struct hashtable *hashtable_new(uint32_t size) {
    // Initialize regular cells
    struct hashtable *h = calloc(1, sizeof(struct hashtable));
    h->m_arraySize = size;
    h->m_cells = calloc(h->m_arraySize, sizeof(struct cell));
    h->m_population = 0;
    return h;
}

struct cell *hashtable_lookup(struct hashtable *h, uint32_t key) {
    // Check regular cells
    for (struct cell* cell = FIRST_CELL(h, integerHash(key));; cell = CIRCULAR_NEXT(h, cell)) {
        if (cell->key == key) return cell;
        if (!cell->key) return NULL;
    }
};

static void repopulate(struct hashtable *h, uint32_t desiredSize) {
    // Get start/end pointers of old array
    struct cell *oldCells = h->m_cells;
    struct cell *end = h->m_cells + h->m_arraySize;

    h->m_arraySize = desiredSize;
    h->m_cells = calloc(h->m_arraySize, sizeof(struct cell));

    // Iterate through old array
    for (struct cell *c = oldCells; c != end; c++) {
        if (c->key) {
            // Insert this element into new array
            for (struct cell *cell = FIRST_CELL(h, integerHash(c->key));; cell = CIRCULAR_NEXT(h, cell)) {
                if (!cell->key) {
                    // Insert here
                    *cell = *c;
                    break;
                }
            }
        }
    }
    free(oldCells);
}

struct cell* hashtable_insert(struct hashtable *h, uint32_t key) {
    for (;;) {
        for (struct cell* cell = FIRST_CELL(h, integerHash(key));; cell = CIRCULAR_NEXT(h, cell)) {
            if (cell->key == key)
                return cell;        // Found
            if (cell->key == 0) {
                // Insert here
                if ((h->m_population + 1) * 4 >= h->m_arraySize * 3) {
                    // Time to resize
                    repopulate(h, h->m_arraySize * 2);
                    break;
                }
                ++h->m_population;
                cell->key = key;
                return cell;
            }
        }
    }
}

struct hashtable *hashtable_dup(struct hashtable *h) {
    struct hashtable *newh = malloc(sizeof(struct hashtable));
    memcpy(newh, h, sizeof(struct hashtable));
    newh->m_cells = malloc(sizeof(struct cell) * h->m_arraySize);
    memcpy(newh->m_cells, h->m_cells, sizeof(struct cell)*h->m_arraySize);

    return newh;
}

void hashtable_free(struct hashtable *h) {
    free(h->m_cells);
    free(h);
}

