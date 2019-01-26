#include "bmap.h"
#include <string.h>
#include <stdio.h>


static inline uint16_t highbits(uint32_t i) {
    return i>>16;
}

static inline uint16_t lowbits(uint32_t i) {
    return (i & 0xFFFF);
}

static int binary_search(struct bmap *b, uint16_t id) {
    int low = 0, high = b->num_c-1, middle;
    // Usually we add to the end, handle that
    if (b->num_c > 0) {
        if (b->c[high].buffer[0] == id) return high;        
        if (b->c[high].buffer[0] < id) return -b->num_c - 1;
    }
    while (low <= high) {
        middle = (low+high)/2;
        if (b->c[middle].buffer[0] < id) {
            low = middle+1;
        } else if (b->c[middle].buffer[0] > id){
            high = middle-1;
        } else {
            return middle;
        }
    }
    return -(low+1);
}

static void bmap_cont_remove(struct bmap *b, int pos) {
    free(b->c[pos].buffer);
    b->num_c--;
    if (pos != b->num_c) {
        memmove(&b->c[pos], &b->c[pos+1], (b->num_c-pos)*sizeof(struct cont));
    }
}

static int bmap_cont_add(struct bmap *b, struct cont *c) {
    int pos = binary_search(b, c->buffer[ID]);
    if (pos >= 0) {
        return pos;
    }
    pos = -pos-1;
    b->num_c++;
    b->c = realloc(b->c, (b->num_c)*sizeof(struct cont));
    if (b->num_c-1 > pos) {
        memmove(b->c+pos+1, b->c+pos, (b->num_c-1-pos)*sizeof(struct cont));
    }
    b->c[pos].buffer = c->buffer;
    return pos;
}

static inline int bmap_advance(const struct bmap *b, uint16_t id, int pos) {
    while(++pos < b->num_c) {
        if (b->c[pos].buffer[ID] >= id) return pos;
    }
    return pos;
}

static inline int bmap_advance_free(struct bmap *b, uint16_t id, int pos) {
    while(++pos < b->num_c) {
        if (b->c[pos].buffer[ID] >= id) {
            return pos;
        } else {
            bmap_cont_remove(b, pos);
            pos--;
        }
    }
    return pos;
}

static inline void bmap_insert_container(struct bmap *b, int pos) {
    b->num_c++;
    b->c = realloc(b->c, (b->num_c)*sizeof(struct cont));
    if (b->num_c-1 > pos) {
        memmove(b->c+pos+1, b->c+pos, (b->num_c-1-pos)*sizeof(struct cont));
    }
}

static uint16_t *bmap_container_to_bitset(const struct bmap *b, int pos) {
    if (cont_cardinality(&b->c[pos]) > CUTOFF) {
        uint16_t *buffer = malloc((CUTOFF + 2) * sizeof(uint16_t));
        memcpy(buffer, b->c[pos].buffer, (CUTOFF + 2) * sizeof(uint16_t));
        return buffer;
    }
    uint16_t *buffer = calloc((CUTOFF + 2), sizeof(uint16_t));
    buffer[ID] = b->c[pos].buffer[ID];
    const struct cont *c = &b->c[pos];
    int card = c->buffer[CARDINALITY];
    buffer[CARDINALITY] = card;
    uint16_t *buf = buffer+2;
    for (int i=2; i<card+2; i++) {
        buf[c->buffer[i]>>4] |= 1 << (c->buffer[i] & 0xF);
    }
    return buffer;
}

// Assumes x1 is a bitset container
static inline void bmap_inplace_lazy_or(struct bmap *x1, const struct bmap *x2) {
    int l2 = x2->num_c;
    if (l2 == 0) {
        return;
    }
    int l1 = x1->num_c;
    int pos1 = 0, pos2 = 0;
    uint16_t s1 = x1->c[pos1].buffer[ID];
    uint16_t s2 = x2->c[pos2].buffer[ID];

    while(true) {
        if (s1 == s2) {
            cont_inplace_union(&x1->c[pos1], &x2->c[pos2]);
            ++pos1;
            ++pos2;
            if (pos1 == l1) break;
            if (pos2 == l2) break;
            s1 = x1->c[pos1].buffer[ID];
            s2 = x2->c[pos2].buffer[ID];
        } else if (s1 < s2) {
            pos1++;
            if (pos1 == l1) break;
            s1 = x1->c[pos1].buffer[ID];
        } else { // s1 > s2
            bmap_insert_container(x1, pos1);
            x1->c[pos1].buffer = bmap_container_to_bitset(x2, pos2);
            pos1++;
            l1++;
            pos2++;
            if (pos2 == l2) break;
            s2 = x2->c[pos2].buffer[ID];
        }
    }
    // Copy x2 to x1
    if (pos1 == l1) {
       for (int i=pos2; i<l2; i++) {
            pos1++;
            bmap_insert_container(x1, pos1);
            x1->c[pos1-1].buffer = bmap_container_to_bitset(x2, i);
        }
    }
}


uint32_t bmap_and_cardinality(const struct bmap *a, const struct bmap *b) {
    uint32_t card = 0;
    int pos1 = 0, pos2 = 0;
    const int length1 = a->num_c, length2 = b->num_c;

    while(pos1 < length1 && pos2 < length2) {
        const uint16_t id1 = a->c[pos1].buffer[0];
        const uint16_t id2 = b->c[pos2].buffer[0];

        if (id1 == id2) {
            card += cont_and_cardinality(&a->c[pos1], &b->c[pos2]);
            ++pos1;
            ++pos2;
        } else if (id1 < id2) {
            pos1 = bmap_advance(a, id2, pos1);
        } else {
            pos2 = bmap_advance(b, id1, pos2);
        }
    }
    return card;
}

struct bmap *bmap_duplicate(const struct bmap *a) {
    struct bmap *b = bmap_new();
    memcpy(b, a, sizeof(struct bmap));
    b->c = malloc(b->num_c * sizeof(struct cont));
    memcpy(b->c, a->c, b->num_c * sizeof(struct cont));
    for (int i = 0; i < b->num_c; i++) {
        b->c[i].buffer = cont_duplicate(&a->c[i]);
    }
    return b;
}

struct bmap *convert_to_bitset_bmap(const struct bmap *f) {
    struct bmap *r = bmap_new();
    r->needs_free = 1;
    r->num_c = f->num_c;
    r->c = malloc(f->num_c * sizeof(struct cont));
    memcpy(r->c, f->c, f->num_c*sizeof(struct cont));
    for (int i=0; i<f->num_c; i++) {
        // THis is a bitset, just blindly copy it
        if (cont_cardinality(&f->c[i]) > CUTOFF) {
            r->c[i].buffer = malloc((CUTOFF+2) * sizeof(uint16_t));
            memcpy(r->c[i].buffer, f->c[i].buffer, (CUTOFF+2)*sizeof(uint16_t));
        } else {
            // Array, build a new bitmap
            uint16_t *buffer = calloc((CUTOFF+2), sizeof(uint16_t));
            const struct cont *c = &f->c[i];
            // Copy over id and cardinality
            buffer[ID] = c->buffer[ID];
            buffer[CARDINALITY] = c->buffer[CARDINALITY];
            int card = buffer[CARDINALITY];
            buffer = buffer+2;
            for (int i=2; i<card+2; i++) {
                buffer[c->buffer[i]>>4] |= 1 << (c->buffer[i] & 0xF);
            }
            r->c[i].buffer = buffer-2;
        }
    }
    return r;
}

struct bmap *bmap_and(const struct bmap *a, const struct bmap *b) {
    struct bmap *r = bmap_new();
    r->needs_free = 1;
    int pos1 = 0, pos2 = 0;
    const int length1 = a->num_c, length2 = b->num_c;

    while(pos1 < length1 && pos2 < length2) {
        const uint16_t id1 = a->c[pos1].buffer[ID];
        const uint16_t id2 = b->c[pos2].buffer[ID];
        // printf("id2 %d id2 %d\n", id1, id2);
        if (id1 == id2) {
            struct cont *c = cont_and(&a->c[pos1],  &b->c[pos2]);
            if (cont_cardinality(c) != 0) {
                bmap_cont_add(r, c);
            } else {
                free(c->buffer);
            }
            free(c);
            ++pos1;
            ++pos2;
        } else if (id1 < id2) {
            pos1 = bmap_advance(a, id2, pos1);
        } else {
            pos2 = bmap_advance(b, id1, pos2);
        }
    }
    return r;
}

void bmap_and_inplace(struct bmap *a, const struct bmap *b) {
    int pos1 = 0, pos2 = 0;
    int length1 = a->num_c, length2 = b->num_c;
    while(pos1 < length1 && pos2 < length2) {
        const uint16_t id1 = a->c[pos1].buffer[0];
        const uint16_t id2 = b->c[pos2].buffer[0];
        // printf("id2 %d id2 %d\n", id1, id2);
        if (id1 == id2) {
            cont_inplace_and(&a->c[pos1], &b->c[pos2]);
            // printf("card1 %d card2 %d card3 %d\n", get_cont_cardinality(&a->c[pos1]), get_cont_cardinality(&b->c[pos2]), get_cont_cardinality(c));
            // printf("a bitset %d b bitset %d \n", a->is_bitset, b->is_bitset);
            if (cont_cardinality(&a->c[pos1]) == 0) {
                bmap_cont_remove(a, pos1);
                length1--;
                pos1--;
            }
            ++pos1;
            ++pos2;
        } else if (id1 < id2) {
            pos1 = bmap_advance_free(a, id2, pos1);
        } else {
            pos2 = bmap_advance(b, id1, pos2);
        }
    }

    if (pos1 < a->num_c) {
        for (int i=pos1; i<a->num_c; i++) {
            free(a->c[pos1].buffer);
        }
        a->num_c = pos1;
    }
}


void bmap_add(struct bmap *b, uint32_t item) {
    uint16_t id = highbits(item);
    uint16_t val = lowbits(item);
    int pos = binary_search(b, id);
    if (pos >= 0) {
        cont_add(&b->c[pos], val);
        return;
    }
    pos = -pos-1;
    b->num_c++;
    b->c = realloc(b->c, (b->num_c)*sizeof(struct cont));
    if (b->num_c-1 > pos) {
        memmove(b->c+pos+1, b->c+pos, (b->num_c-1-pos)*sizeof(struct cont));
    }
    cont_init(&b->c[pos], id);
    cont_add(&b->c[pos], val);
}

void bmap_remove(struct bmap *b, uint32_t item) {
    uint16_t id = highbits(item);
    uint16_t val = lowbits(item);
    int pos = binary_search(b, id);
    if (pos >= 0) {
        bool remove = cont_remove(&b->c[pos], val);
        // If remove is true, the container itself has to be removed !
        if (remove) {
            b->num_c--;
            if (pos != b->num_c) {
                memmove(&b->c[pos], &b->c[pos+1], (b->num_c-pos)*sizeof(struct cont));
            }
        }
    }
}

uint32_t bmap_cardinality(const struct bmap *b) {
    uint32_t cardinality = 0;
    for (int i=0; i<b->num_c; i++) {
        cardinality += cont_cardinality(&b->c[i]);
    }
    return cardinality;
}

void bmap_iterate(const struct bmap *b, bmap_iterator iter, void *ptr) {
    for (int i=0; i<b->num_c; i++) {
        cont_iterate(&b->c[i], iter, ptr);
    }
}

void bmap_free_containers(struct bmap *b) {
    if (!b->mdb_bmap) {
        for (int i=0; i<b->num_c; i++) {
            free(b->c[i].buffer);
        }
    }
    free(b->c);
    b->c = NULL;
    b->num_c = 0;
}

void bmap_free(struct bmap *b) {
    if (!b) return;
    bmap_free_containers(b);
    free(b);
    b = NULL;
}

struct bmap *bmap_new() {
    struct bmap *b = calloc(1, sizeof(struct bmap));
    return b;
}




// BITMAP operations
//

struct oper *oper_new() {
    struct oper *o = calloc(1, sizeof(struct oper));
    return o;
}

// frees only the operation, does not care about stored bitmaps
void oper_free(struct oper *o) {
    free(o->b);
    free(o);
    o = NULL;
}

void oper_add(struct oper *o, struct bmap *b) {
    o->count++;
    o->b = realloc(o->b, o->count*sizeof(struct bmap *));
    o->b[o->count-1] = b;
}

struct bmap *oper_and(const struct oper *o) {
    switch (o->count) {
        case 0: {
                struct bmap *r = bmap_new();
                r->needs_free = 1;
                return r;
            }
            break;
        case 1:
            return bmap_duplicate(o->b[0]);
            break;
        case 2:
            return bmap_and(o->b[0], o->b[1]);
            break;
        default: {
                struct bmap *r = bmap_and(o->b[0], o->b[1]);
                for (int i=2; i<o->count; i++) {
                    bmap_and_inplace(r, o->b[i]);
                }
                return r;
            }
    }
}

/*
static void dump_cards(struct bmap *r, char *msg) {
    printf("**** Dump cards %s\n", msg);
    for (int i=0; i<r->num_c; i++) {
        struct cont *c = &r->c[i];
        // Calculate cardinality
        bitset_cont_cardinality(c);
        printf("dump card %d %d\n", i, cont_cardinality(c));
    }
    printf("\n");
}
*/

struct bmap *oper_or(const struct oper *o) {
#ifdef TRACE_BMAP
    printf("OR OPERATION : %d entries\n", o->count);
#endif
    struct bmap *r = NULL;
    int end = 0;
    // Iterate till first proper bmap
    while (end < o->count) {
        if (o->b[end++]->num_c) {
            r = convert_to_bitset_bmap(o->b[0]);
            break;
        }
    }
    if (!r) {
        r = bmap_new();
        r->needs_free = 1;
        return r;
    }

    for (int i=end; i<o->count; i++) {
        bmap_inplace_lazy_or(r, o->b[i]);
    }

    for (int i=0; i<r->num_c; i++) {
        struct cont *c = &r->c[i];
        // Calculate cardinality
        bitset_cont_cardinality(c);
        // convert bitset container to proper containers
        if (cont_cardinality(c) <= CUTOFF) {
            uint16_t *nb = malloc((2 + cont_cardinality(c)) * sizeof(uint16_t));
            int p = 2;
            uint32_t base = c->buffer[ID] << 16;
            const uint64_t *buffer = (const uint64_t *)&c->buffer[2];
            for (int i=0; i<BCUTOFF; i++) {
                uint64_t w = buffer[i];
                while (w != 0) {
                    uint64_t t = w & (~w + 1);
                    int r = __builtin_ctzll(w);
                    nb[p++] = r+base;
                    w ^= t;
                }
                base += 64;
            }
            nb[ID] = c->buffer[ID];
            nb[CARDINALITY] = c->buffer[CARDINALITY];
            free(c->buffer);
            c->buffer = nb;
        }
    }
    return r;
}

// Returns the first position set
// NOTE : Returns 0xFFFF if nothing is found
uint32_t bmap_get_first(struct bmap *b) {
    uint32_t f = 0xFFFF;
    if (b->num_c) {
        struct cont *c = &b->c[0];
        return ((uint32_t)c->buffer[0] << 16) | (uint32_t)cont_get_first(c);
    }
    return f;
}

uint32_t bmap_get_dumplen(const struct bmap *b) {
    uint32_t mlen = 1; // To store num of items
    for (int i=0; i<b->num_c; i++) {
        mlen += 2; // id + cardinality
        mlen += (cont_cardinality(&b->c[i]) > CUTOFF)?CUTOFF:b->c[i].buffer[1];
    }
    return mlen * sizeof(uint16_t);
}

// Assumes that buf is big enough to dump all data 
void bmap_dump(const struct bmap *b, uint16_t *buf) {
    buf[0] = b->num_c;
    buf++;
    for (int i=0; i<b->num_c; i++) {
        int len = (cont_cardinality(&b->c[i]) > CUTOFF) ? CUTOFF : b->c[i].buffer[1];
        len += 2; // id + card
        memcpy(buf, b->c[i].buffer, len*sizeof(uint16_t));
        buf += len;
    }
}

void bmap_load(struct bmap *b, const uint16_t *buf) {
    b->num_c = *buf;
    b->c = calloc(b->num_c, sizeof(struct cont));
    buf++;
    for (int i=0; i<b->num_c; i++) {
        uint32_t card = buf[1];
        if (card == 0) {
            if (buf[2] == 0xFFFF) {
                card = 65536;
            }
        }
        int len = ((card > CUTOFF)?CUTOFF:buf[1])+2; // 2 for id and cardinality
        b->c[i].buffer = malloc(sizeof(uint16_t)*len);
        memcpy(b->c[i].buffer, buf, len*sizeof(uint16_t));
        buf += len;
    }
}


