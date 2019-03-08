#include "cont.h"
#include "platform.h"
#include <stdio.h>
#include <string.h>


static bool exists_array(const struct cont *c, uint16_t item) {
    int first = 0, last = c->buffer[CARDINALITY]-1, middle = (first+last)/2;
    uint16_t *buffer = &c->buffer[2];
    while (first <= last) {
        if (buffer[middle] == item) return true;
        if (buffer[middle] < item) {
            first = middle+1;
        } else {
            last = middle-1;
        }
        middle = (first+last)/2;
    }
    return false;
}

static void array_to_bitset(struct cont *c) {
    uint16_t *nb = calloc(CUTOFF, sizeof(uint16_t));
    if (!nb) {
        printf("Calloc failure !\n");
        return;
    }
    for (int i=2; i<CUTOFF+2; i++) {
        nb[c->buffer[i]>>4] |= 1 << (c->buffer[i] & 0xF);
    }
    memcpy(&c->buffer[2], nb, CUTOFF*sizeof(uint16_t));
    free(nb);
}

static int binary_search(struct cont *c, uint16_t item) {
    int low = 0, high = c->buffer[CARDINALITY]-1, middle;
    uint16_t *buffer = &c->buffer[2];
    // Usually we add to the end, handle that
    if ((c->buffer[CARDINALITY] > 0) &&  buffer[high]<item) {
        return -c->buffer[CARDINALITY] - 1;
    }
    while (low <= high) {
        middle = (low+high)/2;
        if (buffer[middle] < item) {
            low = middle+1;
        } else if (buffer[middle] > item){
            high = middle-1;
        } else {
            return middle;
        }
    }
    return -(low+1);
}

static void array_add(struct cont *c, uint16_t item) {
    int i = binary_search(c, item);
    if (i < 0) {
        i = -i-1;
        c->buffer[CARDINALITY]++;
        c->buffer = realloc(c->buffer, (c->buffer[CARDINALITY]+2)*sizeof(uint16_t));
        if (!c->buffer) {
            printf("Realloc failure !\n");
            return;
        }
        uint16_t *buffer = &c->buffer[2];
        if (c->buffer[CARDINALITY]-1 > i) {
            memmove(buffer+i+1, buffer+i, ((c->buffer[CARDINALITY]-1)-i)*sizeof(uint16_t));
        }
        buffer[i] = item;
    }
}

static inline void bitset_add(struct cont *c, uint16_t item) {
    c->buffer[CARDINALITY]++;
    c->buffer[(item>>4)+2] |= 1 << (item & 0xF);
}

static inline bool exists_bitset(const struct cont *c, uint16_t item) {
    return c->buffer[(item>>4)+2] & (1 << (item & 0xF));
}

static void bitset_remove(struct cont *c, uint16_t item) {
    c->buffer[CARDINALITY]--;
    c->buffer[(item>>4)+2] &= ~(1 << (item & 0xF));
}

static void array_remove(struct cont *c, uint16_t item) {
    int i = binary_search(c, item);
    if (i >= 0) {
        c->buffer[CARDINALITY]--;
        uint16_t *buffer = &c->buffer[2];
        if (i != c->buffer[CARDINALITY]) {
            memmove(buffer+i, buffer+i+1, (c->buffer[CARDINALITY]-i)*sizeof(uint16_t));
        }
    }
}

static void bitset_to_array(struct cont *c) {
    uint16_t *nb = calloc(CUTOFF, sizeof(uint16_t));
    int p = 0;

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
    memcpy(&c->buffer[2], nb, CUTOFF*sizeof(uint16_t));
    free(nb);
}

static inline bool cont_is_array(const struct cont *c) {
    if (cont_cardinality(c) > CUTOFF) {
        return false;
    }
    return true;
}

static inline int cont_type(const struct cont *c) {
    if (cont_cardinality(c) > CUTOFF) {
        return BITSET_CONT_TYPE;
    }
    return ARRAY_CONT_TYPE;
}

static inline int bitset_bitset_and_cardinality(const struct cont *a, const struct cont *b) {
    const uint64_t *al = (const uint64_t *)&a->buffer[2];
    const uint64_t *bl = (const uint64_t *)&b->buffer[2];
    int card = 0;
    for (int i=0; i<1024; i+=2) {
        card += __builtin_popcountll(al[i] & bl[i]);
        card += __builtin_popcountll(al[i+1] & bl[i+1]);
    }
    return card;
}

static struct cont *bitset_bitset_and(const struct cont *a, const struct cont *b) {
    struct cont *c = malloc(sizeof(struct cont));
    int cardinality = bitset_bitset_and_cardinality(a, b);
    const uint64_t *al = (const uint64_t *)&a->buffer[2];
    const uint64_t *bl = (const uint64_t *)&b->buffer[2];

    // printf("b b and card %d\n", cardinality);
    if (cardinality > CUTOFF) {
        c->buffer = malloc((CUTOFF+2) * sizeof(uint16_t));
        uint64_t *cl = (uint64_t *)&c->buffer[2];
        for (int i=0; i<1024; i+=2) {
            cl[i] = (al[i] & bl[i]);
            cl[i+1] = (al[i+1] & bl[i+1]);
        }
    } else {
        c->buffer = malloc((cardinality+2) * sizeof(uint16_t));
        int pos = 2;
        int base = 0;
        for (int i=0; i<BCUTOFF; i++) {
            uint64_t w = al[i] & bl[i];
            while (w != 0) {
                uint64_t t = w & (~w + 1);
                int r = __builtin_ctzll(w);
                c->buffer[pos++] = base + r;
                w ^= t;
            }
            base += 64;
        }
    }

    c->buffer[ID] = a->buffer[ID];
    c->buffer[CARDINALITY] = cardinality;
    /*
    if (cont_and_cardinality(a, b) != c->buffer[CARDINALITY]) {
        printf("card check failed here 3\n");
        exit(1);
    }
    if (a->buffer[ID] != c->buffer[ID]) {
        printf("Failed id here 1");
        exit(1);
    }*/
    return c;
}

static inline int array_advance(const struct cont *b, int len, uint16_t val, int pos) {
    while(++pos < len) {
        if (b->buffer[pos] >= val) return pos;
    }
    return pos;
}

static struct cont *array_array_and(const struct cont *a, const struct cont *b) {
    const int l1 = a->buffer[CARDINALITY]+2;
    const int l2 = b->buffer[CARDINALITY]+2;
    struct cont *c = malloc(sizeof(struct cont));
    c->buffer = malloc(sizeof(uint16_t) * ((l1 < l2) ? l1 : l2));
    c->buffer[ID] = a->buffer[ID];
    c->buffer[CARDINALITY] = 0;
    c->buffer[CARDINALITY+1] = 0;

    int pos1 = 2;
    int pos2 = 2;
    int pos  = 2;
    while (pos1 < l1 && pos2 < l2) {
        const uint16_t v1 = a->buffer[pos1];
        const uint16_t v2 = b->buffer[pos2];
        if (v1 == v2) {
            c->buffer[CARDINALITY]++;
            c->buffer[pos++] = v1;
            ++pos1;
            ++pos2;
        } else if (v1 < v2) {
            pos1 = array_advance(a, l1, v2, pos1);
        } else {
            pos2 = array_advance(b, l2, v1, pos2);
        }
    }
    /*
    if (cont_and_cardinality(a, b) != c->buffer[CARDINALITY]) {
        printf("card check failed here 2\n");
        exit(1);
    }
    if (a->buffer[ID] != c->buffer[ID]) {
        printf("Failed id here 2");
        exit(1);
    }
    */
    return c;
}

static struct cont *bitset_array_and(const struct cont *a, const struct cont *b) {
    struct cont *c = malloc(sizeof(struct cont));
    if (!c) {
        return NULL;
    }
    c->buffer = malloc(sizeof(uint16_t) * (2 + a->buffer[CARDINALITY]));
    if (!c->buffer) {
        free(c);
        return NULL;
    }
    c->buffer[ID] = a->buffer[ID];
    c->buffer[CARDINALITY] = 0;
    c->buffer[CARDINALITY+1] = 0;

    int pos = 2;
    int len = a->buffer[CARDINALITY] + 2;
    for (int i=2; i<len; i++) {
        if (exists_bitset(b, a->buffer[i])) {
            c->buffer[pos++] = a->buffer[i];
        }
    }
    c->buffer[CARDINALITY] = pos-2;
    /*
    if (cont_and_cardinality(a, b) != c->buffer[CARDINALITY]) {
        printf("card check failed here 1\n");
        exit(1);
    }

    if (a->buffer[ID] != c->buffer[ID]) {
        printf("Failed id here 3");
        exit(1);
    }*/
    return c;
}

struct cont *cont_and(const struct cont *a, const struct cont *b) {
    //printf("Cont and for pair %d\n", CONT_PAIR(cont_type(a), cont_type(b)));
    switch(CONT_PAIR(cont_type(a), cont_type(b))) {
        case CONT_PAIR(BITSET_CONT_TYPE, BITSET_CONT_TYPE):
            return bitset_bitset_and(a, b);
            break;
        case CONT_PAIR(ARRAY_CONT_TYPE, ARRAY_CONT_TYPE):
            return array_array_and(a, b);
            break;
        case CONT_PAIR(BITSET_CONT_TYPE, ARRAY_CONT_TYPE):
            return bitset_array_and(b, a);
            break;
        case CONT_PAIR(ARRAY_CONT_TYPE, BITSET_CONT_TYPE):
            return bitset_array_and(a, b);
            break;
    }
    return NULL;
}

void cont_inplace_and(struct cont *a, const struct cont *b) {
    struct cont *nc = cont_and((const struct cont *)a, b);
    free(a->buffer);
    a->buffer = nc->buffer;
    free(nc);
}

static void array_cont_inplace_union(struct cont *a, const struct cont *b) {
    for (int i=0; i<b->buffer[CARDINALITY]; i++) {
        bitset_add(a, b->buffer[i+2]);
    }
}

void bitset_cont_inplace_union(struct cont *a, const struct cont *b) {
    for (int i=2; i<CUTOFF+2; i++) {
        a->buffer[i] |= b->buffer[i];
    }
}

void cont_inplace_union(struct cont *a, const struct cont *b) {
    if (cont_is_array(b)) {
        array_cont_inplace_union(a, b);
    } else {
        bitset_cont_inplace_union(a, b);
    }
}

static int array_array_and_cardinality(const struct cont *a, const struct cont *b) {
    const int l1 = a->buffer[CARDINALITY];
    const int l2 = b->buffer[CARDINALITY];

    int pos1 = 2;
    int pos2 = 2;
    int result = 0;

    while (pos1 < l1+2 && pos2 < l2+2) {
        const uint16_t v1 = a->buffer[pos1];
        const uint16_t v2 = b->buffer[pos2];
        if (v1 == v2) {
            ++result;
            ++pos1;
            ++pos2;
        } else if (v1 < v2) {
            pos1 = array_advance(a, l1+2, v2, pos1);
        } else {
            pos2 = array_advance(b, l2+2, v1, pos2);
        }
    }
    return result;
}

uint16_t *cont_duplicate(const struct cont *c) {
    uint16_t *buffer;
    if (cont_is_array(c)) {
        int len = (2 + c->buffer[CARDINALITY]) * sizeof(uint16_t);
        buffer = malloc(len);
        memcpy(buffer, c->buffer, len);
    } else {
        int len = (2 + CUTOFF) * sizeof(uint16_t);
        buffer = malloc(len);
        memcpy(buffer, c->buffer, len);
    }
    return buffer;
}

int bitset_array_and_cardinality(const struct cont *a, const struct cont *b) {
    int result = 0;
    for (int i=2; i<a->buffer[CARDINALITY]+2; i++) {
        if (exists_bitset(b, a->buffer[i])) {
            result++;
        }
    }
    return result;
}

uint32_t cont_and_cardinality(const struct cont *a, const struct cont *b) {
    switch(CONT_PAIR(cont_type(a), cont_type(b))) {
        case CONT_PAIR(BITSET_CONT_TYPE, BITSET_CONT_TYPE):
            return bitset_bitset_and_cardinality(a, b);
            break;
        case CONT_PAIR(ARRAY_CONT_TYPE, ARRAY_CONT_TYPE):
            return array_array_and_cardinality(a, b);
            break;
        case CONT_PAIR(BITSET_CONT_TYPE, ARRAY_CONT_TYPE):
            return bitset_array_and_cardinality(b, a);
            break;
        case CONT_PAIR(ARRAY_CONT_TYPE, BITSET_CONT_TYPE):
            return bitset_array_and_cardinality(a, b);
            break;
    }
    return 0;
}


// Removes item from container and returns true when container is empty
bool cont_remove(struct cont *c, uint16_t item) {
    uint32_t card = cont_cardinality(c);
    if (card > CUTOFF) {
        if (!exists_bitset(c, item)) return false;
        bitset_remove(c, item);
    } else {
        if (!exists_array(c, item)) return false;
        array_remove(c, item);
    }
    card--;
    if (card == CUTOFF) {
        bitset_to_array(c);
    }
    // If cardinality was 1 coming in, we are now at 0, return true to
    // remove this container
    // When cardinality is 0, free the buffer
    if (card == 0) {
        free(c->buffer);
        c->buffer = NULL;
    }
    return (card == 0)?true:false;
}


uint32_t cont_cardinality(const struct cont *c) {
    if (c->buffer[CARDINALITY] == 0) {
        if (c->buffer[CARDINALITY+1] == 0xFFFF) return 65536;
    }
    return c->buffer[CARDINALITY];
}

void cont_add(struct cont *c, const uint16_t item) {
    // If it already exists, just bail out
    // Move this down in the if case to speedup
    uint32_t card = cont_cardinality(c);
    if (card == CUTOFF) {
        if (exists_array(c, item)) return;
        array_to_bitset(c);
    }
    if (card < CUTOFF) {
        array_add(c, item);
    } else {
        if (exists_bitset(c, item)) return;
        bitset_add(c, item);
    }
}

bool cont_init(struct cont *c, const uint16_t id) {
    c->buffer = malloc(sizeof(uint16_t) * 3);
    if (UNLIKELY(!c->buffer)) {
        printf("Malloc failure !\n");
        return false;
    }
    c->buffer[ID] = id;
    c->buffer[CARDINALITY] = 0;
    c->buffer[2] = 0;
    return true;
}

void bitset_cont_cardinality(struct cont *c) {
    int card = 0;
    const uint64_t *a = (const uint64_t *)&c->buffer[2];                                       
    for (int i=0; i<1024; i+=4) {                                                              
        card += __builtin_popcountll(a[i]);                                  
        card += __builtin_popcountll(a[i+1]);                                
        card += __builtin_popcountll(a[i+2]);                                
        card += __builtin_popcountll(a[i+3]);                                
    }                                                                                          
    c->buffer[CARDINALITY] = card;
}       

void cont_iterate(const struct cont *c, bmap_iterator iter, void *param) {
    uint32_t base = c->buffer[ID] << 16;

    if (cont_is_array(c)) {
        for (int i=2; i<c->buffer[CARDINALITY]+2; i++) {
            iter(c->buffer[i] + base, param);
        }
    } else {
        const uint64_t *buffer = (const uint64_t *)&c->buffer[2];
        for (int i=0; i<BCUTOFF; i++) {
            uint64_t w = buffer[i];
            while (w != 0) {
                uint64_t t = w & (~w + 1);
                int r = __builtin_ctzll(w);
                iter(r+base, param);
                w ^= t;
            }
            base += 64;
        }
    }
}

void cont_free(struct cont *c) {
    free(c->buffer);
    free(c);
    c = NULL;
}

struct cont *cont_new(uint16_t id) {
    struct cont *c = malloc(sizeof(struct cont));
    if (UNLIKELY(!c)) {
        printf("Malloc failure !\n");
        return NULL;
    }
    if (!cont_init(c, id)) {
        free(c);
        c = NULL;
    }
    return c;
}

// TODO: FIX ISSUES !!!
//
uint16_t cont_get_first(const struct cont *c) {
    uint32_t card = cont_cardinality(c);
    uint16_t item = 0;
    if (UNLIKELY(card == 0)) return item;
    if (card <= CUTOFF) {
        // Array container
        return c->buffer[2];
    } else {
        // Bitset container
        // TODO: Test This !!
        printf("Needs test test test test test\n");
        printf("Needs test test test test test\n");
        printf("Needs test test test test test\n");
        printf("Needs test test test test test\n");
        uint32_t base = 0;
        const uint64_t *buffer = (const uint64_t *)&c->buffer[2];
        for (int i=0; i<BCUTOFF; i++) {
            uint64_t w = buffer[i];
            if (w != 0) {
                int r = __builtin_ctzll(w);
                return r+base;
            }
            base += 64;
        }
 
    }
    return item;
}


bool cont_exists(const struct cont *c, uint16_t item) {
    uint32_t card = cont_cardinality(c);
    if (!card) return false;
    if (card <= CUTOFF) {
        return exists_array(c,item);
    } else {
        return exists_bitset(c,item);
    }
}
