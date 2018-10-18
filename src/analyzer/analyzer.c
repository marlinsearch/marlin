#include <string.h>
#include "analyzer.h"

static struct analyzer *analyzers = NULL;
void init_default_analyzer(void);
void init_whitespace_analyzer(void);

void init_analyzers(void) {
    init_default_analyzer();
    init_whitespace_analyzer();
}

void register_analyzer(struct analyzer *a) {
    if (analyzers) {
        a->next = analyzers;
    }
    analyzers = a;
}

struct analyzer *get_analyzer(const char *name) {
    struct analyzer *a = analyzers;
    while (a) {
        if (strcmp(a->name, name) == 0) {
            return a;
        }
        a = a->next;
    }
    return NULL;
}

struct analyzer *get_default_analyzer(void) {
    return get_analyzer("default");
}

