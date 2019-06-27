#include "aggs.h"
#include "metric_aggs.h"
#include "static_bkt_aggs.h"

struct agg_info {
    const char *agg_name;
    struct agg *(*parse_cb)(const char *name, json_t *j, struct index *in);
};


static void consume_root_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    for (int i = 0; i < kv_size(a->children); i++) {
        struct agg *c = kv_A(a->children, i);
        c->consume(c, sq, docid, data);
    }
}

static json_t *root_agg_as_json(struct agg *a) {
    json_t *j = json_object();

    for (int i = 0; i < kv_size(a->children); i++) {
        struct agg *c = kv_A(a->children, i);
        json_object_set_new(j, c->name, c->as_json(c));
    }

    return j;
}

static struct agg *root_agg_dup(const struct agg *a) {
    struct agg *r = malloc(sizeof(struct agg));
    memcpy(r, a, sizeof(struct agg));
    kv_init(r->children);
    for (int i = 0; i < kv_size(a->children); i++) {
        struct agg *c = kv_A(a->children, i);
        kv_push(struct agg *, r->children, c->dup(c));
    }
    return r;
}

static void root_agg_merge(struct agg *into, const struct agg *from) {
    for (int i = 0; i < kv_size(from->children); i++) {
        struct agg *ic = kv_A(into->children, i);
        const struct agg *fc = kv_A(from->children, i);
        ic->merge(ic, fc);
    }
}

static void root_agg_free(struct agg *a) {
    for (int i = 0; i < kv_size(a->children); i++) {
        struct agg *c = kv_A(a->children, i);
        c->free(c);
    }
    kv_destroy(a->children);
    free(a);
}

static struct agg *parse_root_agg(const char *name, json_t *j, struct index *in) {
    struct agg *root = calloc(1, sizeof(struct agg));
    root->kind = AGGK_ROOT;
    root->type = AGG_ROOT;
    root->consume = consume_root_agg;
    root->as_json = root_agg_as_json;
    root->dup = root_agg_dup;
    root->merge = root_agg_merge;
    root->free = root_agg_free;
    kv_init(root->children);

    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        struct agg *c = detect_and_parse(key, value, in);
        // If we cannot detect the type, its an error
        if (!c) {
            root->type = AGG_ERROR;
            snprintf(root->name, sizeof(root->name), "Could not parse agg %s", key);
            break;
        } else if (c->type == AGG_ERROR) {
            // If we detected a type but encountered an error while parsing
            // aggregation, copy over the error
            root->type = AGG_ERROR;
            snprintf(root->name, sizeof(root->name), "%s", c->name);
            c->free(c);
            break;
        } else {
            // We have a properly parsed aggregation, add it as a child
            kv_push(agg_t *, root->children, c);
        }
    }

    return root;
}


/* Each aggregation needs to be parsed differently based on its type and kind */
const struct agg_info aggs[] = {
    // Root aggregation
    {"aggs", parse_root_agg},
    // Max - metric aggregation
    {"max", parse_max_agg},
    // Min - metric aggregation
    {"min", parse_min_agg},
    // Avg - metric aggregation
    {"avg", parse_avg_agg},
    // Stags - metric aggregation
    {"stats", parse_stats_agg},
    // Cardinality - metric aggregation
    {"cardinality", parse_card_agg},
    // Range - static bucket aggregation
    {"range", parse_range_agg},
    {NULL, NULL}
};


struct agg *detect_and_parse(const char *name, json_t *j, struct index *in) {
    struct agg *nagg = NULL;
    struct agg *agg = NULL;

    // Iterate, detect and parse aggregation
    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        int c = 0;
        while (aggs[c].agg_name) {
            // If we see a matching aggregation, use that
            if (strcmp(key, aggs[c].agg_name) == 0) {
                struct agg *a = aggs[c].parse_cb(name, value, in);
                if (strcmp(key, "aggs") == 0) {
                    nagg = a;
                } else {
                    agg = a;
                }
            }
            c++;
        }
    }

    // Handle nested aggregation of bucket aggregations
    if (agg && agg->kind == AGGK_BUCKET) {
        agg->naggs = nagg;
        // For static bucket aggregations, we can set the nested
        // aggregations right away
        for (int i = 0; i < kv_size(agg->bkts); i++) {
            struct bkt *b = kv_A(agg->bkts, i);
            if (nagg) {
                b->aggs = aggs_dup(nagg);
            }
        }
    }

    return agg;
}

struct agg *aggs_dup(const struct agg *a) {
    struct agg *n = a->dup(a);
    return n;
}

/* Used to parse aggregations and all nested aggregations under it.  If parsing
 * fails for any of the aggregation, it is propogated to the top root aggregation.
 * The aggregation type is set to AGG_ERROR and the name field contains the error
 * message*/
struct agg *parse_aggs(json_t *j, struct index *in) {
    return parse_root_agg("aggs", j, in);
}

