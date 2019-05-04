#include "static-bkt-aggs.h"

static void static_bkt_agg_free(struct agg *f) {
    free(f);
}


static void consume_range_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
}

static json_t *range_agg_as_json(struct agg *a) {
    return NULL;
}

static void range_agg_merge(struct agg *into, const struct agg *from) {
}

struct agg *parse_range_agg(const char *name, json_t *j, struct index *in) {
    struct agg *a = calloc(1, sizeof(struct agg));
    a->kind = AGGK_BUCKET;
    a->type = AGG_RANGE;
    a->free = static_bkt_agg_free;
    snprintf(a->name, sizeof(a->name), "%s", name);
    json_t *f = json_object_get(j, JA_FIELD);
    json_t *r = json_object_get(j, JA_RANGES);
    bool valid = false;
    if (f) {
        const char *field = json_string_value(f);
        if (field) {
            struct schema *s = get_field_schema(in, field);
            if (s && s->is_indexed && (s->type == F_NUMBER || s->type == F_NUMLIST)) {
                a->field = s->i_priority;
                a->consume = consume_range_agg;
                a->as_json = range_agg_as_json;
                a->dup = agg_dup;
                a->merge = range_agg_merge;
                valid = true;
            }
        }
    }
    if (valid && r && json_is_array(r)) {
    } 
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse min aggr %s", name);
    return a;
}
}
