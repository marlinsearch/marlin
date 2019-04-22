#include "metric-aggs.h"
#include "float.h"


/************ MAX METRICS AGGREGATION ************/
static void consume_max_agg(struct agg *a, struct index *in, uint32_t docid, void *data) {
    struct agg_max *am = (struct agg_max *)a;
    uint8_t *pos = data;
    double *dpos = (double *)(pos + sizeof(uint32_t));
    if (dpos[am->max_field] > am->value) {
        am->value = dpos[am->max_field];
    }
}

static json_t *max_agg_as_json(struct agg *a) {
    struct agg_max *am = (struct agg_max *)a;
    json_t *j = json_object();
    json_object_set_new(j, JA_VALUE, json_real(am->value));
    return j;
}

struct agg *parse_max_agg(const char *name, json_t *j, struct index *in) {
    struct agg_max *max = calloc(1, sizeof(struct agg_max));
    struct agg *a = (struct agg *)max;
    a->kind = AGGK_METRIC;
    a->type = AGG_MAX;
    snprintf(a->name, sizeof(a->name), "%s", name);
    json_t *f = json_object_get(j, JA_FIELD);
    if (f) {
        const char *field = json_string_value(f);
        if (field) {
            struct schema *s = get_field_schema(in, field);
            if (s && s->is_indexed && (s->type == F_NUMBER || s->type == F_NUMLIST)) {
                max->max_field = s->i_priority;
                max->value = -DBL_MAX;
                a->consume = consume_max_agg;
                a->as_json = max_agg_as_json;
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse max aggr %s", name);
    return a;
}
