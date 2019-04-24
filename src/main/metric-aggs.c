#include "metric-aggs.h"
#include "float.h"

/************ AVG METRICS AGGREGATION ************/
static void consume_avg_agg(struct agg *a, struct index *in, uint32_t docid, void *data) {
    struct agg_avg *am = (struct agg_avg *)a;
    uint8_t *pos = data;
    double *dpos = (double *)(pos + sizeof(uint32_t));
    // TODO: Handle NULL values
    am->value += (dpos[am->field]);
    am->count++;
}

static json_t *avg_agg_as_json(struct agg *a) {
    struct agg_avg *am = (struct agg_avg *)a;
    json_t *j = json_object();
    json_object_set_new(j, JA_VALUE, json_real((am->value / am->count)));
    return j;
}

static struct agg *avg_agg_dup(const struct agg *a) {
    struct agg_avg *avg = malloc(sizeof(struct agg_avg));
    memcpy(avg, a, sizeof(struct agg_avg));
    return (struct agg *)avg;
}

static void avg_agg_merge(struct agg *into, const struct agg *from) {
    struct agg_avg *f = (struct agg_avg *)from;
    struct agg_avg *i = (struct agg_avg *)into;
    i->value += f->value;
    i->count += f->count;
}

static void avg_agg_free(struct agg *f) {
    free(f);
}

struct agg *parse_avg_agg(const char *name, json_t *j, struct index *in) {
    struct agg_avg *avg = calloc(1, sizeof(struct agg_avg));
    struct agg *a = (struct agg *)avg;
    a->kind = AGGK_METRIC;
    a->type = AGG_AVG;
    snprintf(a->name, sizeof(a->name), "%s", name);
    json_t *f = json_object_get(j, JA_FIELD);
    if (f) {
        const char *field = json_string_value(f);
        if (field) {
            struct schema *s = get_field_schema(in, field);
            if (s && s->is_indexed && (s->type == F_NUMBER || s->type == F_NUMLIST)) {
                avg->field = s->i_priority;
                a->consume = consume_avg_agg;
                a->as_json = avg_agg_as_json;
                a->dup = avg_agg_dup;
                a->merge = avg_agg_merge;
                a->free = avg_agg_free;
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse avg aggr %s", name);
    return a;
}


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

static struct agg *max_agg_dup(const struct agg *a) {
    struct agg_max *max = malloc(sizeof(struct agg_max));
    memcpy(max, a, sizeof(struct agg_max));
    return (struct agg *)max;
}

static void max_agg_merge(struct agg *into, const struct agg *from) {
    struct agg_max *f = (struct agg_max *)from;
    struct agg_max *i = (struct agg_max *)into;
    if (f->value > i->value) {
        i->value = f->value;
    }
}

static void max_agg_free(struct agg *f) {
    free(f);
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
                a->dup = max_agg_dup;
                a->merge = max_agg_merge;
                a->free = max_agg_free;
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse max aggr %s", name);
    return a;
}

// TODO: DRY.. combine max & min aggr
/************ MIN METRICS AGGREGATION ************/
static void consume_min_agg(struct agg *a, struct index *in, uint32_t docid, void *data) {
    struct agg_min *am = (struct agg_min *)a;
    uint8_t *pos = data;
    double *dpos = (double *)(pos + sizeof(uint32_t));
    if (dpos[am->min_field] < am->value) {
        am->value = dpos[am->min_field];
    }
}

static json_t *min_agg_as_json(struct agg *a) {
    struct agg_min *am = (struct agg_min *)a;
    json_t *j = json_object();
    json_object_set_new(j, JA_VALUE, json_real(am->value));
    return j;
}

static struct agg *min_agg_dup(const struct agg *a) {
    struct agg_min *min = malloc(sizeof(struct agg_min));
    memcpy(min, a, sizeof(struct agg_min));
    return (struct agg *)min;
}

static void min_agg_merge(struct agg *into, const struct agg *from) {
    struct agg_min *f = (struct agg_min *)from;
    struct agg_min *i = (struct agg_min *)into;
    if (f->value < i->value) {
        i->value = f->value;
    }
}

static void min_agg_free(struct agg *f) {
    free(f);
}

struct agg *parse_min_agg(const char *name, json_t *j, struct index *in) {
    struct agg_min *min = calloc(1, sizeof(struct agg_min));
    struct agg *a = (struct agg *)min;
    a->kind = AGGK_METRIC;
    a->type = AGG_MIN;
    snprintf(a->name, sizeof(a->name), "%s", name);
    json_t *f = json_object_get(j, JA_FIELD);
    if (f) {
        const char *field = json_string_value(f);
        if (field) {
            struct schema *s = get_field_schema(in, field);
            if (s && s->is_indexed && (s->type == F_NUMBER || s->type == F_NUMLIST)) {
                min->min_field = s->i_priority;
                min->value = DBL_MAX;
                a->consume = consume_min_agg;
                a->as_json = min_agg_as_json;
                a->dup = min_agg_dup;
                a->merge = min_agg_merge;
                a->free = min_agg_free;
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse min aggr %s", name);
    return a;
}
