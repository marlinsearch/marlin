#include "static_bkt_aggs.h"
#include <float.h>

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

static bool parse_range_bkt(struct json_t *jb, struct agg *a) {
    const char *key;
    json_t *value;
    struct range_bkt *bkt = calloc(1, sizeof(struct range_bkt));
    bkt->from = -DBL_MAX;
    bkt->to = DBL_MAX;
    json_object_foreach(jb, key, value) {
        if (strcmp(key, JA_FROM) == 0) {
            bkt->from = json_number_value(value);
        } else if (strcmp(key, JA_TO) == 0) {
            bkt->to = json_number_value(value);
        } else if (strcmp(key, JA_KEY) == 0) {
            snprintf(bkt->b.key, sizeof(bkt->b.key), "%s", key);
        } else {
            free(bkt);
            // TODO: Handle errors properly.. we may end up not freeing nested aggs
            // or children or bkts;
            a->type = AGG_ERROR;
            snprintf(a->name, sizeof(a->name), "Invalid field %s in ranges", key);
            return false;
        }
    }

    if (bkt->from == -DBL_MAX && bkt->to == DBL_MAX) {
        free(bkt);
        a->type = AGG_ERROR;
        snprintf(a->name, sizeof(a->name), "From or to values missing in a range bucket");
        return false;
    }

    kv_push(struct bkt *, a->bkts, (struct bkt *)bkt);
    return true;
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
                a->dup = aggs_dup;
                a->merge = range_agg_merge;
                valid = true;
            }
        }
    }
    kv_init(a->bkts);
    if (valid && r && json_is_array(r)) {
        size_t index;
        json_t *jb;
        json_array_foreach(r, index, jb) {
            // If parsing failed, we expect the type to be AGG_ERROR and
            // a->name to have the error message
            if (!parse_range_bkt(jb, a)) {
                return a;
            }
        }
        // We are done parsing all ranges
        return a;
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse range aggr %s", name);
    return a;
}

