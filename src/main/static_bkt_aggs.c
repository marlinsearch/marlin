#include "static_bkt_aggs.h"
#include "aggs_common.h"
#include <float.h>

static void static_bkt_agg_free(struct agg *a) {
    for (int i = 0; i < kv_size(a->children); i++) {
        struct agg *c = kv_A(a->children, i);
        c->free(c);
    }
    kv_destroy(a->children);

    for (int i = 0; i < kv_size(a->bkts); i++) {
        struct bkt *b = kv_A(a->bkts, i);
        if (b->aggs) {
            b->aggs->free(b->aggs);
        }
        free(b);
    }
    kv_destroy(a->bkts);
    free(a);
}

static void consume_range_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    printf("consuming range agg \n");
    double val = get_agg_num_value(sq, docid, data, a->field);
    // Iterate all buckets and consume if its within range
    for (int i = 0; i < kv_size(a->bkts); i++) {
        struct range_bkt *rb = (struct range_bkt *)kv_A(a->bkts, i);
        if (val >= rb->from && val < rb->to) {
            rb->b.count++;
        }
    }
}

static json_t *range_agg_as_json(struct agg *a) {
    json_t *j = json_object();
    json_t *ja = json_array();

    int nbkts = kv_size(a->bkts);
    for (int i = 0; i < nbkts; i++) {
        struct range_bkt *rb = (struct range_bkt *)kv_A(a->bkts, i);
        json_t *jb = json_object();
        json_object_set_new(jb, JA_KEY, json_string(rb->b.key));
        json_object_set_new(jb, JA_COUNT, json_integer(rb->b.count));
        if (rb->from != -DBL_MAX) {
            json_object_set_new(jb, JA_FROM, json_real(rb->from));
        }
        if (rb->to != DBL_MAX) {
            json_object_set_new(jb, JA_TO, json_real(rb->to));
        }
        json_array_append_new(ja, jb);
    }

    json_object_set_new(j, JA_BKTS, ja);
    return j;
}

static void range_agg_merge(struct agg *into, const struct agg *from) {
    int nbkts = kv_size(into->bkts);

    for (int i = 0; i < nbkts; i++) {
        struct range_bkt *ri = (struct range_bkt *)kv_A(into->bkts, i);
        struct range_bkt *rf = (struct range_bkt *)kv_A(from->bkts, i);
        ri->b.count += rf->b.count;
        // TODO: merge nested aggs
    }
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

    // Set key if not present *-100.0 or 100.0-200.0 or 200-*
    if (bkt->b.key[0] == '\0') {
        char temp[128];
        if (bkt->from != -DBL_MAX) {
            snprintf(temp, sizeof(temp), "%f-", bkt->from);
        } else {
            strcpy(temp, "*-");
        }
        snprintf(bkt->b.key, sizeof(bkt->b.key), "%s", temp);
        if (bkt->to != DBL_MAX) {
            snprintf(temp, sizeof(temp), "%f", bkt->to);
        } else {
            strcpy(temp, "*");
        }
        strcat(bkt->b.key, temp);
    }

    kv_push(struct bkt *, a->bkts, (struct bkt *)bkt);
    return true;
}

static struct agg *range_agg_dup(const struct agg *a) {
    struct agg *r = malloc(sizeof(struct agg));
    memcpy(r, a, sizeof(struct agg));
    kv_init(r->children);
    for (int i = 0; i < kv_size(a->children); i++) {
        struct agg *c = kv_A(a->children, i);
        kv_push(struct agg *, r->children, c->dup(c));
    }
    kv_init(r->bkts);
    for (int i = 0; i < kv_size(a->bkts); i++) {
        struct range_bkt *rb = (struct range_bkt *)kv_A(a->bkts, i);
        struct range_bkt *nrb = malloc(sizeof(struct range_bkt));
        memcpy(nrb, rb, sizeof(struct range_bkt));
        kv_push(struct bkt *, r->bkts, (struct bkt *)nrb);
    }
    return r;
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
                a->dup = range_agg_dup;
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

