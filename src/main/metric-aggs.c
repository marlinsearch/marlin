#include "metric-aggs.h"
#include "float.h"


/****************** Common functions **********************/
static void metric_agg_free(struct agg *f) {
    free(f);
}

/************ CARDINALITY METRICS AGGREGATION ************/
/* TODO: Currently uses a bitmap, Use hyperlog++ after tracking memory usage if required */
static void card_agg_free(struct agg *a) {
    struct agg_card *ac = (struct agg_card *)a;
    if (ac->bmap) {
        bmap_free(ac->bmap);
    }
    if (ac->oper) {
        oper_free(ac->oper);
    }
    free(ac);
}

static void consume_card_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    struct agg_card *ac = (struct agg_card *)a;
    uint8_t *pos = data;
    pos = pos + sizeof(uint32_t); // Skip offset
    struct mapping *m = sq->shard->index->mapping;
    // calculate facet offset
    size_t fo = m->num_numbers * sizeof(double);
    // Get the facet position
    uint8_t *fpos = pos + fo;
    for (int i = 0; i < m->num_facets; i++) {
        facets_t *f = (facets_t *)fpos;
        // If facet is enabled, count it
        if (i == ac->field) {
            for (int j = 0; j < f->count; j++) {
                bmap_add(ac->bmap, f->data[j]);
            }
            return;
        }
        // Move to next facet
        fpos += ((f->count * sizeof(uint32_t)) + sizeof(uint32_t));
    }

}

static json_t *card_agg_as_json(struct agg *a) {
    struct agg_card *ac = (struct agg_card *)a;
    json_t *j = json_object();
    if (ac->oper) {
        bmap_free(ac->bmap);
        ac->bmap = oper_or(ac->oper);
    }
    json_object_set_new(j, JA_VALUE, json_integer(bmap_cardinality(ac->bmap)));
    return j;
}

static struct agg *card_agg_dup(const struct agg *a) {
    struct agg_card *card = malloc(sizeof(struct agg_card));
    memcpy(card, a, sizeof(struct agg_card));
    // NOTE: Do not copy bmap, just create a new bitmap.
    card->bmap = bmap_new();
    return (struct agg *)card;
}

static void card_agg_merge(struct agg *into, const struct agg *from) {
    struct agg_card *f = (struct agg_card *)from;
    struct agg_card *i = (struct agg_card *)into;
    if (!i->oper) {
        i->oper = oper_new();
    }
    oper_add(i->oper, f->bmap);
}

struct agg *parse_card_agg(const char *name, json_t *j, struct index *in) {
    struct agg_card *card = calloc(1, sizeof(struct agg_card));
    struct agg *a = (struct agg *)card;
    a->kind = AGGK_METRIC;
    a->type = AGG_CARDINALITY;
    a->free = card_agg_free;

    snprintf(a->name, sizeof(a->name), "%s", name);
    json_t *f = json_object_get(j, JA_FIELD);
    if (f) {
        const char *field = json_string_value(f);
        if (field) {
            struct schema *s = get_field_schema(in, field);
            if (s && s->is_facet) {
                card->field = s->f_priority;
                a->consume = consume_card_agg;
                a->as_json = card_agg_as_json;
                a->dup = card_agg_dup;
                a->merge = card_agg_merge;
                card->bmap = bmap_new();
                return a;
            }
        }
    }

    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse card aggr %s, requires a facet value.", name);
    return a;
}

static double get_num_value(struct squery *sq, uint32_t docid, void *data, int priority) {
    double val = -DBL_MAX;
    if (data) {
        uint8_t *pos = data;
        double *dpos = (double *)(pos + sizeof(uint32_t));
        // TODO: Handle NULL values
        val = dpos[priority];
    } else {
        int docpos = docid % 1000;
        uint64_t docgrp_id = IDNUM((docid - docpos), priority);
        khash_t(IDNUM2DBL) *kh = sq->kh_idnum2dbl;
        khiter_t k = kh_get(IDNUM2DBL, kh, docgrp_id);
        double *grpd = NULL;

        if (k == kh_end(kh)) {
            // We need to add the entry to be written
            int ret = 0;
            k = kh_put(IDNUM2DBL, kh, docgrp_id, &ret);

            // Check if it already exists in lmdb
            MDB_val key, mdata;
            key.mv_size = sizeof(uint64_t);
            key.mv_data = (void *)&docgrp_id;
            // If it already exists, we need not write so set a NULL value to 
            // avoid looking up mdb everytime we encounter this facetid
            if (mdb_get(sq->txn, sq->shard->sindex->idnum2dbl_dbi, &key, &mdata) == 0) {
                grpd = mdata.mv_data;
            }
            kh_value(kh, k) = grpd;
        } else {
            grpd = kh_value(kh, k);
        }
        if (grpd) {
            return grpd[docpos];
        }
    }
    return val;
}


/************ STATS METRICS AGGREGATION ************/
static void consume_stats_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    struct agg_stats *am = (struct agg_stats *)a;
    double val = get_num_value(sq, docid, data, am->field);
    if (val != -DBL_MAX) {
        am->sum += val;
        if (val < am->min) {
            am->min = val;
        }
        if (val > am->max) {
            am->max = val;
        }
        am->count++;
    }
}

static json_t *stats_agg_as_json(struct agg *a) {
    struct agg_stats *am = (struct agg_stats *)a;
    json_t *j = json_object();
    json_object_set_new(j, JA_AVG, json_real((am->sum / am->count)));
    json_object_set_new(j, JA_SUM, json_real(am->sum));
    json_object_set_new(j, JA_MIN, json_real(am->min));
    json_object_set_new(j, JA_MAX, json_real(am->max));
    json_object_set_new(j, JA_COUNT, json_integer(am->count));
    return j;
}

static struct agg *stats_agg_dup(const struct agg *a) {
    struct agg_stats *stats = malloc(sizeof(struct agg_stats));
    memcpy(stats, a, sizeof(struct agg_stats));
    return (struct agg *)stats;
}

static void stats_agg_merge(struct agg *into, const struct agg *from) {
    struct agg_stats *f = (struct agg_stats *)from;
    struct agg_stats *i = (struct agg_stats *)into;
    i->sum += f->sum;
    i->count += f->count;
    if (f->min < i->min) {
        i->min = f->min;
    }
    if (f->max > i->max) {
        i->max = f->max;
    }
}

struct agg *parse_stats_agg(const char *name, json_t *j, struct index *in) {
    struct agg_stats *stats = calloc(1, sizeof(struct agg_stats));
    struct agg *a = (struct agg *)stats;
    a->kind = AGGK_METRIC;
    a->type = AGG_STATS;
    a->free = metric_agg_free;
    snprintf(a->name, sizeof(a->name), "%s", name);
    json_t *f = json_object_get(j, JA_FIELD);
    if (f) {
        const char *field = json_string_value(f);
        if (field) {
            struct schema *s = get_field_schema(in, field);
            if (s && s->is_indexed && (s->type == F_NUMBER || s->type == F_NUMLIST)) {
                stats->field = s->i_priority;
                stats->min = DBL_MAX;
                stats->max = -DBL_MAX;
                a->consume = consume_stats_agg;
                a->as_json = stats_agg_as_json;
                a->dup = stats_agg_dup;
                a->merge = stats_agg_merge;
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse stats aggr %s", name);
    return a;
}



/************ AVG METRICS AGGREGATION ************/
static void consume_avg_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    struct agg_avg *am = (struct agg_avg *)a;
    double val = get_num_value(sq, docid, data, am->field);
    if (val != -DBL_MAX) {
        am->sum += val;
        am->count++;
    }
}

static json_t *avg_agg_as_json(struct agg *a) {
    struct agg_avg *am = (struct agg_avg *)a;
    json_t *j = json_object();
    json_object_set_new(j, JA_VALUE, json_real((am->sum / am->count)));
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
    i->sum += f->sum;
    i->count += f->count;
}

struct agg *parse_avg_agg(const char *name, json_t *j, struct index *in) {
    struct agg_avg *avg = calloc(1, sizeof(struct agg_avg));
    struct agg *a = (struct agg *)avg;
    a->kind = AGGK_METRIC;
    a->type = AGG_AVG;
    a->free = metric_agg_free;
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
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse avg aggr %s", name);
    return a;
}


/************ MAX METRICS AGGREGATION ************/
static void consume_max_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    struct agg_max *am = (struct agg_max *)a;
    double val = get_num_value(sq, docid, data, am->max_field);
    if (val != -DBL_MAX) {
        if (val > am->value) {
            am->value = val;
        }
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

struct agg *parse_max_agg(const char *name, json_t *j, struct index *in) {
    struct agg_max *max = calloc(1, sizeof(struct agg_max));
    struct agg *a = (struct agg *)max;
    a->kind = AGGK_METRIC;
    a->type = AGG_MAX;
    a->free = metric_agg_free;
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
static void consume_min_agg(struct agg *a, struct squery *sq, uint32_t docid, void *data) {
    struct agg_min *am = (struct agg_min *)a;
    double val = get_num_value(sq, docid, data, am->min_field);
    if (val != -DBL_MAX) {
        if (val < am->value) {
            am->value = val;
        }
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

struct agg *parse_min_agg(const char *name, json_t *j, struct index *in) {
    struct agg_min *min = calloc(1, sizeof(struct agg_min));
    struct agg *a = (struct agg *)min;
    a->kind = AGGK_METRIC;
    a->type = AGG_MIN;
    a->free = metric_agg_free;
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
                return a;
            }
        }
    }
    a->type = AGG_ERROR;
    snprintf(a->name, sizeof(a->name), "Failed to parse min aggr %s", name);
    return a;
}
