#include "filter.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

static khash_t(foper) *opermap;
const char *filter_str[] = {
    "F_AND",// and
    "F_OR", // OR or IN
    "F_NIN",// Not in
    "F_EQ", // Equals
    "F_NE", // Not equal
    "F_GT", // Greater than
    "F_GTE", // Greater than equals
    "F_LT", // Lesser than
    "F_LTE", // Lesser than equals
    "F_NUMCMP", // num comparison
    "F_ERROR" // Error occured while parsing
};

static struct filter *parse_schema_json(const struct schema *s, json_t *json);

static struct filter *filter_new(void) {
    struct filter *f = calloc(1, sizeof(struct filter));
    kv_init(f->children);
    return f;
}

static struct filter *filter_error(const char *error, const char *data) {
    struct filter *f = filter_new();
    f->type = F_ERROR;
    snprintf(f->error, sizeof(f->error), "%s %s", error, data);
    return f;
}

static void filter_children_free(struct filter *f) {
    for (int i = 0; i < kv_size(f->children); i++) {
        filter_free(kv_A(f->children, i));
    }
    kv_destroy(f->children);
    kv_init(f->children);
}

void filter_free(struct filter *f) {
    filter_children_free(f);
    kv_destroy(f->children);
    free(f);
}

FILTER_TYPE get_operator_type(const char *oname) {
    khiter_t k = kh_get(foper, opermap, oname);
    if (k != kh_end(opermap)) {
        return kh_value(opermap, k);
    }
    return F_ERROR;
}

static struct filter *validate_schema_json(const struct schema *s, json_t *json) {
    // Make sure values are ok
    if (json_is_string(json)) {
        // Example : "subreddit" : "videos"
        if ((s->type != F_STRING) && (s->type != F_STRLIST)) {
            return filter_error("String value for non-string field", s->fname);
        }
        if (!s->is_facet) {
            return filter_error("String filter for non-faceted string field", s->fname);
        }
    } else if (json_is_number(json)) {
        // Example : "numcomments" : 1000
        if ((s->type != F_NUMBER) && (s->type != F_NUMLIST)) {
            return filter_error("Number value for non-numeric field", s->fname);
        }
    } else if (json_is_boolean(json)) {
        // Example : "is_self" : true
        if (s->type != F_BOOLEAN) {
            return filter_error("Boolean value for non-boolean field", s->fname);
        }
    }
    return NULL;
}

static struct filter *parse_schema_array(const struct schema *s, json_t *json, FILTER_TYPE ftype) {
    json_t *value;
    struct filter *f = filter_new();
    f->type = ftype;
    f->field_type = s->type;
    size_t index;
    json_array_foreach(json, index, value) {
        // Get the result of parsing each filter
        struct filter *c = parse_schema_json(s, value);
        if (c == NULL) {
            f->type = F_ERROR;
            snprintf(f->error, sizeof(f->error), "Could not parse field %s", s->fname);
            break;
        } else if (c->type == F_ERROR) {
            f->type = F_ERROR;
            strcpy(f->error, c->error);
            filter_free(c);
            break;
        }
        kv_push(struct filter *, f->children, c);
    }
    return f;
}

static struct filter *parse_schema_key_json(const struct schema *s, const char *key, json_t *json) {
    // Validate and if we get a error filter, return it
    struct filter *v = validate_schema_json(s, json);
    if (v) return v;
    struct filter *f = filter_new();
    f->s = s;
    // By default 
    f->type = F_ERROR;
    snprintf(f->error, sizeof(f->error), "Failed to parse field %s", s->fname);

    if (json_is_string(json)) {
        // Example : "$eq" : "videos"
        f->strval = json_string_value(json);
        f->field_type = F_STRING;
    } else if (json_is_number(json)) {
        // Example : "$gt" : 1000
        f->numval = json_number_value(json);
        f->field_type = F_NUMBER;
    } else if (json_is_boolean(json)) {
        // Example : "$ne" : true
        f->numval = json_is_true(json);
        f->field_type = F_BOOLEAN;
    }

    f->type = get_operator_type(key);
    if (f->type == F_ERROR) {
        snprintf(f->error, sizeof(f->error), "Invalid operator %s specified for field %s", key, s->fname);
        return f;
    }
    if ((f->type == F_GT) || (f->type == F_GTE) || (f->type == F_LT) || (f->type == F_LTE)) {
        if (s->type != F_NUMBER) {
            // It has to be a number
            f->type = F_ERROR;
            snprintf(f->error, sizeof(f->error), "Invalid operator %s specified for field %s", key, s->fname);
            return f;
        }
    }

    // Handle in / nin
    if ((f->type == F_OR) || (f->type == F_NIN) || (f->type == F_AND)) {
        if (!json_is_array(json)) {
            f->type = F_ERROR;
            snprintf(f->error, sizeof(f->error), "Array expected for operator %s field %s", key, s->fname);
        } else {
            struct filter *fa = parse_schema_array(s, json, f->type);
            filter_free(f);
            f = fa;
        }
    } else {
        if (json_is_array(json)) {
            f->type = F_ERROR;
            snprintf(f->error, sizeof(f->error), "Array not expected for operator %s field %s", key, s->fname);
        }
        if (json_is_null(json)) {
            f->type = F_ERROR;
            snprintf(f->error, sizeof(f->error), "NULL not expected for operator %s field %s", key, s->fname);
        }
    }

    return f;
}

// Json object value called by below function
// Example 
// "subreddit" : { "$in": ['videos', 'pics']},
// "numcomments": {"$gt": 1000, "$lte": 1500},
// "numcomments": {"$ne": 1000},
static struct filter *parse_schema_object(const struct schema *s, json_t *json) {
    struct filter *f = NULL;
    const char *key;
    json_t *value;

    if (json_object_size(json) == 1) {
        json_object_foreach(json, key, value) {
            f = parse_schema_key_json(s, key, value);
        }
    } else {
        f = filter_new();
        f->type = F_AND;
        f->field_type = s->type;

        json_object_foreach(json, key, value) {
            // Get the result of parsing each filter
            struct filter *c = parse_schema_key_json(s, key, value);
            if (c == NULL) {
                f->type = F_ERROR;
                snprintf(f->error, sizeof(f->error), "Could not parse key %s", key);
                break;
            } else if (c->type == F_ERROR) {
                f->type = F_ERROR;
                strcpy(f->error, c->error);
                break;
            }
            if (c->type == F_GT || c->type == F_GTE) {
                f->numcmp1 = c->type;
                f->numval = c->numval;
            }
            if (c->type == F_LT || c->type == F_LTE) {
                f->numcmp2 = c->type;
                f->numval2 = c->numval;
            }

            kv_push(struct filter *, f->children, c);
        }
        if (f->type == F_AND && f->field_type == F_NUMBER && kv_size(f->children) == 2) {
            // Optimize this case
            if (kv_A(f->children, 0)->type == F_AND || kv_A(f->children, 1) == F_AND) {
                f->type = F_ERROR;
                snprintf(f->error, sizeof(f->error), "Invalid and operation for numeric filter %s", key);
            } else {
                f->type = F_NUMCMP;
                f->s = s;
                filter_children_free(f);
            }
        }
    }
    return f;

}


// Any json value
static struct filter *parse_schema_json(const struct schema *s, json_t *json) {
    // Validate and if we get a error filter, return it
    struct filter *v = validate_schema_json(s, json);
    if (v) return v;
    if (json_is_string(json)) {
        // Example : "subreddit" : "videos"
        struct filter *f = filter_new();
        f->type = F_EQ;
        f->strval = json_string_value(json);
        f->field_type = F_STRING;
        f->s = s;
        return f;
    } else if (json_is_number(json)) {
        // Example : "numcomments" : 1000
        struct filter *f = filter_new();
        f->type = F_EQ;
        f->numval = json_number_value(json);
        f->field_type = F_NUMBER;
        f->s = s;
        return f;
    } else if (json_is_boolean(json)) {
        // Example : "is_self" : true
        struct filter *f = filter_new();
        f->type = F_EQ;
        f->numval = json_is_true(json);
        f->field_type = F_BOOLEAN;
        f->s = s;
        return f;
    } else if (json_is_object(json)) {
        // Example 
        // "subreddit" : { "$in": ['videos', 'pics']},
        // "numcomments": {"$gt": 1000, "$lte": 1500},
        return parse_schema_object(s, json);
    } else if (json_is_array(json)) {
        return parse_schema_array(s, json, F_AND);
    }
    return filter_error("Json parse failure for field", s->fname);
}


static struct filter *parse_key_json(struct index *in, const char *key, json_t *json) {
    struct filter *f = NULL;
    const struct schema *s = get_field_schema(in, key);
    M_INFO("Key %s, s %s\n", key, s->fname);
    if (s) {
        return parse_schema_json(s, json);
    } 
    // TODO: Handle filter: {$and: [{$or : [{'a':1},{'b':2}]},{$or : [{'a':2},{'b':3}]}]}
    else if (strcmp(key, "$or")  == 0) {
    } else if (strcmp(key, "$and") == 0) {
    } else if (strcmp(key, "$nin") == 0) {
    }
    return (f)?f: filter_error("Invalid field or operator", key);
}


struct filter *parse_filter(struct index *in, json_t *j) {
    struct filter *f = NULL;
    const char *key;
    json_t *value;

    if (json_object_size(j) == 1) {
        json_object_foreach(j, key, value) {
            f = parse_key_json(in, key, value);
        }
    } else {
        f = filter_new();
        f->type = F_AND;

        json_object_foreach(j, key, value) {
            // Get the result of parsing each filter
            struct filter *c = parse_key_json(in, key, value);
            if (c == NULL) {
                f->type = F_ERROR;
                snprintf(f->error, sizeof(f->error), "Could not parse key %s", key);
            } else if (c->type == F_ERROR) {
                f->type = F_ERROR;
                strcpy(f->error, c->error);
                break;
            }
            kv_push(struct filter *, f->children, c);
        }
    }

    dump_filter(f, 0);

    return f;
}


static void hash_operator(const char *key, FILTER_TYPE value) {
    khiter_t k;
    int ret;
    k = kh_put(foper, opermap, strdup(key), &ret);
    kh_value(opermap, k) = value;
}

// Initialize filters, setup operator to type mapping
void init_filters(void) {
    opermap = kh_init(foper);
    hash_operator("$eq", F_EQ);
    hash_operator("$ne", F_NE);
    hash_operator("$and", F_AND);
    hash_operator("$or", F_OR);
    hash_operator("$in", F_OR);
    hash_operator("$nin", F_NIN);
    hash_operator("$gt", F_GT);
    hash_operator("$gte", F_GTE);
    hash_operator("$lt", F_LT);
    hash_operator("$lte", F_LTE);

}

void dump_filter(struct filter *f, int indent) {
    char ind[256];
    ind[0] = '\0';
    for (int i=0; i<indent; i++) {
        strcat(ind, "    ");
    }
    M_INFO("%sType      : %s", ind, filter_str[f->type]);
    if (f->type == F_ERROR) {
        M_INFO("%sError     : %s", ind, f->error);
    }
    M_INFO("%sFieldType : %s", ind, type_to_str(f->field_type));
    M_INFO("%sField     : %s", ind, f->s?f->s->fname:"");
    M_INFO("%sStrVal     : %s", ind, (f->strval)?f->strval:"");
    M_INFO("%sNumVal     : %f", ind, f->numval);
    M_INFO("%sBmap       : %d", ind, f->fr_bmap?1:0);
    M_INFO("\n");

    for (int i = 0; i < kv_size(f->children); i++) {
        dump_filter(kv_A(f->children, i), indent+1);
    }
}
