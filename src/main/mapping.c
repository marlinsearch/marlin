#include <string.h>
#include "mapping.h"


// *** NOTE : Needs to be in sync with F_TYPE in mapping.h !
const char *type2str[] = {
    "null",
    "string",
    "string array",
    "number",
    "number array",
    "boolean",
    "object",
    "object array",
    "geo",
    "geo array",
    "array"
};

static struct schema *find_field_under_schema(struct schema *sa, const char *name){
    struct schema *s = sa->child;
    while (s) {
        if (strcmp(s->fname, name) == 0) {
            return s;
        }
        s = s->next;
    }
    return NULL;
}

static struct schema *schema_new(void) {
    struct schema *s = calloc(1, sizeof(struct schema));
    return s;
}

static void schema_to_json(const struct schema *s, json_t *j) {
    while (s) {
        json_t *jn = json_object();
        json_object_set_new(jn, J_TYPE, json_string(type2str[s->type]));
        json_object_set_new(jn, J_FIELDID, json_integer(s->field_id));
        if (s->type == F_OBJECT || s->type == F_OBJLIST) {
            json_t *jp = json_object();
            schema_to_json(s->child, jp);
            json_object_set_new(jn, J_PROPERTIES, jp);
        }
        // Add other stuff to jn
        json_object_set_new(j, s->fname, jn);
        if (s->is_indexed) {
            json_object_set_new(jn, J_IS_INDEXED, json_boolean(1));
        }
        if (s->is_facet) {
            json_object_set_new(jn, J_IS_FACET, json_boolean(1));
        }
        s = s->next;
    }
}

static void schema_extract(struct schema *s, json_t *j, struct mapping *m) {
    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        M_DBG("extract key %s", key);
        struct schema *child = find_field_under_schema(s, key);
        if (!child) {
            child = schema_new();
            child->next = s->child;
            s->child = child;
            snprintf(child->fname, sizeof(child->fname), "%s", key);
            child->field_id = ++m->num_fields;
        }
        // Only if the child type is unknown, we care about parsing the field types
        if (child->type == F_NULL && value) {
            switch(json_typeof(value)) {
                case JSON_STRING:
                    child->type = F_STRING;
                    break;
                case JSON_INTEGER:
                case JSON_REAL:
                    child->type = F_NUMBER;
                    break;
                case JSON_TRUE:
                case JSON_FALSE:
                    child->type = F_BOOLEAN;
                    break;
                case JSON_OBJECT:
                    child->type = F_OBJECT;
                    schema_extract(child, value, m);
                    break;
                case JSON_ARRAY:
                    if (json_array_size(value) > 0) {
                        switch(json_typeof(json_array_get(value, 0))) {
                            case JSON_STRING:
                                child->type = F_STRLIST;
                                break;
                            case JSON_INTEGER:
                            case JSON_REAL:
                                child->type = F_NUMLIST;
                                break;
                            case JSON_OBJECT:
                                child->type = F_OBJLIST;
                                schema_extract(child, json_array_get(value, 0), m);
                                break;
                            case JSON_ARRAY:
                                child->type = F_LIST;
                                schema_extract(child, json_array_get(value, 0), m);
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                default:
                    break;
            }
        } else if (child->type == F_OBJECT ) {
            // If it is an object, we have to look for fields in the innerobject recursively
            schema_extract(child, value, m);
        } else if (child->type == F_LIST) {
            schema_extract(child, json_array_get(value, 0), m);
        }
    }
}

/* This parses the incoming json object and parses the fields */
void mapping_extract(struct mapping *m, json_t *j) {
    if (!m->full_schema) {
        m->full_schema = schema_new();
    }
    schema_extract(m->full_schema, j, m);
}

struct mapping* mapping_new(void) {
    struct mapping *m = calloc(1, sizeof(struct mapping));
    return m;
}


