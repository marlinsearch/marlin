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

static struct schema *schema_find_field(struct schema *s, const char *name) {
    M_DBG("FindField %s", name);
    if (!s) return NULL;
    struct schema *c = s;
    // First tries to find in all children
    struct schema *ret = NULL;
    while (c) {
        if (strcmp(c->fname, name) == 0) {
            return c;
        } 
        c = c->next;
    }
    if (!strstr(name, ".")) return NULL;
    // May be it is inside an object "aaa.bbb.ccc" ?
    char *namecopy = strdup(name);
    char *token = NULL;
    char *rest = namecopy;
    struct schema *ns = s;
    while((token = strtok_r(rest, ".", &rest))) {
        ns = schema_find_field(ns, token);
        M_DBG("Token is %s found %d %s", token, ns?1:0, rest);
        if (!rest || !strlen(rest)) {
            // We have reached the end and we have a result
            ret = ns;
            break;
        }
        if (!ns) break; // we did not find a field, breakout
        ns = ns->child;
        if (!ns) break; // we did not find a field, breakout
    }
    // If we found a result by tokenizing, set everything in
    // this path to indexed
    if (ret) {
        struct schema *is = s;
        free(namecopy);
        namecopy = strdup(name);
        rest = namecopy;
        while((token = strtok_r(rest, ".", &rest))) {
            is = schema_find_field(is, token);
            M_DBG("Token again is %s found %d", token, is?1:0);
            if (!rest || !strlen(rest)) {
                // We have reached the end and we have a result
                break;
            }
            if (!is) break; // we did not find a field, breakout
            is->is_indexed = true;
            is = is->child;
            if (!is) break; // we did not find a field, breakout
        }
    }

    free(namecopy);
    return ret;
}

// Returns 1 if any changes happened to the mapping due to this schema extraction
static int schema_extract(struct schema *s, json_t *j, struct mapping *m) {
    int updated = 0;
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
            updated = 1;
        }
        // Only if the child type is unknown, we care about parsing the field types
        if (child->type == F_NULL && value) {
            switch(json_typeof(value)) {
                case JSON_STRING:
                    child->type = F_STRING;
                    updated = 1;
                    break;
                case JSON_INTEGER:
                case JSON_REAL:
                    child->type = F_NUMBER;
                    updated = 1;
                    break;
                case JSON_TRUE:
                case JSON_FALSE:
                    child->type = F_BOOLEAN;
                    updated = 1;
                    break;
                case JSON_OBJECT:
                    child->type = F_OBJECT;
                    updated |= schema_extract(child, value, m);
                    break;
                case JSON_ARRAY:
                    if (json_array_size(value) > 0) {
                        switch(json_typeof(json_array_get(value, 0))) {
                            case JSON_STRING:
                                child->type = F_STRLIST;
                                updated = 1;
                                break;
                            case JSON_INTEGER:
                            case JSON_REAL:
                                child->type = F_NUMLIST;
                                updated = 1;
                                break;
                            case JSON_OBJECT:
                                child->type = F_OBJLIST;
                                updated |= schema_extract(child, json_array_get(value, 0), m);
                                break;
                            case JSON_ARRAY:
                                child->type = F_LIST;
                                updated |= schema_extract(child, json_array_get(value, 0), m);
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
            updated |= schema_extract(child, value, m);
        } else if (child->type == F_LIST) {
            updated |= schema_extract(child, json_array_get(value, 0), m);
        }
    }
    return updated;
}

static void mapping_save(const struct mapping *m) {
    json_t *jo = json_object();

    // Dump full schema
    if (m->full_schema) {
        json_t *full_schema = json_object();
        schema_to_json(m->full_schema, full_schema);
        json_object_set_new(jo, J_FULL_SCHEMA, full_schema);
    } else {
        json_object_set_new(jo, J_FULL_SCHEMA, NULL);
    }

    // Index schema
    if (m->index_schema) {
        json_t *index_schema = json_object();
        schema_to_json(m->index_schema, index_schema);
        json_object_set_new(jo, J_INDEX_SCHEMA, index_schema);
    } else {
        json_object_set_new(jo, J_INDEX_SCHEMA, NULL);
    }

    json_object_set_new(jo, J_INDEX_READY, json_boolean(m->ready_to_index));

    char *jstr = json_dumps(jo, JSON_PRESERVE_ORDER|JSON_INDENT(4));
}

/* This parses the incoming json object and parses the fields */
void mapping_extract(struct mapping *m, json_t *j) {
    if (!m->full_schema) {
        m->full_schema = schema_new();
    }
    // If there are any updates  to the schema, save it
    if (schema_extract(m->full_schema, j, m) > 0 ) {
        M_INFO("Saving mapping !!!!!!!!!!!!!!!!!!");
        // Save mapping as it has changed
        mapping_save(m);
    }
}

static struct schema *schema_dup(struct schema *in) {
    struct schema *out = calloc(1, sizeof(struct schema));
    memcpy(out, in, sizeof(struct schema));
    out->next = NULL;
    out->child = NULL;
    return out;
}

static void extract_index_schema(struct schema *in, struct schema *out) {
    struct schema *cin = in->child;
    while (cin) {
        if (cin->is_indexed || cin->is_facet) {
            struct schema *dup = schema_dup(cin);
            if (!out->child) {
                out->child = dup;
            } else {
                dup->next = out->child;
                out->child = dup;
            }
            if (cin->type == F_OBJECT || cin->type == F_OBJLIST) {
                extract_index_schema(cin, dup);
            }
        }
        cin = cin->next;
    }
}


/* Once full schema is available, find out all fields specified in the 
 * configuration for index_fields and facet_fields and update data.  
 * Till this function returns true, indexing of data does not start
 * just storage happens */
bool mapping_apply_config(struct mapping *m) {
    struct index_cfg *cfg = &m->index->cfg;
    // First make sure all fields exist and have a type learnt
    // else do not bother to extract fields
    for (int i=0; i<kv_size(cfg->index_fields); i++) {
        struct schema *fs = schema_find_field(m->full_schema->child, kv_A(cfg->index_fields, i)); 
        if (!fs || fs->type == F_NULL) return false;
    }

    for (int i=0; i<kv_size(cfg->facet_fields); i++) {
        struct schema *fs = schema_find_field(m->full_schema->child, kv_A(cfg->facet_fields, i)); 
        if (!fs || fs->type == F_NULL) return false;
        // Only strings, numbers and objects can be facets
        if (fs->type != F_STRING && fs->type != F_STRLIST && 
            fs->type != F_NUMBER && fs->type != F_NUMLIST && 
            fs->type != F_OBJECT && fs->type != F_OBJLIST ) return false;
    }

    for (int i=0; i<kv_size(cfg->index_fields); i++) {
        M_DBG("\n\nFinding field %s", kv_A(cfg->index_fields, i));
        struct schema *fs = schema_find_field(m->full_schema->child, kv_A(cfg->index_fields, i)); 
        fs->is_indexed = true;
        if (fs->type == F_STRING || fs->type == F_STRLIST) {
            fs->i_priority = m->num_strings;
            kv_push(char *, m->strings, strdup(kv_A(cfg->index_fields, i)));
            m->num_strings++;
        } else if (fs->type == F_NUMBER || fs->type == F_NUMLIST) {
            fs->i_priority = m->num_numbers;
            kv_push(char *, m->numbers, strdup(kv_A(cfg->index_fields, i)));
            m->num_numbers++;
        } else if (fs->type == F_BOOLEAN) {
            fs->i_priority = m->num_bools;
            kv_push(char *, m->bools, strdup(kv_A(cfg->index_fields, i)));
            m->num_bools++;
        } 
    }

    for (int i=0; i<kv_size(cfg->facet_fields); i++) {
        struct schema *fs = schema_find_field(m->full_schema->child, kv_A(cfg->facet_fields, i)); 
        fs->is_facet = true;
        fs->f_priority = m->num_facets;
        struct facet_info *fi = malloc(sizeof(struct facet_info));
        fi->name = strdup(kv_A(cfg->facet_fields, i));
        fi->type = fs->type;
        kv_push(struct facet_info *, m->facets, fi);
        m->num_facets++;
    }

    // Extract index schema
    m->index_schema = calloc(1, sizeof(struct schema));
    extract_index_schema(m->full_schema, m->index_schema);
    // By now all configured fields have been found and type determined
    m->ready_to_index = true;
    mapping_save(m);
    return true;
}

struct mapping* mapping_new(struct index *in) {
    struct mapping *m = calloc(1, sizeof(struct mapping));
    kv_init(m->strings);
    kv_init(m->numbers);
    kv_init(m->bools);
    kv_init(m->facets);
    m->index = in;
    return m;
}

