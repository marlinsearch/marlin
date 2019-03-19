#include <string.h>
#include "mapping.h"
#include "marlin.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="

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

static F_TYPE typestr_to_type(const char *ts) {
    for (int i=0; i<F_MAX; i++) {
        if (strcmp(type2str[i], ts) == 0) {
            return i;
        }
    }
    return 0;
}

const char * type_to_str(F_TYPE type) {
    if (type < F_MAX) return type2str[type];
    return "";
}

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
    s->i_priority = -1;
    s->f_priority = -1;
    return s;
}

static void schema_free(struct schema *s) {
    if (s->child) {
        schema_free(s->child);
    }
    if (s->next) {
        schema_free(s->next);
    }
    free(s);
}

static void schema_reset(struct schema *s) {
    if (s->child) {
        schema_reset(s->child);
    }
    if (s->next) {
        schema_reset(s->next);
    }
    s->is_facet = false;
    s->is_indexed = false;
    s->i_priority = -1;
    s->f_priority = -1;
}

static void schema_to_json(const struct schema *s, json_t *j) {
    while (s) {
        json_t *jn = json_object();
        json_object_set_new(jn, J_TYPE, json_string(type2str[s->type]));
        json_object_set_new(jn, J_FIELDID, json_integer(s->field_id));
        if (s->i_priority >= 0) {
            json_object_set_new(jn, J_INDEX_PRIORITY, json_integer(s->i_priority));
        }
        if (s->f_priority >= 0) {
            json_object_set_new(jn, J_FACET_PRIORITY, json_integer(s->f_priority));
        }
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

static void json_to_schema(json_t *j, struct schema *rs) {
    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        struct schema *c = schema_new();
        // name
        snprintf(c->fname, sizeof(c->fname), "%s", key);
        // type
        c->type = typestr_to_type(json_string_value(json_object_get(value, J_TYPE)));
        // is_indexed
        json_t *temp = json_object_get(value, J_IS_INDEXED);
        if (temp && json_is_boolean(temp)) {
            c->is_indexed = true;
        }
        // is_facet
        temp = json_object_get(value, J_IS_FACET);
        if (temp && json_is_boolean(temp)) {
            c->is_facet = true;
        }

        // field id
        temp = json_object_get(value, J_FIELDID);
        if (temp && json_is_number(temp)) {
            c->field_id = json_number_value(temp);
        } else {
            c->field_id = -1;
        }
        // index_priority
        temp = json_object_get(value, J_INDEX_PRIORITY);
        if (temp && json_is_number(temp)) {
            c->i_priority = json_number_value(temp);
        } else {
            c->i_priority = -1;
        }
        // facet priority
        temp = json_object_get(value, J_FACET_PRIORITY);
        if (temp && json_is_number(temp)) {
            c->f_priority = json_number_value(temp);
        } else {
            c->f_priority = -1;
        }
        // Add it to result schema
        if (!rs->child) {
            rs->child = c;
        } else {
            c->next = rs->child;
            rs->child = c;
        }
        if (c->type == F_OBJECT || c->type == F_OBJLIST) {
            temp = json_object_get(value, J_PROPERTIES);
            if (temp && json_is_object(temp)) {
                json_to_schema(temp, c);
            }
        }
    }
}


struct schema *schema_find_field(struct schema *s, const char *name) {
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

char *mapping_to_json_str(const struct mapping *m) {
    json_t *jo = json_object();

    // Dump full schema
    if (m->full_schema) {
        json_t *full_schema = json_object();
        schema_to_json(m->full_schema->child, full_schema);
        json_object_set_new(jo, J_FULL_SCHEMA, full_schema);
    } else {
        json_object_set_new(jo, J_FULL_SCHEMA, json_null());
    }

    // Index schema
    if (m->index_schema) {
        json_t *index_schema = json_object();
        schema_to_json(m->index_schema->child, index_schema);
        json_object_set_new(jo, J_INDEX_SCHEMA, index_schema);
    } else {
        json_object_set_new(jo, J_INDEX_SCHEMA, json_null());
    }

    json_object_set_new(jo, J_NUM_FIELDS, json_integer(m->num_fields));
    json_object_set_new(jo, J_INDEX_READY, json_boolean(m->ready_to_index));
    // Store field names sorted by priority
    json_t *jas = json_array();
    for (int i = 0; i < kv_size(m->strings); i++) {
        json_array_append_new(jas, json_string(kv_A(m->strings, i)));
    }
    json_object_set_new(jo, J_STRINGS, jas);

    json_t *jan = json_array();
    for (int i = 0; i < kv_size(m->numbers); i++) {
        json_array_append_new(jan, json_string(kv_A(m->numbers, i)));
    }
    json_object_set_new(jo, J_NUMBERS, jan);

    json_t *jab = json_array();
    for (int i = 0; i < kv_size(m->bools); i++) {
        json_array_append_new(jab, json_string(kv_A(m->bools, i)));
    }
    json_object_set_new(jo, J_BOOLS, jab);

    json_t *jaf = json_array();
    for (int i = 0; i < kv_size(m->facets); i++) {
        struct facet_info *fi = kv_A(m->facets, i);
        json_t *jf = json_object();
        json_object_set_new(jf, "name", json_string(fi->name));
        json_object_set_new(jf, "type", json_integer(fi->type));
        json_array_append_new(jaf, jf);
    }
    json_object_set_new(jo, J_FACETS, jaf);
    char *jstr = json_dumps(jo, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(jo);
    return jstr;
}

static void mapping_save(const struct mapping *m) {
    char *jstr = mapping_to_json_str(m);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, m->index->app->name,
                                 m->index->name, MAPPING_FILE);
    FILE *fp = fopen(path, "w");
    if (fp) {
        fprintf(fp, "%s", jstr);
        fclose(fp);
    } else {
        M_ERR("Failed to save mapping file %s", path);
    }
    free(jstr);
}

static void mapping_load(struct mapping *m) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, 
             m->index->app->name, m->index->name, MAPPING_FILE);
    json_error_t error;
    json_t *j = json_load_file(path, 0, &error);
    if (!j) {
        M_DBG("Failed to load mapping, not created yet ?");
        return;
    } 
    // Load full schema
    json_t *jfs = json_object_get(j, J_FULL_SCHEMA);
    if (jfs && json_is_object(jfs) && json_object_size(jfs)) {
        struct schema *fs = schema_new();
        m->full_schema = fs;
        json_to_schema(jfs, fs);
    }
    // Load indexed schema
    json_t *jis = json_object_get(j, J_INDEX_SCHEMA);
    if (jis && json_is_object(jis) && json_object_size(jis)) {
        struct schema *is = schema_new();
        m->index_schema = is;
        json_to_schema(jis, is);
    }

    json_t *temp = json_object_get(j, J_INDEX_READY);
    if (temp && m->full_schema && json_is_boolean(temp)) {
        m->ready_to_index = json_boolean_value(temp);
    }

    temp = json_object_get(j, J_NUM_FIELDS);
    if (temp && m->full_schema && json_is_number(temp)) {
        m->num_fields = json_number_value(temp);
    }
 
    // TODO: DRY
    temp = json_object_get(j, J_STRINGS);
    if (json_is_array(temp)) {
        size_t jid;
        json_t *js;
        json_array_foreach(temp, jid, js) {
            if (json_is_string(js)) {
                kv_push(char *, m->strings, strdup(json_string_value(js)));
            }
        }
    }
    m->num_strings = kv_size(m->strings);

    temp = json_object_get(j, J_NUMBERS);
    if (json_is_array(temp)) {
        size_t jid;
        json_t *js;
        json_array_foreach(temp, jid, js) {
            if (json_is_string(js)) {
                kv_push(char *, m->numbers, strdup(json_string_value(js)));
            }
        }
    }
    m->num_numbers = kv_size(m->numbers);

    temp = json_object_get(j, J_BOOLS);
    if (json_is_array(temp)) {
        size_t jid;
        json_t *js;
        json_array_foreach(temp, jid, js) {
            if (json_is_string(js)) {
                kv_push(char *, m->bools, strdup(json_string_value(js)));
            }
        }
    }
    m->num_bools = kv_size(m->bools);

    temp = json_object_get(j, J_FACETS);
    if (json_is_array(temp)) {
        size_t jid;
        json_t *js;
        json_array_foreach(temp, jid, js) {
            if (json_is_object(js)) {
                struct facet_info *fi = malloc(sizeof(struct facet_info));
                fi->name = strdup(json_string_value(json_object_get(js, "name")));
                fi->type = json_integer_value(json_object_get(js, "type"));
                kv_push(struct facet_info*, m->facets, fi);
            }
        }
    }
    m->num_facets = kv_size(m->facets);

    json_decref(j);
}

/* This parses the incoming json object and parses the fields */
void mapping_extract(struct mapping *m, json_t *j) {
    if (!m->full_schema) {
        m->full_schema = schema_new();
    }
    // If there are any updates  to the schema, save it
    if (schema_extract(m->full_schema, j, m) > 0 ) {
        // Save mapping as it has changed
        mapping_save(m);
    }
}

static struct schema *schema_dup(struct schema *in) {
    struct schema *out = schema_new();
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

static void reset_field_kv(struct mapping *m, bool init) {
    for (int i=0; i<kv_size(m->facets); i++) {
        struct facet_info *fi = kv_A(m->facets, i);
        free(fi->name);
        free(fi);
    }
    kv_destroy(m->facets);
    if (init) kv_init(m->facets);

    for (int i=0; i<kv_size(m->numbers); i++) {
        char *f = kv_A(m->numbers, i);
        free(f);
    }
    kv_destroy(m->numbers);
    if (init) kv_init(m->numbers);

    for (int i=0; i<kv_size(m->strings); i++) {
        char *f = kv_A(m->strings, i);
        free(f);
    }
    kv_destroy(m->strings);
    if (init) kv_init(m->strings);

    for (int i=0; i<kv_size(m->bools); i++) {
        char *f = kv_A(m->bools, i);
        free(f);
    }
    kv_destroy(m->bools);
    if (init) kv_init(m->bools);
    m->num_bools = m->num_facets = m->num_strings = m->num_numbers = 0;
}


const struct facet_info *get_facet_info(struct mapping *m, int facet_id) {
    if (facet_id < kv_size(m->facets)) {
        return kv_A(m->facets, facet_id);
    }
    return NULL;
}

/* Once full schema is available, find out all fields specified in the 
 * configuration for index_fields and facet_fields and update data.  
 * Till this function returns true, indexing of data does not start
 * just storage happens */
bool mapping_apply_config(struct mapping *m) {
    M_DBG("Apply configuration for mapping");
    // Make sure we have a full schema parsed and ready before starting to apply config
    if (!m->full_schema) return false;
    // First clear out existing index_schema and all field vectors
    if (m->index_schema) {
        schema_free(m->index_schema);
    }
    // Reset is_indexed, is_facet and priorities in full_schema
    if (m->full_schema) {
        schema_reset(m->full_schema);
    }
    reset_field_kv(m, true);

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
    m->index_schema = schema_new();
    extract_index_schema(m->full_schema, m->index_schema);
    // By now all configured fields have been found and type determined
    m->ready_to_index = true;
    mapping_save(m);
    return true;
}

void mapping_free(struct mapping *m) {
    if (m->full_schema) {
        schema_free(m->full_schema);
    }
    if (m->index_schema) {
        schema_free(m->index_schema);
    }
    reset_field_kv(m, false);
    free(m);
}

void mapping_delete(struct mapping *m) {
    // Get the mapping file path
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, m->index->app->name,
                                 m->index->name, MAPPING_FILE);

    // Free the mapping
    mapping_free(m);
    // Delete the mapping file
    unlink(path);
}

struct mapping* mapping_new(struct index *in) {
    struct mapping *m = calloc(1, sizeof(struct mapping));
    kv_init(m->strings);
    kv_init(m->numbers);
    kv_init(m->bools);
    kv_init(m->facets);
    m->index = in;
    mapping_load(m);
    return m;
}

