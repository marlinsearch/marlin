#ifndef __MAPPING_H__
#define __MAPPING_H__

#include <jansson.h>
#include "common.h"
#include "index.h"

typedef enum field_type {
    F_NULL,
    F_STRING,
    F_STRLIST,
    F_NUMBER,
    F_NUMLIST,
    F_BOOLEAN,
    F_OBJECT,
    F_OBJLIST,
    F_GEO,
    F_GEOLIST,
    F_LIST,
    F_MAX
} F_TYPE;

struct schema {
    char fname[MAX_FIELD_NAME];      // Field name
    F_TYPE type;            // Field type
    bool is_indexed;        // Is this field indexed
    bool is_facet;          // Is this field a facet
    int field_id;           // Sequential field id
    int i_priority;         // Index priority
    int f_priority;         // Facet priority
    struct schema *next;    // Next property in this level
    struct schema *child;   // Child properties of an object
};

struct facet_info {
    char *name;
    F_TYPE type;
};

struct mapping {
    struct index *index;
    struct schema *full_schema;
    struct schema *index_schema;
    bool ready_to_index;
    int num_fields;
    int num_strings;
    int num_numbers;
    int num_bools;
    int num_facets;

    // THe field names parsed
    kvec_t(char *) strings;
    kvec_t(char *) numbers;
    kvec_t(char *) bools;
    kvec_t(char *) geos;
    kvec_t(struct facet_info *) facets;
};

struct mapping* mapping_new(struct index *index);
void mapping_extract(struct mapping *m, json_t *j);
bool mapping_apply_config(struct mapping *m);
void mapping_free(struct mapping *m);
void mapping_delete(struct mapping *m);
char *mapping_to_json_str(const struct mapping *m);
const char * type_to_str(F_TYPE type);
struct schema *schema_find_field(struct schema *s, const char *name);
const struct facet_info *get_facet_info(struct mapping *m, int facet_id);

#endif
