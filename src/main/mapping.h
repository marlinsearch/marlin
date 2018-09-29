#ifndef __MAPPING_H__
#define __MAPPING_H__

#include <jansson.h>
#include "common.h"

#define MAX_FIELD_NAME 256

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
    struct schema *next;    // Next property in this level
    struct schema *child;   // Child properties of an object
};

struct mapping {
    struct schema *full_schema;
    struct schema *index_schema;
    int num_fields;
};

struct mapping* mapping_new(void);
void mapping_extract(struct mapping *m, json_t *j);


#endif
