#include "aggs.h"
#include "metric-aggs.h"

struct agg_info {
    const char *agg_name;
    struct agg *(*parse_cb)(const char *name, json_t *j, struct index *in);
};


static struct agg *parse_root_agg(const char *name, json_t *j, struct index *in) {
    struct agg *root = calloc(1, sizeof(struct agg));
    root->kind = AGGK_ROOT;
    root->type = AGG_ROOT;
    kv_init(root->children);

    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        struct agg *c = detect_and_parse(key, value, in);
        // If we cannot detect the type, its an error
        if (!c) {
            root->type = AGG_ERROR;
            snprintf(root->name, sizeof(root->name), "Could not parse agg %s", key);
            break;
        } else if (c->type == AGG_ERROR) {
            // If we detected a type but encountered an error while parsing
            // aggregation, copy over the error
            root->type = AGG_ERROR;
            snprintf(root->name, sizeof(root->name), "%s", c->name);
            break;
        } else {
            // We have a properly parsed aggregation, add it as a child
            kv_push(agg_t *, root->children, c);
        }
    }

    return root;
}


/* Each aggregation needs to be parsed differently based on its type and kind */
const struct agg_info aggs[] = {
    // Root aggregation
    {"aggs", parse_root_agg},
    // Max - metric aggregation
    {"max", parse_max_agg},
    {NULL, NULL}
};


struct agg *detect_and_parse(const char *name, json_t *j, struct index *in) {
    struct agg *nagg = NULL;
    struct agg *agg = NULL;

    // Iterate, detect and parse aggregation
    const char *key;
    json_t *value;
    json_object_foreach(j, key, value) {
        int c = 0;
        while (aggs[c].agg_name) {
            // If we see a matching aggregation, use that
            if (strcmp(key, aggs[c].agg_name) == 0) {
                struct agg *a = aggs[c].parse_cb(name, value, in);
                if (strcmp(key, "aggs") == 0) {
                    nagg = a;
                } else {
                    agg = a;
                }
            }
            c++;
        }
    }

    // Handle nested aggregation of bucket aggregations
    if (agg && agg->kind == AGGK_BUCKET) {
        agg->naggs = nagg;
    }

    return agg;
}

/* Used to parse aggregations and all nested aggregations under it.  If parsing
 * fails for any of the aggregation, it is propogated to the top root aggregation.
 * The aggregation type is set to AGG_ERROR and the name field contains the error
 * message*/
struct agg *parse_aggs(const char *agg_key, const char *name, json_t *j, struct index *in) {
    return NULL;
}
