#include <h2o.h>
#include "index.h"
#include "utils.h"
#include "keys.h"
#include "url.h"
#include "api.h"
#include "platform.h"
#include "marlin.h"
#include "workers.h"
#include "analyzer.h"
#include "query.h"
#include "debug.h"
#include "filter.h"
#include "aggs.h"

#pragma GCC diagnostic ignored "-Wformat-truncation="
#define USE_INDEX_THREAD_POOL 1

struct api_path {
    const char *method;
    const char *path;
    KEY_ACCESS access;
    char *(*api_cb)(h2o_req_t *req, void *data);
};

/* Per thread data while adding documents */
struct add_obj_tdata {
    struct worker *tdata;
    struct index *index;
    json_t *sh_j; // Json object / array of documents for a shard
    int shard_idx;
};

void worker_add_process(void *w) {
    struct add_obj_tdata *add = w;
    shard_add_documents(kv_A(add->index->shards, add->shard_idx), add->sh_j);
    M_INFO("Shard worker %d len %lu", add->shard_idx, json_array_size(add->sh_j));
    json_decref(add->sh_j);
    worker_done(add->tdata);
}

void index_worker_add_documents(struct index *in, json_t **sh_j) {
    struct worker worker;
    worker_init(&worker, in->num_shards);
    struct add_obj_tdata *sh_add = malloc(in->num_shards * sizeof(struct add_obj_tdata));

    for (int i = 0; i < in->num_shards; i++) {
        if (json_array_size(sh_j[i])) {
            sh_add[i].tdata = &worker;
            sh_add[i].sh_j = sh_j[i];
            sh_add[i].index = in;
            sh_add[i].shard_idx = i;
            threadpool_add(index_pool, worker_add_process, &sh_add[i], 0);
        } else {
            // It is an empty json array, which needs to be freed
            json_decref(sh_j[i]);
            worker.pending--;
        }
    }

    wait_for_workers(&worker);
    worker_destroy(&worker);
    free(sh_add);
}

static void index_apply_config(struct index *in) {
    struct query_cfg *qcfg = in->cfg.qcfg;
    free(qcfg->facet_enabled);
    qcfg->facet_enabled = malloc(sizeof(int) * in->mapping->num_facets);
    for (int i = 0; i < in->mapping->num_facets; i++) {
        qcfg->facet_enabled[i] = 1;
    }

}

static void update_shard_mappings(struct index *in) {
    for (int i=0; i < kv_size(in->shards); i++) {
        shard_set_mapping(kv_A(in->shards, i), in->mapping);
    }
    index_apply_config(in);
}

/* When documents are added to an index, they may have to be parsed for schema
 * discovery.  This happens only when the index_schema is not yet ready. */
static void index_extract_document_mapping(struct index *in, json_t *j) {
    // The incoming json can be an array or a single json object as our
    // api to add documents supports both
    if (json_is_array(j)) {
        size_t index;
        json_t *jo;

        json_array_foreach(j, index, jo) {
            mapping_extract(in->mapping, jo);
        }
    } else {
        mapping_extract(in->mapping, j);
    }
    M_DBG("configured %d", in->cfg.configured);

    // Try to apply config to get index schema ready
    if (in->cfg.configured && mapping_apply_config(in->mapping)) {
        // If index schema is ready, reindex existing documents before
        // adding new documents
        // TODO: Reindex existing shards
        // Set mapping after reindexing
        update_shard_mappings(in);
    }
}

/* This is where new documents are added to the index.
 * First we generate a id for every document.  use the id
 * to determine which shard it belongs to and send it over */
static void index_add_documents(struct index *in, json_t *j) {
    // Perform schema extraction if necessary if we are not
    // ready to index yet.  This may also end up reindexing
    // existing documents in all shards
    if (UNLIKELY(!in->mapping->ready_to_index)) {
        index_extract_document_mapping(in, j);
    }

    if (json_is_array(j)) {
        size_t index;
        json_t *jo;
        // First create shard specific array of documents
        json_t **sh_j = malloc(sizeof(json_t *) * in->num_shards);
        for (int i = 0; i < in->num_shards; i++) {
            sh_j[i] = json_array();
        }

        // set the document id for every document and hash the document id
        // to send to the shard the document belongs.  Add it to the
        // shard array
        json_array_foreach(j, index, jo) {
            const char *orig_id = json_string_value(json_object_get(jo, J_ID));
            int sid = 0;
            if (!orig_id) {
                char *id = generate_docid(in->fctx);
                json_object_set_new(jo, J_ID, json_string(id));
                sid = get_shard_routing_id(id, in->num_shards);
                free(id);
            } else {
                sid = get_shard_routing_id(orig_id, in->num_shards);
            }
            json_array_append(sh_j[sid], jo);
        }

#ifndef USE_INDEX_THREAD_POOL
        // Finally add documents to the shards
        // When not using thread pool it is simple, just iterate and
        // get things done
        for (int i=0; i < in->num_shards; i++) {
            // send to shard
            shard_add_documents(kv_A(in->shards, i), sh_j[i]);
            printf("Shard %d len %lu\n", i, json_array_size(sh_j[i]));
            json_decref(sh_j[i]);
        }
#else
        // When we use a thread pool we need to handle some additional
        // stuff 
        index_worker_add_documents(in, sh_j);
#endif
        free(sh_j);
    } else {
        const char *orig_id = json_string_value(json_object_get(j, J_ID));
        int sid = 0;
        if (!orig_id) {
            char *id = generate_docid(in->fctx);
            json_object_set_new(j, J_ID, json_string(id));
            sid = get_shard_routing_id(id, in->num_shards);
            free(id);
        } else {
            sid = get_shard_routing_id(orig_id, in->num_shards);
        }
        shard_add_documents(kv_A(in->shards, sid), j);
    }
}

/* This is where any modifications to the index happen.  It happens one at a time
 * no two index jobs can ever run simultaneously and is controlled by the threadpool */
static void index_work_job(void *data) {
    struct in_job *job = data;
    struct index *in = job->index;
    if (in) {
        switch (job->type) {
            case JOB_ADD:
                index_add_documents(in, job->j);
                break;
            case JOB_DELETE: {
                struct shard *s = kv_A(in->shards, job->id);
                shard_delete_document(s, job->j);
                }
                break;
            case JOB_REPLACE: {
                struct shard *s = kv_A(in->shards, job->id);
                shard_replace_document(s, job->j, job->j2);
                }
                break;
            case JOB_UPDATE: {
                struct shard *s = kv_A(in->shards, job->id);
                shard_update_document(s, job->j, job->j2);
                }
                break;
            default:
                break;
        }
        ATOMIC_DEC(&in->job_count);
    }
    if (job->j) json_decref(job->j);
    if (job->j2) json_decref(job->j2);
    free(job);
}

/* This is how a job gets scheduled for an index.  It basically
 * adds the job to a threadpool, which invokes one job a time. */
static int index_add_job(struct index *in, struct in_job *job) {
    // No jobs, the wpool may need to be initialized
    if (in->job_count == 0) {
        RDLOCK(&in->wpool_lock);
        if (!in->wpool) {
            UNLOCK(&in->wpool_lock);
            WRLOCK(&in->wpool_lock);
            in->wpool = threadpool_create(1, JOB_QUEUE_LEN, 0);
            UNLOCK(&in->wpool_lock);
        } else {
            UNLOCK(&in->wpool_lock);
        }
    }
    int r = threadpool_add(in->wpool, index_work_job, job, 0);
    // Job added successfully
    if (r == 0) {
        ATOMIC_INC(&in->job_count);
    }
    return r;
}

/* Creates a new index job for a given index and job type */
static struct in_job *in_job_new(struct index *in, JOB_TYPE type) {
    struct in_job *job = calloc(1, sizeof(struct in_job));
    job->index = in;
    job->type = type;
    return job;
}

static void index_save_info(struct index *in) {
    json_t *j = json_object();
    json_object_set_new(j, J_NUM_SHARDS, json_integer(in->num_shards));
    json_object_set_new(j, J_CREATED, json_integer(in->time_created));
    json_object_set_new(j, J_UPDATED, json_integer(in->time_updated));
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, INDEX_FILE);
 
    FILE *fp = fopen(path, "w");
    if (fp) {
        char *jstr = json_dumps(j, JSON_PRESERVE_ORDER|JSON_INDENT(4));
        fprintf(fp, "%s", jstr);
        free(jstr);
        fclose(fp);
        json_decref(j);
    } else {
        M_ERR("Failed to store index info for %s/%s", in->app->name, in->name);
    }
}

/* This tries to load index information regarding shards and creation / update times
 * If none exists, dumps the current information in a index into the file.  The latter
 * happens when this is a newly created index */
static void index_load_info(struct index *in) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, INDEX_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_object(json)) {
        in->num_shards = json_number_value(json_object_get(json, J_NUM_SHARDS));
        in->time_created = json_number_value(json_object_get(json, J_CREATED));
        in->time_updated = json_number_value(json_object_get(json, J_UPDATED));
        json_decref(json);
        return;
    } else {
        index_save_info(in);
    }
}

/* Documents are being added to the index.  The request can either
 * be a single document or an array of documents.  We never try to update the
 * index right away.  A job is created and added to the job queue.  The
 * jobqueue gets processed serially one at at time */
static char *index_data_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, JSON_ALLOW_NUL, &error);
    if (!j) return strdup(J_FAILURE);
    struct in_job *job = in_job_new(in, JOB_ADD);
    job->j = j;
    index_add_job(in, job);
    M_DBG("Index job added type %d count %u", job->type, in->job_count);
    return api_success(req);
}

static json_t *index_get_info_json(struct index *in) {
    json_t *j = json_object();
    json_object_set_new(j, J_NAME, json_string(in->name));
    json_object_set_new(j, J_NUM_JOBS, json_integer(in->job_count));
    json_t *ja = json_array();
    size_t total_docs = 0;
    for (int i = 0; i < in->num_shards; i++) {
        struct shard *s = kv_A(in->shards, i);
        json_t *js = json_object();
        size_t shard_docs = bmap_cardinality(s->sdata->used_bmap);
        char shard_name[32];
        snprintf(shard_name, sizeof(shard_name), "shard-%d", i);
        json_object_set_new(js, J_NAME, json_string(shard_name));
        json_object_set_new(js, J_NUM_DOCS, json_integer(shard_docs));
        total_docs += shard_docs;
        json_array_append_new(ja, js);
    }
    json_object_set_new(j, J_NUM_DOCS, json_integer(total_docs));
    json_object_set_new(j, J_NUM_SHARDS, json_integer(in->num_shards));
    json_object_set_new(j, J_SHARDS, ja);
    return j;
}

static int cfg_set_list_fields(void *data, json_t *ja) {
    size_t index;
    json_t *value;
    int changed = 0;
    kvec_t(char *) *fields = data;
    // First check if any change has been made
    if (kv_size(*fields) == json_array_size(ja)) {
        json_array_foreach(ja, index, value) {
            if (json_is_string(value)) {
                if (strcmp(json_string_value(value), kv_A(*fields, index)) != 0) {
                    changed = 1;
                    break;
                }
            }
        }
    } else {
        changed = 1;
    }

    // If changed, clear old values and set new values
    if (changed) {
        // First clear old index fields
        while (kv_size(*fields) > 0) {
            char *f = kv_pop(*fields);
            free(f);
        }
        kv_destroy(*fields);
        kv_init(*fields);

        json_array_foreach(ja, index, value) {
            if (json_is_string(value)) {
                kv_push(char *, *fields, strdup(json_string_value(value)));
            }
        }
    }
    return changed;
}

static const char *cfg_set_rank_sort_field(struct query_cfg *qcfg, struct mapping *m, json_t *jo, bool sort) {
    const char *key;
    json_t *value;
    qcfg->rank_sort = sort;

    json_object_foreach(jo, key, value) {
        if (json_is_string(value)) {
            const char *dir = json_string_value(value);
            if (strcmp(dir, ORDER_ASC) == 0) {
                qcfg->rank_asc = true;
            } else if (strcmp(dir, ORDER_DESC) == 0){
                qcfg->rank_asc = false;
            } else {
                return "Invalid sort / rank direction specified";
            }
            // If we have a mapping use that to validate its ok
            if (m && m->index_schema) {
                struct schema *s = schema_find_field(m->index_schema->child, key);
                if (!s) {
                    return "The field to sort / rank by is not indexed";
                }
                if (s->type != F_NUMBER && s->type != F_NUMLIST) {
                    return "The field to sort / rank by is not a number";
                }
                // Finally set the indexed field id / priority
                qcfg->rank_by = s->i_priority;
            }
            // We do not have a mapping yet, just save the field name for now
            snprintf(qcfg->rank_by_field, sizeof(qcfg->rank_by_field), "%s", key);
        } else {
            return "Invalid sort / rank direction specified";
        }
    }

    return NULL;
}

static void fields_free(struct field *l) {
    if (l) {
        if (l->child) fields_free(l->child);
        if (l->next) fields_free(l->next);
        free(l);
    }
}

static struct field *lookup_field(struct field *f, const char *name) {
    while (f) {
        if (strcmp(f->name, name) == 0) {
            return f;
        }
        f = f->next;
    }
    return NULL;
}

static void parse_field_modifiers(struct field *f) {
    char *pos = strstr(f->name, ":");
    if (pos) {
        *pos = '\0';
        pos++;
        f->snippet = atoi(pos);
    }
}

/* Parses limit fields eg., highlighting, retreival, facets etc., 
 * NOTE: does not validate if field exists in schema and is correct */
static struct field *parse_limit_fields(json_t *ja) {
    struct field *lf = NULL, *cur = NULL;
    size_t index;
    json_t *value;
    json_array_foreach(ja, index, value) {
        if (json_is_string(value)) {
            struct field *newlf = NULL;
            // Make sure we have a field name
            const char *fname = json_string_value(value);
            if (!fname) continue;
            // Check if it is a inner field
            if (!strstr(fname, ".")) {
                newlf = lookup_field(lf, fname);
                // If not just add it
                if (LIKELY(!newlf)) {
                    newlf = calloc(1, sizeof(struct field));
                } else {
                    continue;
                }
                snprintf(newlf->name, sizeof(newlf->name), "%s", fname);
                parse_field_modifiers(newlf);
            } else {
                // Inner field.. setup newlf, which adds it to the root if necessary
                char *fnamecopy = strdup(fname);
                char *token = NULL;
                char *rest = fnamecopy;
                struct field *fparent = NULL;

                while((token = strtok_r(rest, ".", &rest))) {
                    struct field *fnew = lookup_field(fparent ? fparent : lf, token);
                    if (fnew) {
                        fparent = fnew;
                        continue;
                    } else {
                        fnew = calloc(1, sizeof(struct field));
                    }
                    snprintf(fnew->name, sizeof(fnew->name), "%s", token);
                    parse_field_modifiers(fnew);

                    if (!fparent) {
                        newlf = fnew;
                    } else {
                        fnew->next = fparent->child;
                        fparent->child = fnew;
                    }
                    fparent = fnew;
                }
                free(fnamecopy);
            }
            if (newlf) {
                if (!lf) {
                    lf = cur = newlf;
                } else {
                    cur->next = newlf;
                    cur = newlf;
                }
            }
        }
    }
    return lf;
}


static const char *parse_query_settings(struct json_t *j, struct query_cfg *qcfg, struct mapping *m, bool config) {
    const char *key;
    json_t *value;

    // TODO: Validate settings are within limits.. eg., maxhits cannot be more than 1000
    // hitsperpage cannot be more than maxhits etc.,
    json_object_foreach(j, key, value) {
        // hitsPerPage
        if (strcmp(J_S_HITS_PER_PAGE, key) == 0) {
            qcfg->hits_per_page = json_integer_value(value);
        }
        // maxHits
        if (strcmp(J_S_MAX_HITS, key) == 0) {
            qcfg->max_hits = json_integer_value(value);
        }
        // maxFacetResults
        if (strcmp(J_S_MAX_FACET_RESULTS, key) == 0) {
            qcfg->max_facet_results = json_integer_value(value);
        }
        // rankAlgorithm
        if (strcmp(J_S_RANKALGO, key) == 0) {
            if (!json_is_array(value)) {
                return "Rank algorithm needs to be an array";
            }
            size_t index;
            json_t *s;
            json_array_foreach(value, index, s) {
                if (!json_is_string(s)) {
                    return "Rank alorithms must be string";
                }
                SORT_RULE r = rulestr_to_rule(json_string_value(s));
                if (r == R_MAX) {
                    return "Unknown rank rule specified";
                }
                qcfg->rank_algo[index] = r;
            }
            qcfg->num_rules = index;
        }
        // rankBy / sortBy
        if (strcmp(J_S_RANK_BY, key) == 0) {
            const char *err = cfg_set_rank_sort_field(qcfg, m, value, false);
            if (err) return err;
        } else  if (strcmp(J_S_SORT_BY, key) == 0) {
            const char *err = cfg_set_rank_sort_field(qcfg, m, value, true);
            if (err) return err;
        }
        // fullScan
        if (strcmp(J_S_FULLSCAN, key) == 0) {
            qcfg->full_scan = json_is_true(value);
        }
        // fullScanThreshold
        if (strcmp(J_S_FULLSCAN_THRES, key) == 0) {
            qcfg->full_scan_threshold = json_number_value(value);
        }
        // getFields
        if (strcmp(J_S_GET_FIELDS, key) == 0 ) {
            if (json_is_array(value)) {
                if (qcfg->get_fields && config) {
                    fields_free(qcfg->get_fields);
                }
                qcfg->get_fields = parse_limit_fields(value);
            } else {
                return "getFields needs to be a string array of fields to retrieve";
            }
        }
        // highlightFields
        if (strcmp(J_S_HIGHLIGHT_FIELDS, key) == 0 ) {
            // An array of fields to highlight
            if (json_is_array(value)) {
                if (qcfg->highlight_fields && config) {
                    fields_free(qcfg->highlight_fields);
                }
                // ["*"] to highlight all values, handle that
                if (json_array_size(value) == 1) {
                    json_t *jav = json_array_get(value, 0);
                    if (json_is_string(jav) && strcmp(json_string_value(jav), "*") == 0) {
                        qcfg->highlight_fields = calloc(1, sizeof(struct field));
                        continue;
                    } 
                } 
                // Nothing matched, parse fields as usual
                qcfg->highlight_fields = parse_limit_fields(value);
            // Null to highlight nothing
            } else if (json_is_null(value)) {
                if (qcfg->highlight_fields && config) {
                    fields_free(qcfg->highlight_fields);
                }
                qcfg->highlight_fields = NULL;
            } else {
                return "highlightFields needs to be a string array of fields to highlight";
            }
        }
        // highlightSource
        if (strcmp(J_S_HIGHLIGHT_SOURCE, key) == 0 ) {
            qcfg->highlight_source = json_is_true(value);
        }
 
    }
    if (qcfg->hits_per_page > qcfg->max_hits) {
        return "Hits per page cannot be more than maximum hits";
    }
    if (qcfg->max_hits > MAX_HITS_LIMIT) {
        return "Maximum hits cannot be more than 1000";
    }
    if (qcfg->full_scan_threshold < MAX_HITS_LIMIT * 5) {
        return "Full scan threshold cannot be less than 5000";
    }
    return NULL;
}

static const char *index_load_json_settings(struct index *in, json_t *j, int *c) {
    const char *key;
    json_t *value;
    // Make sure we are getting valid settings first
    // Validation here
    json_object_foreach(j, key, value) {
        if ((strcmp(J_S_INDEXFIELDS, key) == 0) && !is_json_string_array(value)) {
            return "indexFields should be a string array";
        }
        if ((strcmp(J_S_FACETFIELDS, key) == 0) && !is_json_string_array(value)) {
            return "facetFields should be a string array";
        }
        if ((strcmp(J_S_HITS_PER_PAGE, key) == 0) && !json_is_number(value)) {
            return "hitsPerPage should be a number";
        }
        if ((strcmp(J_S_MAX_HITS, key) == 0) && !json_is_number(value)) {
            return "maxHits should be a number";
        }
        if ((strcmp(J_S_MAX_FACET_RESULTS, key) == 0) && !json_is_number(value)) {
            return "maxFacetResults should be a number";
        }
        if ((strcmp(J_S_RANK_BY, key) == 0) && !json_is_object(value)) {
            return "rankBy should be an object";
        }
        if ((strcmp(J_S_SORT_BY, key) == 0) && !json_is_object(value)) {
            return "sortBy should be an object";
        }
        if ((strcmp(J_S_FULLSCAN, key) == 0) && !json_is_boolean(value)) {
            return "fullScan should be a boolean";
        }
        if ((strcmp(J_S_FULLSCAN_THRES, key) == 0) && !json_is_number(value)) {
            return "fullScanThreshold should be a number";
        }
        // Get fields is an array of fields or null 
        if ((strcmp(J_S_GET_FIELDS, key) == 0) && (!is_json_string_array(value) && !json_is_null(value))) {
            return "getFields should be a string array or null";
        }
        if ((strcmp(J_S_HIGHLIGHT_FIELDS, key) == 0) && (!is_json_string_array(value) && !json_is_null(value))) {
            printf("type is %d ", json_typeof(value));
            return "highlightFields should be a string array or null";
        }
        if ((strcmp(J_S_HIGHLIGHT_SOURCE, key) == 0) && !json_is_boolean(value)) {
            return "highlightFields should be a boolean";
        }
    }
    // Now do the actual parsing of settings
    int changed = 0;
    json_object_foreach(j, key, value) {
        if (strcmp(J_S_INDEXFIELDS, key) == 0) {
            changed |= cfg_set_list_fields(&in->cfg.index_fields, value);
        }
        if (strcmp(J_S_FACETFIELDS, key) == 0) {
            changed |= cfg_set_list_fields(&in->cfg.facet_fields, value);
        }
    }
    *c = changed;

    const char *parse_fail = parse_query_settings(j, in->cfg.qcfg, in->mapping, true);
    if (parse_fail) {
        M_ERR("Parse error : %s", parse_fail);
        return parse_fail;
    }
 
    // We need atleast index fields to be configured, before we are in configured status
    // unless this happens, we do not start indexing data
    if (kv_size(in->cfg.index_fields)) {
        in->cfg.configured = true;
    }
    return NULL;
}

/* Fills fields in array format
 * NOTE: path needs to be of size MAX_FIELD_NAME * 10*/
static void fill_fields(json_t *ja, struct field *f, char *path) {
    while (f) {
        if (f->child) {
            if (strlen(path) > (MAX_FIELD_NAME * 9)) return;
            strcat(path, f->name);
            strcat(path, ".");
            fill_fields(ja, f, path);
            path[strlen(path) - strlen(f->name) - 1] = '\0';
        } else {
            if (strlen(path)) {
                char newpath[MAX_FIELD_NAME * 10];
                snprintf(newpath, sizeof(newpath), "%s%s", path, f->name);
                json_array_append_new(ja, json_string(newpath));
            } else {
                json_array_append_new(ja, json_string(f->name));
            }
        }
        f = f->next;
    }
}

static char *index_settings_to_json(const struct index *in) {
    json_t *jo = json_object();
    struct query_cfg *qcfg = in->cfg.qcfg;
    // set index fields
    json_t *ji = json_array();
    for (int i=0; i<kv_size(in->cfg.index_fields); i++) {
        json_array_append_new(ji, json_string(kv_A(in->cfg.index_fields, i)));
    }
    json_object_set_new(jo, J_S_INDEXFIELDS, ji);
    // set facet fields
    json_t *jf = json_array();
    for (int i=0; i<kv_size(in->cfg.facet_fields); i++) {
        json_array_append_new(jf, json_string(kv_A(in->cfg.facet_fields, i)));
    }

    json_object_set_new(jo, J_S_FACETFIELDS, jf);
    json_object_set_new(jo, J_S_HITS_PER_PAGE, json_integer(qcfg->hits_per_page));
    json_object_set_new(jo, J_S_MAX_HITS, json_integer(qcfg->max_hits));
    json_object_set_new(jo, J_S_MAX_FACET_RESULTS, json_integer(qcfg->max_facet_results));
    json_object_set_new(jo, J_S_FULLSCAN, json_boolean(qcfg->full_scan));
    json_object_set_new(jo, J_S_FULLSCAN_THRES, json_integer(qcfg->full_scan_threshold));

    // Save rules
    json_t *jr = json_array();
    json_object_set_new(jo, J_S_RANKALGO, jr);
    for (int i = 0; i < qcfg->num_rules; i++) {
        const char *rstr = rule_to_rulestr(qcfg->rank_algo[i]);
        if (rstr) {
            json_array_append_new(jr, json_string(rstr));
        }
    }

    // We have a rank / sort by field
    if (qcfg->rank_by_field[0] != '\0') {
        json_t *jr = json_object();
        json_object_set_new(jr, qcfg->rank_by_field, json_string(qcfg->rank_asc ? ORDER_ASC : ORDER_DESC));
        json_object_set_new(jo, qcfg->rank_sort ? J_S_SORT_BY : J_S_RANK_BY, jr);
    }

    if (qcfg->get_fields) {
        json_t *jgf = json_array();
        json_object_set_new(jo, J_S_GET_FIELDS, jgf);
        char buf[MAX_FIELD_NAME * 10] = "";
        fill_fields(jgf, qcfg->get_fields, buf);
    }

    if (qcfg->highlight_fields && qcfg->highlight_fields->name[0] != '\0') {
        json_t *jhf = json_array();
        json_object_set_new(jo, J_S_HIGHLIGHT_FIELDS, jhf);
        char buf[MAX_FIELD_NAME * 10] = "";
        fill_fields(jhf, qcfg->highlight_fields, buf);
    }

    if (!qcfg->highlight_fields) {
        json_object_set_new(jo, J_S_HIGHLIGHT_FIELDS, json_null());
    }

    if (qcfg->highlight_source) {
        json_object_set_new(jo, J_S_HIGHLIGHT_SOURCE, json_true());
    }
 
    char *resp = json_dumps(jo, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(jo);
    return resp;
}

static void index_save_settings(struct index *in) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, SETTINGS_FILE);
    FILE *f = fopen(path, "w");
    if (!f) {
        M_ERR("Failed to save index settings");
        return;
    }
    char *s = index_settings_to_json(in);
    fprintf(f, "%s", s);
    free(s);
    fclose(f);
}

static char *index_set_settings_callback(h2o_req_t *req, void *data) {
    struct index *in = (struct index *) data;
    json_error_t error;
    const char *errstr = NULL;
    json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);

    if (j && json_is_object(j)) {
        int changed = 0;
        if ((errstr = index_load_json_settings(in, j, &changed)) != NULL) {
            goto jerror;
        }
        index_save_settings(in);
        if (changed) {
            mapping_apply_config(in->mapping);
        }
        json_decref(j);
        // TODO: Ask all shards to reindex existing data
        // TODO: This should be a background job, settings callback should return
        // immediately
        if (in->mapping->ready_to_index) {
            update_shard_mappings(in);
        }
        return api_success(req);
    } else {
        M_ERR("Json error %s", error.text);
    }
jerror:
    M_ERR("Parse error : %s", errstr);
    json_decref(j);
    req->res.status = 400;
    req->res.reason = errstr ? errstr : "Bad Request";
    return strdup(J_FAILURE);
}

static char *index_get_settings_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    char *s = index_settings_to_json(in);
    return s;
}

static char *index_info_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    json_t *j = index_get_info_json(in);
    char *response = json_dumps(j, JSON_PRESERVE_ORDER|JSON_INDENT(4));
    json_decref(j);
    return response;
}

static char *index_mapping_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    char *response = mapping_to_json_str(in->mapping);
    return response;
}

/* Deletes an index.  THis actually informs the app containing the index
 * to perform the deletion.  This lets the app do its bookeeping. This 
 * handler is here and not in app as delete can be invoked by user keys
 * with delete permission, which is enforced here */
//TODO: This should be a delete job as requests may be in progress
static char *index_delete_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    M_INFO("Deleting index %s", in->name);
    if (app_delete_index(in->app, in)) {
        return api_success(req);
    } else {
        return api_bad_request(req);
    }
}

static void index_destroy_threadpool(struct index *in) {
    WRLOCK(&in->wpool_lock);
    if (in->wpool) {
        M_INFO("Destroying threadpool for index %s", in->name);
        // TODO: Free threadpool jobs if any
        threadpool_destroy(in->wpool, 0);
        in->wpool = NULL;
    }
    UNLOCK(&in->wpool_lock);
}

// TODO: This should be a clear job as requests may be in progress?
static char *index_clear_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    index_destroy_threadpool(in);
    for (int i = 0; i < in->num_shards; i++) {
        struct shard *s = kv_A(in->shards, i);
        shard_clear(s);
    }
    return api_success(req);
}

/* Called by the analyzer for every word in the query string.
 * store a copy of the word in the query and update num words*/
static void query_string_word_cb(word_pos_t *wp, void *data) {
    struct query *q = (struct query *) data;
    word_t *word = malloc(sizeof(word_t));
    word->length = wp->word.length;
    word->chars = malloc(sizeof(chr_t) * word->length);
    memcpy(word->chars, wp->word.chars, sizeof(chr_t) * word->length);
    q->num_words++;
    kv_push(word_t *, q->words, word);
}


static char *parse_req_document_id(struct index *in, h2o_req_t *req) {
    char strid[PATH_MAX];
    int path_len = req->query_at < req->path.len ? req->query_at: req->path.len;
    int start = path_len;
    while (start > 3) {
        start--;
        if (req->path.base[start] == '/') {
            if ((path_len - start) > PATH_MAX) {
                M_ERR("Document id too big");
                req->res.status = 400;
                req->res.reason = "Bad Request";
                return NULL;
            } else {
                memcpy(strid, &req->path.base[start+1], path_len+1-start);
                strid[path_len-start-1] = '\0';
            }
            break;
        }
    }
    return strdup(strid);
}


static char *index_get_document_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    char *resp = NULL;
    char *id = parse_req_document_id(in, req);
    if (id) {
        int sid = get_shard_routing_id(id, in->num_shards);
        char *doc = shard_get_document(kv_A(in->shards, sid), id);
        if (!doc) {
            req->res.status = 404;
            req->res.reason = "Not Found";
            doc = strdup(J_FAILURE);
        }
        resp = doc;
    } else {
        resp = strdup(J_FAILURE);
        goto send_response;
    }
    free(id);
send_response:
    return resp;
}

static char *failure_message(const char *msg) {
    char *tmp = malloc(PATH_MAX);
    snprintf(tmp, PATH_MAX, "{\"success\": false, \"message\": \"%s\"}", msg);
    return tmp;
}


/* Performs the job type replace / update on a document.  A previous document of the same
 * id may or may not be required based on type */
static char *index_document_job(h2o_req_t *req, void *data, JOB_TYPE type, bool required) {
    struct index *in = data;
    // First load the incoming document
    json_error_t error;
    json_t *j = json_loadb(req->entity.base, req->entity.len, 0, &error);
    if (!j) {
        return http_error(req, HTTP_BAD_REQUEST);
    }
 
    // Parse the document id
    char *id = parse_req_document_id(in, req);
    if (!id) {
        json_decref(j);
        return http_error(req, HTTP_BAD_REQUEST);
    }

    // Try to lookup the current document
    int sid = get_shard_routing_id(id, in->num_shards);
    char *doc = shard_get_document(kv_A(in->shards, sid), id);

    // We could not find the current document
    if (required && !doc) {
        json_decref(j);
        free(id);
        return http_error(req, HTTP_NOT_FOUND);
    }

    // J2 holds the old document
    json_t *j2 = json_loads(doc, 0, &error);
    if (!j2 && required) {
        json_decref(j);
        free(id);
        free(doc);
        return http_error(req, HTTP_SERVER_ERROR);
    }

    // Set the id for the new document
    json_object_set_new(j, J_ID, json_string(id));

    // Create a new job 
    struct in_job *job = in_job_new(in, type);
    job->index = in;
    job->id = sid;
    job->j = j;
    job->j2 = j2;

    free(id);
    free(doc);

    // Handle index job addition failure
    if (index_add_job(in, job) == 0) {
        return api_success(req);
    } else {
        return http_error(req, HTTP_TOO_MANY);
    }
}

/* A PUT replaces the object */
static char *index_replace_document_callback(h2o_req_t *req, void *data) {
    // Create a relace document job
    return index_document_job(req, data, JOB_REPLACE, false);
}

/* A PATCH updates the object */
static char *index_update_document_callback(h2o_req_t *req, void *data) {
    // Create an update document job
    return index_document_job(req, data, JOB_UPDATE, true);
}

/* Deletes a single document from the index by the document id */
static char *index_delete_document_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    char *resp = NULL;
    char *id = parse_req_document_id(in, req);
    if (id) {
        int sid = get_shard_routing_id(id, in->num_shards);
        char *jd = shard_get_document(kv_A(in->shards, sid), id);
        // We do not need id anymore
        free(id);
        // Handle errors if any
        if (!jd) {
            return http_error(req, HTTP_NOT_FOUND);
        } else {
            // Load the document to be deleted
            json_error_t error;
            json_t *j = json_loads(jd, 0, &error);
            free(jd);
            if (!j) {
                return http_error(req, HTTP_SERVER_ERROR);
            }
            struct in_job *job = in_job_new(in, JOB_DELETE);
            job->j = j;
            job->id = sid;
            if (index_add_job(in, job) == 0) {
                resp = api_success(req);
            } else {
                return http_error(req, HTTP_TOO_MANY);
            }
        }
    } else {
        resp = api_bad_request(req);
    }
    return resp;
}

static char *index_query_callback(h2o_req_t *req, void *data) {
    struct index *in = data;
    // TODO: Apply query limits 
    struct query *q = NULL;
    char *response = NULL;

    // Load and parse the query object
    json_error_t error;
    json_t *jq = json_loadb(req->entity.base, req->entity.len, 0, &error);

    if (jq && json_is_object(jq)) {
        q = query_new(in);
        q->page_num = 1; // By default get the first page
        
        // Copy default query settings for the index
        memcpy(&q->cfg, in->cfg.qcfg, sizeof(struct query_cfg));

        // Parse query config overrides and other query info
        const char *fail = parse_query_settings(jq, &q->cfg, in->mapping, false);
        if (fail) {
            response = failure_message(fail);
            req->res.status = 400;
            goto send_response;
        }

        // Parse filters
        json_t *jf = json_object_get(jq, J_FILTER);
        // We have a filter, let us parse it
        if (jf && !json_is_null(jf) && json_object_size(jf)) {
            q->filter = parse_filter(in, jf);
            if (!q->filter) {
                req->res.status = 400;
                response = failure_message("Query filter json parsing error");
                goto send_response;
            }
            if (q->filter->type == F_ERROR) {
                req->res.status = 400;
                response = failure_message(q->filter->error);
                goto send_response;
            }
        }
        json_t *ja = json_object_get(jq, J_AGGS);
        // We have a filter, let us parse it
        if (ja && !json_is_null(ja) && json_object_size(ja)) {
            q->agg = parse_aggs(ja, in);
        }

        json_t *jp = json_object_get(jq, J_PAGE);
        if (jp && json_is_integer(jp)) {
            q->page_num = json_integer_value(jp);
        }
        // Make sure page is within our limits
        if ((q->page_num <= 0) || (q->page_num * q->cfg.hits_per_page > q->cfg.max_hits)) {
            M_DBG("Query request for page '%d' more than our limit", q->page_num);
            req->res.status = 400;
            response = failure_message("Page number requested over limit");
            goto send_response;
        }

        json_t *je = json_object_get(jq, J_EXPLAIN);
        if (je && json_is_boolean(je)) {
            q->explain = json_is_true(je);
        }

        // Update rank_rule with rankBy or sortBy
        int rcount = 0;
        if (q->cfg.rank_by >= 0) {
            if (q->cfg.rank_sort) {
                q->rank_rule[0] = q->cfg.rank_asc ? R_COMP_ASC : R_COMP;
                q->rank_rule[q->cfg.num_rules + 1] = R_DONE;
                rcount++;
            } else {
                q->rank_rule[q->cfg.num_rules] = q->cfg.rank_asc ? R_COMP_ASC : R_COMP;
                q->rank_rule[q->cfg.num_rules + 1] = R_DONE;
            }
        } else {
            q->rank_rule[q->cfg.num_rules] = R_DONE;
        }
        memcpy(&q->rank_rule[rcount], q->cfg.rank_algo, q->cfg.num_rules * sizeof(SORT_RULE));

        const char *qstr = json_string_value(json_object_get(jq, J_QUERY));
        if (qstr) {
            q->text = strdup(qstr);
            struct analyzer *a = get_default_analyzer();
            a->analyze_string_for_search(qstr, query_string_word_cb, q);
        }
        generate_query_terms(q);
        dump_query(q);

        response = execute_query(q);
    } else {
        req->res.status = 400;
        return strdup(J_FAILURE);
    }

send_response:
    if (q) {
        // Free anything that was overridden
        if (q->cfg.get_fields != q->in->cfg.qcfg->get_fields) {
            fields_free(q->cfg.get_fields);
        }
        if (q->cfg.highlight_fields != q->in->cfg.qcfg->highlight_fields) {
            fields_free(q->cfg.highlight_fields);
        }
        query_free(q);
    }
    json_decref(jq);

    // Send query response
    return response;
}


static void index_load_settings(struct index *in) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, SETTINGS_FILE);
    json_t *json;
    json_error_t error;
    json = json_load_file(path, 0, &error);
    if (json && json_is_object(json)) {
        int changed = 0;
        index_load_json_settings(in, json, &changed);
        json_decref(json);
    }
}

struct schema *get_field_schema(struct index *in, const char *key) {
    if (UNLIKELY(!in->mapping) || !in->mapping->index_schema) {
        M_ERR("Mapping not set");
        return NULL;
    }
    return schema_find_field(in->mapping->index_schema->child, key);
}

const struct api_path apipaths[] = {
    // Set Settings
    {"POST", URL_SETTINGS, KA_S_CONFIG, index_set_settings_callback},
    // Get current settings for this index
    {"GET", URL_SETTINGS, KA_G_CONFIG, index_get_settings_callback},
    // Lets you add new documents to this index
    {"POST", NULL, KA_ADD, index_data_callback},
    // Index info
    {"GET", URL_INFO, KA_QUERY|KA_BROWSE, index_info_callback},
    // Mapping
    {"GET", URL_MAPPING, KA_G_CONFIG, index_mapping_callback},
    // Delete index
    {"DELETE", NULL, KA_DELETE, index_delete_callback},
    // Clear Index
    {"POST", URL_CLEAR, KA_DELETE, index_clear_callback},
    // Bulk
    // Query Index
    {"POST", URL_QUERY, KA_QUERY, index_query_callback},
    // Get a single document
    {"GET", URL_MULTI, KA_BROWSE, index_get_document_callback},
    // delete document
    {"DELETE", URL_MULTI, KA_DELETE, index_delete_document_callback},
    // replace object
    {"PUT", URL_MULTI, KA_UPDATE, index_replace_document_callback},
    // update object
    {"PATCH", URL_MULTI, KA_UPDATE, index_update_document_callback},
    // Done here
    {"", "", KA_NONE, NULL}
    /*
    // Browse
    {"GET", NULL, KA_BROWSE, index_browse_callback},
    // Post data
    {"POST", NULL, KA_ADD, index_data_callback},
    // Reindex
    {"POST", URL_REINDEX, KA_S_CONFIG, index_reindex_callback},
    // Index Mapping
    {"GET", URL_INDEX_MAPPING, KA_G_CONFIG, index_index_mapping_callback},
    // Query get
    // {"GET", URL_QUERY, KA_QUERY, index_query_callback},
    // Bulk
    {"POST", URL_BULK, KA_DELETE|KA_ADD|KA_UPDATE, index_bulk_callback},
    // replace object
    {"PUT", URL_MULTI, KA_UPDATE, index_replace_object_callback},
    // update object
    {"PATCH", URL_MULTI, KA_UPDATE, index_update_object_callback},
    {"", "", KA_NONE, NULL}
    */
};

/* Applies a given key to the index, this registers all necessary callbacks for this key
 * and handles permissions */
void index_apply_key(struct index *in, struct key *k, KEY_ACCESS access) {
    char path[PATH_MAX];

    int idx = 0;
    while(1) {
        const struct api_path *ap = &apipaths[idx];
        if (ap->api_cb == NULL) break;
        if (ap->path) {
            sprintf(path, "%s/%s/%s", URL_INDEXES, in->name, ap->path);
        } else {
            sprintf(path, "%s/%s", URL_INDEXES, in->name);
        }
        register_api_callback(in->app->appid, k->apikey, ap->method,
             path, url_cbdata_new((access & ap->access)?ap->api_cb:api_forbidden, in));
        idx++;
    }
}

/* Revokes a key for an index */
// NOTE: index_apply_key, index_delete_key, index_register_handlers all 3 need to be updated
void index_delete_key(struct index *in, struct key *k) {
    char path[PATH_MAX];
    int idx = 0;
    while(1) {
        const struct api_path *ap = &apipaths[idx];
        if (ap->api_cb == NULL) break;
        if (ap->path) {
            sprintf(path, "%s/%s/%s", URL_INDEXES, in->name, ap->path);
        } else {
            sprintf(path, "%s/%s", URL_INDEXES, in->name);
        }
        deregister_api_callback(in->app->appid, k->apikey, ap->method, path);
        idx++;
    }
}

/* Handles registering all handlers for this index, this is only to be used for app keys */
// NOTE: index_apply_key, index_delete_key, index_register_handlers all 3 need to be updated
static void setup_index_handlers(struct index *in, const char *appid, const char *apikey, bool reg) {
    char path[PATH_MAX];
    int idx = 0;
    while(1) {
        const struct api_path *ap = &apipaths[idx];
        if (ap->api_cb == NULL) break;
        if (ap->path) {
            sprintf(path, "%s/%s/%s", URL_INDEXES, in->name, ap->path);
        } else {
            sprintf(path, "%s/%s", URL_INDEXES, in->name);
        }
        if (reg) {
            register_api_callback(appid, apikey, ap->method, path, url_cbdata_new(ap->api_cb, in));
        } else {
            deregister_api_callback(appid, apikey, ap->method, path);
        }
        idx++;
    }
}

static struct query_cfg *query_config_new(void) {
    struct query_cfg *qcfg = calloc(1, sizeof(struct query_cfg));
    qcfg->hits_per_page = DEF_HITS_PER_PAGE;
    qcfg->max_facet_results = DEF_FACET_RESULTS;
    qcfg->full_scan_threshold = DEF_FULLSCAN_THRES;
    qcfg->max_hits = DEF_MAX_HITS;
    qcfg->rank_by = -1;         // Rank by no fields
    qcfg->rank_sort = false;    // Apply ranking finally
    qcfg->rank_asc  = false;    // Rank in descending order
    qcfg->num_rules = default_num_rules;
    memcpy(qcfg->rank_algo, &default_rule, sizeof(SORT_RULE) * default_num_rules);
    qcfg->highlight_fields = calloc(1, sizeof(struct field));
    return qcfg;
}

/* Creates a new index or loads existing index */
struct index *index_new(const char *name, struct app *a, int num_shards) {
    struct index *in = calloc(1, sizeof(struct index));
    snprintf(in->name, sizeof(in->name), "%s", name);
    in->app = a;
    in->num_shards = num_shards;
    in->time_created = get_utc_seconds();
    in->fctx = flakeid_ctx_create_with_spoof(NULL);
    in->mapping = mapping_new(in);
    kv_init(in->shards);

    // Initialize config
    in->cfg.configured = false;
    kv_init(in->cfg.index_fields);
    kv_init(in->cfg.facet_fields);

    // Setup default query config
    in->cfg.qcfg = query_config_new();

    // Create index path if not present
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, a->name, in->name);
    mkdir(path, 0775);
    // create data path to store index data
    snprintf(path, sizeof(path), "%s/%s/%s/data", marlin->db_path, a->name, in->name);
    mkdir(path, 0775);
    // Load / Update stored index information if any
    index_load_info(in);
    index_load_settings(in);

    // NOTE : Only after updating information do we have the correct number of shards
    // TODO: Properly handle shards when more than 1 node is present and all shards
    // of an index may not be on the same node
    // For now it is just shards by id.. so load it
    for (int i = 0; i < in->num_shards; i++) {
        struct shard *s = shard_new(in, i);
        if (in->mapping->ready_to_index) {
            shard_set_mapping(s, in->mapping);
        }
        kv_push(struct shard*, in->shards, s);
    }

    if (in->mapping->ready_to_index) {
        update_shard_mappings(in);
    }

    // Register the handlers for the app
    setup_index_handlers(in, a->appid, a->apikey, true);

    return in;
}

static void free_kvec_strings(void *kv) {
    kvec_t(char *) *fields = kv;
    while (kv_size(*fields) > 0) {
        char *f = kv_pop(*fields);
        free(f);
    }
    kv_destroy(*fields);
}

static void free_query_cfg(struct query_cfg *qcfg) {
    fields_free(qcfg->get_fields);
    fields_free(qcfg->highlight_fields);
    free(qcfg->facet_enabled);
    free(qcfg);
}

void index_free(struct index *in) {
    // Deregister handlers for this app
    setup_index_handlers(in, in->app->appid, in->app->apikey, false);
    // Destroy the threadpool if running
    index_destroy_threadpool(in);
    // Remove flake id context
    flakeid_ctx_destroy(in->fctx);
    // Free all the shards
    for (int i = 0; i < in->num_shards; i++) {
        struct shard *s = kv_A(in->shards, i);
        shard_free(s);
    }
    kv_destroy(in->shards);
    if (in->mapping) {
        mapping_free(in->mapping);
    }
    free_kvec_strings(&in->cfg.index_fields);
    free_kvec_strings(&in->cfg.facet_fields);
    free_query_cfg(in->cfg.qcfg);
    free(in);
}

void index_delete(struct index *in) {
    index_destroy_threadpool(in);
    // Just clear all the shards after destroying the threadpool
    for (int i = 0; i < in->num_shards; i++) {
        struct shard *s = kv_A(in->shards, i);
        shard_delete(s);
    }
    // set num_shards to 0.  We already deleted it, not need to free it
    in->num_shards = 0;
    mapping_delete(in->mapping);
    in->mapping = NULL;
    // Delete the index file
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, INDEX_FILE);
    unlink(path);
    // Delete the settings file
    snprintf(path, sizeof(path), "%s/%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name, SETTINGS_FILE);
    unlink(path);
    snprintf(path, sizeof(path), "%s/%s/%s/data", marlin->db_path, in->app->name, 
                                 in->name);
    // Delete the index/data folder
    rmdir(path);
    snprintf(path, sizeof(path), "%s/%s/%s", marlin->db_path, in->app->name, 
                                 in->name);
    // Delete the index folder
    rmdir(path);
    // Finally free the index itself
    index_free(in);
}


