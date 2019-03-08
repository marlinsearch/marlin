#ifndef __SDATA_H_
#define __SDATA_H_

#include <lmdb.h>
#include "shard.h"
#include "mbmap.h"

/* sdata holds the document data for a shard */
struct sdata {
    struct shard *shard;
    uint32_t last_docid;

    // LMDB
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi docid2json_dbi;     // Maps docid to json data
    MDB_dbi id2docid_dbi;       // Maps the document id to shard bitmap id (docid)
    MDB_dbi usedfree_dbi;       // Maps the used and free docids

    // Used and free docid bmaps
    struct mbmap *free_mbmap;
    struct mbmap *used_mbmap;
    struct bmap *used_bmap;
    struct bmap *free_bmap;
};

struct sdata *sdata_new(struct shard *s);
void sdata_add_documents(struct sdata *sd, json_t *j);
void sdata_free(struct sdata *sd);
void sdata_delete(struct sdata *sd);
void sdata_clear(struct sdata *sd);
char *sdata_get_document(const struct sdata *sd, const char *id);
char *sdata_get_document_byid(const struct sdata *sd, uint32_t docid);
uint32_t sdata_delete_document(struct sdata *sd, const char *id);

#endif
