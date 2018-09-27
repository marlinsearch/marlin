#ifndef __FLAKEID_H__
#define __FLAKEID_H__

#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>

typedef struct flakeid_ctx_s flakeid_ctx_t;
typedef struct flakeid64_ctx_s flakeid64_ctx_t;

flakeid_ctx_t *flakeid_ctx_create(const unsigned char *machine, size_t len);
flakeid_ctx_t *flakeid_ctx_create_with_if(const char *if_name);
flakeid_ctx_t *flakeid_ctx_create_with_spoof(unsigned char *out);
void flakeid_ctx_destroy(flakeid_ctx_t *ctx);

int flakeid_updatetime(flakeid_ctx_t *ctx, struct timeval *tv);
int flakeid_generate(flakeid_ctx_t *ctx, unsigned char *out);
int flakeid_get(flakeid_ctx_t *ctx, unsigned char *out);
void flakeid_hexdump(const unsigned char *id, char delimiter, unsigned char *out);
void flakeid_extract(const unsigned char *id, uint64_t *time, unsigned char *mac, uint16_t *pid, uint16_t *seq);

flakeid64_ctx_t *flakeid64_ctx_create(unsigned int machine);
flakeid64_ctx_t *flakeid64_ctx_create_with_spoof(unsigned int *out);
void flakeid64_ctx_destroy(flakeid64_ctx_t *ctx);

int flakeid64_updatetime(flakeid64_ctx_t *ctx, struct timeval *tv);
int flakeid64_generate(flakeid64_ctx_t *ctx, int64_t *out);
int flakeid64_get(flakeid64_ctx_t *ctx, int64_t *out);
void flakeid64_hexdump(int64_t id, unsigned char *out);
void flakeid64_extract(int64_t id, uint64_t *time, unsigned int *machine, uint16_t *seq);

#endif //!__FLAKEID_H__
