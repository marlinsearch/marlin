#include "flakeid.h"

#include <endian.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>

static const uint64_t twepoch = 1288834974657L;

struct flakeid_ctx_s {
  uint64_t time;
  struct {
    unsigned char machine[6];
    uint16_t pid;
  } worker;
  uint16_t seq;
};

struct flakeid64_ctx_s {
  uint64_t time;
  uint16_t machine;
  uint16_t seq;
};

static inline int random_with_range(int min, int max) {
  return random() % (max + 1 - min) + min;
}

static inline void spoof_mac(unsigned char *mac) {
  // randomly assign a VM vendor's mac address prefix, which should
  // decrease chance of colliding with existing device's address

  static const unsigned char vendors[][3] = {
    {0x00, 0x05, 0x69}, // VMware
    {0x00, 0x50, 0x56}, // VMware
    {0x00, 0x0C, 0x29}, // VMware
    {0x00, 0x16, 0x3E}, // Xen
    {0x00, 0x03, 0xFF}, // Microsoft Hyper-V, Virtual Server, Virtual PC
    {0x00, 0x1C, 0x42}, // Parallels
    {0x00, 0x0F, 0x4B}, // Virtual Iron 4
    {0x08, 0x00, 0x27}  // Sun Virtual Box
  };

  // seed the random
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand(tv.tv_sec * 1000 + (tv.tv_usec / 1000));

  int vendor = random_with_range(0, sizeof(vendors) / sizeof(vendors[0]));

  memcpy(mac, vendors[vendor], 3);
  mac[3] = random_with_range(0x00, 0x7F);
  mac[4] = random_with_range(0x00, 0xFF);
  mac[5] = random_with_range(0x00, 0xFF);
}

static inline unsigned spoof_machine() {
  // seed the random
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand(((tv.tv_sec * 1000 + tv.tv_usec / 1000) << 2) | (0xFFFF & getpid()));
  return random_with_range(0, 1023);
}

flakeid_ctx_t *flakeid_ctx_create(const unsigned char *machine, size_t len) {
  flakeid_ctx_t *ret = NULL;
  flakeid_ctx_t *ctx = (flakeid_ctx_t *)calloc(sizeof(flakeid_ctx_t), 1);

  if (ctx) {
    ctx->worker.pid = htobe16(0xFFFF & getpid());
    ret = ctx;
  }

  if (machine) {
    memcpy(ctx->worker.machine, machine, len > 6 ? 6 : len);
  }

  return ret;
}

flakeid_ctx_t *flakeid_ctx_create_with_if(const char *if_name) {
  unsigned char mac[6];
  flakeid_ctx_t *ret = flakeid_ctx_create(mac, 6);
  return ret;
}

flakeid_ctx_t *flakeid_ctx_create_with_spoof(unsigned char *out) {
  unsigned char mac[6];
  spoof_mac(mac);

  if (out) {
    memcpy(out, mac, 6);
  }

  return flakeid_ctx_create(mac, 6);
}

void flakeid_ctx_destroy(flakeid_ctx_t *ctx) {
  free(ctx);
}

int flakeid_updatetime(flakeid_ctx_t *ctx, struct timeval *tv) {
    uint64_t time = tv->tv_sec * 1000 + tv->tv_usec / 1000;

    if (ctx->time != time) {
        ctx->time = time;
        ctx->seq = 0;
    }

  return 0;
}

int flakeid_generate(flakeid_ctx_t *ctx, unsigned char *out) {
  uint64_t time_be = htobe64(ctx->time);
  uint16_t seq_be = htobe16(ctx->seq++);

  /* [48 bits | Timestamp, in milliseconds since the epoch] */
  /* [48 bits | a host identifier] */
  /* [16 bits | the process ID] */
  /* [16 bits | a per-process counter, reset each millisecond] */
  memcpy(out, (char *)&time_be + 2, 6);
  memcpy(out + 6, ctx->worker.machine, 6);
  memcpy(out + 12, &ctx->worker.pid, 2);
  memcpy(out + 14, &seq_be, 2);
  return 0;
}

int flakeid_get(flakeid_ctx_t *ctx, unsigned char *out) {
  int ret = -1;

  struct timeval now;
  gettimeofday(&now, NULL);
  if (!flakeid_updatetime(ctx, &now)) {
    flakeid_generate(ctx, out);
    ret = 0;
  }

  return ret;
}

void flakeid_hexdump(const unsigned char *id, char delimiter, unsigned char *out) {
  static const char *hex = "0123456789abcdef";

  if (delimiter) {
    int i = 0;
    int j = 0;

    for (; i < 6; ++i) {
      unsigned char ch = id[i];
      out[j++]         = hex[(ch >> 4) & 0X0F];
      out[j++]         = hex[ch & 0x0F];
    }

    out[j++] = delimiter;

    for (; i < 12; ++i) {
      unsigned char ch = id[i];
      out[j++]         = hex[(ch >> 4) & 0X0F];
      out[j++]         = hex[ch & 0x0F];
    }

    out[j++] = delimiter;

    for (; i < 14; ++i) {
      unsigned char ch = id[i];
      out[j++]         = hex[(ch >> 4) & 0X0F];
      out[j++]         = hex[ch & 0x0F];
    }

    out[j++] = delimiter;

    for (; i < 16; ++i) {
      unsigned char ch = id[i];
      out[j++]         = hex[(ch >> 4) & 0X0F];
      out[j++]         = hex[ch & 0x0F];
    }
  } else {
    int i = 0;
    int j = 0;

    for (; i < 16; ++i) {
      unsigned char ch = id[i];
      out[j++]         = hex[(ch >> 4) & 0X0F];
      out[j++]         = hex[ch & 0x0F];
    }
  }
}

void flakeid_extract(const unsigned char *id, uint64_t *time, unsigned char *mac, uint16_t *pid, uint16_t *seq) {
  if (time) {
    uint64_t time_be = 0;
    memcpy((char *)&time_be + 2, id, 6);
    *time = be64toh(time_be);
  }

  if (mac) {
    memcpy(mac, id + 6, 6);
  }

  if (pid) {
    *pid = be16toh(*(uint16_t *)(id + 12));
  }

  if (seq) {
    uint16_t seq_be = *(uint16_t *)(id + 14);
    *seq = be16toh(seq_be);
  }
}

flakeid64_ctx_t *flakeid64_ctx_create(unsigned int machine) {
  flakeid64_ctx_t *ret = NULL;
  flakeid64_ctx_t *ctx = (flakeid64_ctx_t *)calloc(sizeof(flakeid64_ctx_t), 1);

  if (ctx) {
    ctx->machine = machine & 0x03FF;
    ret = ctx;
  }

  return ret;
}

flakeid64_ctx_t *flakeid64_ctx_create_with_spoof(unsigned int *out) {
  flakeid64_ctx_t *ret = NULL;
  flakeid64_ctx_t *ctx = (flakeid64_ctx_t *)calloc(sizeof(flakeid64_ctx_t), 1);

  if (ctx) {
    ctx->machine = spoof_machine();

    if (out) {
      *out = ctx->machine;
    }

    ret = ctx;
  }

  return ret;
}

void flakeid64_ctx_destroy(flakeid64_ctx_t *ctx) {
  free(ctx);
}

int flakeid64_updatetime(flakeid64_ctx_t *ctx, struct timeval *tv) {
  uint64_t time = tv->tv_sec * 1000 + tv->tv_usec / 1000;

  if (ctx->time != time) {
      ctx->time = time;
      ctx->seq = 0;
  }
  return 0;
}

int flakeid64_generate(flakeid64_ctx_t *ctx, int64_t *out) {
  /* [1  bit  | signed bit, always 0] */
  /* [41 bits | Timestamp, in milliseconds since the epoch] */
  /* [10 bits | machine id] */
  /* [12 bits | a per-process counter, reset each millisecond] */
  *out = (((ctx->time - twepoch) & 0xFFFFFFFFFFFF) << 22) | (ctx->machine << 12) | (ctx->seq++ & 0x0FFF);
  return 0;
}

int flakeid64_get(flakeid64_ctx_t *ctx, int64_t *out) {
  int ret = -1;

  struct timeval now;
  gettimeofday(&now, NULL);
  if (!flakeid64_updatetime(ctx, &now)) {
    flakeid64_generate(ctx, out);
    ret = 0;
  }

  return ret;
}

void flakeid64_hexdump(int64_t id, unsigned char *out) {
  static const char *hex = "0123456789abcdef";
  int i = 8;
  int j = 0;
  unsigned char *id_buf = (unsigned char *)&id;

  while (--i >= 0) {
    unsigned char ch = id_buf[i];
    out[j++]         = hex[(ch >> 4) & 0X0F];
    out[j++]         = hex[ch & 0x0F];
  }
}

void flakeid64_extract(int64_t id, uint64_t *time, unsigned int *machine, uint16_t *seq) {
  if (time) {
    *time = (id >> 22) + twepoch;
  }

  if (machine) {
    *machine = (id >> 12) & 0x03FF;
  }

  if (seq) {
    *seq = id & 0x0FFF;
  }
}
