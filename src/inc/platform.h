#ifndef PLATFORM_H
#define PLATFORM_H
#include <limits.h>

#define ATOMIC_INC(x)  __sync_fetch_and_add((x), 1)
#define ATOMIC_DEC(x)  __sync_fetch_and_sub((x), 1)

#ifdef __GNUC__
# define LIKELY(x)       __builtin_expect(!!(x), 1)
# define UNLIKELY(x)     __builtin_expect(!!(x), 0)
#else
# define LIKELY(x)       (x)
# define UNLIKELY(x)     (x)
#endif

#define INITLOCK(x)   pthread_rwlock_init((x), NULL)
#define TRY_RDLOCK(x) pthread_rwlock_tryrdlock((x))

#if 0
# define RDLOCK(x)     {printf("rdlock\n");pthread_rwlock_rdlock((x));printf("rdlock done\n");}
# define WRLOCK(x)     {printf("wrlock\n");pthread_rwlock_rdlock((x));printf("wrlock done\n");}
# define UNLOCK(x)     {pthread_rwlock_unlock((x)); printf("unlock\n");}
#else
# define RDLOCK(x)     pthread_rwlock_rdlock((x))
# define WRLOCK(x)     pthread_rwlock_wrlock((x))
# define UNLOCK(x)     pthread_rwlock_unlock((x))
#endif

#define PACKED  __attribute__((__packed__))

#endif
