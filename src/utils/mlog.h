#ifndef M_LOG_H
#define M_LOG_H

#include <stdio.h>
#include <time.h>

extern void mlog_func(const char *fmt, ...);
extern int global_log_level;

#define IP_FMT "%u.%u.%u.%u"
#define IP_DATA(x) (ntohl(x)>>24)&0xffU, (ntohl(x)>>16)&0xffU, (ntohl(x)>>8)&0xffU, ntohl(x)&0xffU


// gclock_gettime is ~20x faster than times() and ~10x faster than gettimeofday(). 
// it is 2x slower than time() but gives us sub-second resolution... picking time() for now
// * struct timespec ts; clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
// * using __FUNCTION__ instead of __FILE__ as FILE has the complete path given how make is invoked for us
#ifdef DEBUG_BUILD
# define M_DBG(fmt, arg...)  do { if (global_log_level == 0) \
    mlog_func("[%d] "fmt" @%s():%d\n", time(NULL), ##arg, __FUNCTION__, __LINE__); \
} while(0)
#else
# define M_DBG(fmt, arg...)  ;
#endif

#define M_INFO(fmt, arg...) do { if (global_log_level < 2) {\
    char buff[20]; \
    time_t now = time(NULL); \
    strftime(buff, 20, "%m-%d-%Y %H:%M:%S", localtime(&now)); \
    mlog_func("[%s] "fmt" @%s():%d\n", buff, ##arg, __FUNCTION__, __LINE__);} \
} while(0)

#define M_ERR(fmt, arg...)  do { if (global_log_level < 3) {\
    char buff[20]; \
    time_t now = time(NULL); \
    strftime(buff, 20, "%m-%d-%Y %H:%M:%S", localtime(&now)); \
    mlog_func("[%s] *NOTE:* "fmt" @%s():%d\n", buff, ##arg, __FUNCTION__, __LINE__);} \
} while(0)
#endif

