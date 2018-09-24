#include <stdio.h>
#include <stdarg.h>

// 0: debug 
// 1: info
// 2: err
int global_log_level = 0; 

void mlog_func(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
}
