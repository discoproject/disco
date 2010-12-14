
#ifndef __DDB_PROFILE_H__
#define __DDB_PROFILE_H__

#include <time.h>
#include <stdio.h>

#ifdef DDB_PROFILE
#define DDB_TIMER_DEF clock_t __start;
#define DDB_TIMER_START __start = clock();
#define DDB_TIMER_END(msg) fprintf(stderr, "PROF: %s took %2.4fms\n", msg,\
        ((double) (clock() - __start)) / (CLOCKS_PER_SEC / 1000.0));
#else
#define DDB_TIMER_DEF
#define DDB_TIMER_START
#define DDB_TIMER_END(x)
#endif

#endif /* __DDB_PROFILE_H__ */
