#ifndef __THREAD_POOL_H__
#define __THREAD_POOL_H__

#include "minispark.h"

int get_num_threads(void);

void thread_pool_init(int numthreads);

void thread_pool_destroy(void);

void thread_pool_wait(void);

void thread_pool_submit(Task* task);

#endif  // !__THREAD_POOL_H__
