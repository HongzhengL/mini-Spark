#define _GNU_SOURCE

#include "thread_pool.h"

#include <sched.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    List* threads;

    // TODO: add some cv?
} ThreadPool;

int get_num_threads() {
    cpu_set_t set;
    CPU_ZERO(&set);

    if (sched_getaffinity(0, sizeof(set), &set) == -1) {
        perror("sched_getaffinity");
        exit(EXIT_FAILURE);
    }

    return CPU_COUNT(&set);
}

void thread_pool_init(int numthreads) {
    // TODO
}

void thread_pool_destroy() {
    // TODO
}

void thread_pool_wait() {
    // TODO
}

void thread_pool_submit(Task* task) {
    // TODO
}
