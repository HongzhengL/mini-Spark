#define _GNU_SOURCE

#include "thread_pool.h"

#include <sched.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
  pthread_t* threads;
  int numThread;

  // TODO: add some cv?
} ThreadPool;

ThreadPool pool;

int get_num_threads() {
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(EXIT_FAILURE);
  }

  return CPU_COUNT(&set);
}

void* consumer(void* arg) {
  while (1) {
    pthread_mutex_lock(&cvLock);
    while (numFull == 0) {
      pthread_cond_wait(&cvFill, &cvLock);
    }
    int temp = pop_and_compute();
    pthread_cond_signal(&cvEmpty);
    pthread_mutex_unlock(&cvLock);
  }
}

void thread_pool_init(int numthreads) {
  pool.threads = (pthread_t*)malloc(sizeof(pthread_t) * numthreads);
  for (int i = 0; i < numthreads; i++) {
    if (pthread_create(&(pool.threads[i]), NULL, consumer, NULL) != 0) {
      thread_pool_destroy();
      return;
    }
  }
  pool.numThread = numthreads;
}

void thread_pool_destroy() {
  for (int i = 0; i < pool.numThread; i++) {
  }
}

void thread_pool_wait() {
  // TODO
}

void thread_pool_submit(Task* task) {
  // TODO
}
