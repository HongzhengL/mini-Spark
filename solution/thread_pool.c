#include <pthread.h>
#include "list.h"
#include "minispark.h"
#define _GNU_SOURCE

#include "thread_pool.h"

#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

typedef struct {
  pthread_t* threads;
  int numThread;

  // TODO: add some cv?
} ThreadPool;

ThreadPool pool;
int numFull = 0;
List* taskQueue = NULL;

int get_num_threads() {
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(EXIT_FAILURE);
  }

  return CPU_COUNT(&set);
}

void pop_and_compute() {
  Task* topTask = NULL;
  bool waitDependencies = true;

  pthread_mutex_lock(&queueLock);
  topTask = (Task*)list_remove_front(taskQueue);
  pthread_mutex_unlock(&queuelock);

  int partitionIndex = topTask->pnum;
  void* computeFunction = topTask->rdd->fn;
  RDD** dependentRDD = topTask->rdd->dependencies;
  if (topTask) {
    // spin until dependencies get computed
    while (waitDependencies) {
      waitDependencies = false;
      for (int i = 0; i < topTask->rdd->numdependencies; i++) {
        if (dependentRDD[i]->numComputed < dependentRDD[i]->numpartitions) {
          waitDependencies = true;
        }
      }
    }

    List* contentList = list_init(1024);
    if (topTask->rdd->trans == MAP) {
      // TODO: How to handle root node? (numdependencies == 0)

      // There must be only one dependent RDD
      while (1) {
        void* line = computeFunction(
          get_nth_elem(dependentRDD[0]->partitions, partitionIndex)));
        if (line)
          list_add_elem(contentList, line);
        else
          break;
      }
    } else if (topTask->rdd->trans == FILTER) {
      // There must be only one dependent RDD
      void* line = NULL;
      List* oldContent =
          (List*)get_nth_elem(dependentRDD[0]->partitions, partitionIndex);
      seek_to_start(oldContent);
      while (line = next(oldContent)) {
        if (computeFunction(line, topTask->rdd->ctx)) {
          list_add_elem(contentList, line);
        }
      }
      free_list(oldContent);
    } else if (topTask->rdd->trans == JOIN) {
      // There must be two dependent RDD
      void* lineA = NULL;
      void* lineB = NULL;
      List* oldContentA =
          (List*)get_nth_elem(dependentRDD[0]->partitions, partitionIndex);
      List* oldContentB =
          (List*)get_nth_elem(dependentRDD[1]->partitions, partitionIndex);

      seek_to_start(oldContentA);
      while (lineA = next(oldContentA)) {
        seek_to_start(oldContentB);
        while (lineB = next(oldContentB)) {
        }
      }
    }
    topTask->rdd->numComputed++;
  }
}

void* consumer(void* arg) {
  while (1) {
    pthread_mutex_lock(&cvLock);
    while (numFull == 0) {
      pthread_cond_wait(&cvFill, &cvLock);
    }
    pop_and_compute();
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
  taskQueue = list_init(1024);
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
