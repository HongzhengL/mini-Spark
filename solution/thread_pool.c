#define _GNU_SOURCE
#include "thread_pool.h"

#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "list.h"
#include "minispark.h"

#define QUEUE_CAPACITY 1024

typedef struct {
    pthread_t *threads;
    int num_thread;
    List *task_queue;
    int num_task;

    pthread_mutex_t queue_lock;
    pthread_cond_t queue_not_empty;
    pthread_cond_t queue_not_full;
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

static void do_computation(Task *topTask) {
    bool waitDependencies = true;

    int partitionIndex = topTask->pnum;
    void *computeFunction = topTask->rdd->fn;
    RDD **dependentRDD = topTask->rdd->dependencies;
    if (topTask) {
        // spin until dependencies get computed
        while (waitDependencies) {
            waitDependencies = false;
            for (int i = 0; i < topTask->rdd->numdependencies; i++) {
                pthread_mutex_lock(&(dependentRDD[i]->partitionListLock));
                int computed = dependentRDD[i]->numComputed;
                int total = dependentRDD[i]->numpartitions;
                pthread_mutex_unlock(&(dependentRDD[i]->partitionListLock));

                if (computed < total) {
                    waitDependencies = true;
                }
            }
        }

        // All new results of the partitions[partitionIndex] are stored in this
        // contentList
        List *contentList = list_init(QUEUE_CAPACITY);

        if (topTask->rdd->trans == MAP) {
            // There must be only one dependent RDD
            if (dependentRDD[0]->trans ==
                FILE_BACKED) {  // partition with a single FilePointer inside
                while (1) {
                    void *line = ((Mapper)(computeFunction))(get_nth_elem(
                        dependentRDD[0]->partitions, partitionIndex));
                    if (line)
                        list_add_elem(contentList, line);
                    else
                        break;
                }
            } else {  // partion with finite regular elements
                void *line = NULL;
                List *oldContent = (List *)get_nth_elem(
                    dependentRDD[0]->partitions, partitionIndex);
                seek_to_start(oldContent);
                while ((line = next(oldContent))) {
                    list_add_elem(contentList,
                                  ((Mapper)(computeFunction))(line));
                }
            }
        } else if (topTask->rdd->trans == FILTER) {
            // There must be only one dependent RDD
            void *line = NULL;
            List *oldContent = (List *)get_nth_elem(dependentRDD[0]->partitions,
                                                    partitionIndex);
            seek_to_start(oldContent);
            while ((line = next(oldContent))) {
                if (((Filter)(computeFunction))(line, topTask->rdd->ctx)) {
                    list_add_elem(contentList, line);
                }
            }
        } else if (topTask->rdd->trans == JOIN) {
            // There must be two dependent RDDs
            void *lineA = NULL;
            void *lineB = NULL;
            void *newLine = NULL;
            List *oldContentA = (List *)get_nth_elem(
                dependentRDD[0]->partitions, partitionIndex);
            List *oldContentB = (List *)get_nth_elem(
                dependentRDD[1]->partitions, partitionIndex);

            seek_to_start(oldContentA);
            while ((lineA = next(oldContentA))) {
                seek_to_start(oldContentB);
                while ((lineB = next(oldContentB))) {
                    newLine = ((Joiner)(computeFunction))(lineA, lineB,
                                                          topTask->rdd->ctx);
                    if (newLine) {
                        list_add_elem(contentList, newLine);
                    }
                }
            }
        } else if (topTask->rdd->trans == PARTITIONBY) {
            // There must be one dependent RDD
            for (int i = 0; i < dependentRDD[0]->numpartitions; i++) {
                assert(dependentRDD[0]->partitions != NULL);
                for (int lineIndex = 0;
                     lineIndex <
                     get_size(get_nth_elem(dependentRDD[0]->partitions, i));
                     lineIndex++) {
                    int repartitionNum = ((Partitioner)(computeFunction))(
                        get_nth_elem(
                            get_nth_elem(dependentRDD[0]->partitions, i),
                            lineIndex),
                        topTask->rdd->numpartitions, topTask->rdd->ctx);
                    if (repartitionNum == partitionIndex) {
                        list_add_elem(
                            contentList,
                            get_nth_elem(
                                get_nth_elem(dependentRDD[0]->partitions, i),
                                lineIndex));
                    }
                }
            }
        }

        // update the result partitions for this RDD
        pthread_mutex_lock(&(topTask->rdd->partitionListLock));
        if (topTask->rdd->partitions == NULL)
            topTask->rdd->partitions = list_init(topTask->rdd->numpartitions);
        if (get_size(contentList)) {  // To handle root node (FILE_BACKED)
            list_insert_at(topTask->rdd->partitions, contentList,
                           partitionIndex);
        }
        topTask->rdd->numComputed++;
        pthread_mutex_unlock(&(topTask->rdd->partitionListLock));
    }
}

void *consumer(void *arg) {
    while (1) {
        pthread_mutex_lock(&pool.queue_lock);
        while (pool.num_task == 0) {
            pthread_cond_wait(&pool.queue_not_empty, &pool.queue_lock);
        }
        Task *task = (Task *)list_remove_front(pool.task_queue);
        --pool.num_task;
        pthread_cond_signal(&pool.queue_not_full);
        pthread_mutex_unlock(&pool.queue_lock);
        if (task->rdd == NULL) {
            break;
        }
        do_computation(task);
    }
    return NULL;
}

void thread_pool_init(int numthreads) {
    pool.threads = (pthread_t *)malloc(sizeof(pthread_t) * numthreads);
    pool.num_thread = numthreads;
    pool.task_queue = list_init(QUEUE_CAPACITY);
    pool.num_task = 0;
    pthread_mutex_init(&pool.queue_lock, NULL);
    pthread_cond_init(&pool.queue_not_empty, NULL);
    pthread_cond_init(&pool.queue_not_full, NULL);
    for (int i = 0; i < numthreads; i++) {
        if (pthread_create(&(pool.threads[i]), NULL, consumer, NULL) != 0) {
            perror("pthread_create");
            thread_pool_destroy();
            exit(EXIT_FAILURE);
        }
    }
}

void thread_pool_destroy() {
    free(pool.threads);
    pthread_mutex_destroy(&pool.queue_lock);
    pthread_cond_destroy(&pool.queue_not_empty);
    pthread_cond_destroy(&pool.queue_not_full);
}

void thread_pool_wait() {
    for (int i = 0; i < pool.num_thread; ++i) {
        Task *task = (Task *)malloc(sizeof(Task));
        task->rdd = NULL;
        thread_pool_submit(task);
    }
    for (int i = 0; i < pool.num_thread; ++i) {
        pthread_join(pool.threads[i], NULL);
    }
}

void thread_pool_submit(Task *task) {
    if (!task) {
        return;
    }
    pthread_mutex_lock(&pool.queue_lock);
    while (pool.num_task == QUEUE_CAPACITY) {
        pthread_cond_wait(&pool.queue_not_full, &pool.queue_lock);
    }
    list_add_elem(pool.task_queue, task);
    ++pool.num_task;
    pthread_cond_signal(&pool.queue_not_empty);
    pthread_mutex_unlock(&pool.queue_lock);
}
