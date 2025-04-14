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
                if (dependentRDD[i]->numComputed <
                    dependentRDD[i]->numpartitions) {
                    waitDependencies = true;
                }
            }
        }

        List* contentList = list_init(1024);
        if (topTask->rdd->trans == MAP) {
            // TODO: How to handle root node? (numdependencies == 0)

            // There must be only one dependent RDD
            if (dependentRDD[0]->trans == FILE_BACKED) { // partition with a single FP inside
                /*
                List* oldContent = (List*)get_nth_elem(dependentRDD[0]->partitions, partitionIndex);
                seek_to_start(oldContent);
                while(line = dependentRDD[0]->fn(oldContent)) {
                    list_add_elem(contentList, computeFunction(line));
                }
                */
                while (1) {
                    void* line = computeFunction(get_nth_elem(dependentRDD[0]->partitions, partitionIndex)));
                    if (line)
                        list_add_elem(contentList, line);
                    else
                        break;
                }
            } else { // partion with finite regular elements
                void* line = NULL;
                List* oldContent = (List*)get_nth_elem(dependentRDD[0]->partitions, partitionIndex);
                seek_to_start(oldContent);
                while(line = next(oldContent)) {
                    list_add_elem(contentList, computeFunction(line));
                }
            }
        } else if (topTask->rdd->trans == FILTER) {
            // There must be only one dependent RDD
            void* line = NULL;
            List* oldContent = (List*)get_nth_elem(dependentRDD[0]->partitions,
                                                   partitionIndex);
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
            void* newLine = NULL;
            List* oldContentA = (List*)get_nth_elem(dependentRDD[0]->partitions,
                                                    partitionIndex);
            List* oldContentB = (List*)get_nth_elem(dependentRDD[1]->partitions,
                                                    partitionIndex);

            seek_to_start(oldContentA);
            while (lineA = next(oldContentA)) {
                seek_to_start(oldContentB);
                while (lineB = next(oldContentB)) {
                    newLine = computeFunction(lineA, lineB, topTask->rdd->ctx);
                    list_add_elem(contentList, newLine);
                }
            }
        } else if (topTask->rdd->trans == PARTITIONBY) {
            // There must be one dependent RDD
            for (int i = 0; i < dependentRDD[0]->numpartitions; i++) {
                for (int lineIndex = 0; lineIndex < size(get_nth_elem(dependentRDD[0]->partitions, i)); lineIndex++) {
                    int repartitionNum = computeFunction(
                        get_nth_elem(dependentRDD[0]->partitions, lineIndex),
                        topTask->rdd->numpartitions,
                        topTask->rdd->ctx
                    );
                    if (repartitionNum == partitionIndex) {
                        list_add_elem(
                            contentList,
                            get_nth_elem(dependentRDD[0]->partitions, lineIndex)
                        );
                    }
                }
            }
        }

        pthread_mutex_lock(&(topTask->rdd->partitionListLock));
        topTask->rdd->numComputed++;
        if(topTask->rdd->partitions == NULL)
            topTask->rdd->partitions = list_init(1024);
        list_add_elem(topTask->rdd->partitions, contentList);
        pthread_mutex_unlock(&(topTask->rdd->partitionListLock));
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
