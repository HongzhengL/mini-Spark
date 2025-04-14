#define _GNU_SOURCE
#include "thread_pool.h"

#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "list.h"
#include "minispark.h"

#define QUEUE_CAPACITY 1024

typedef struct {
    pthread_t* threads;
    int num_thread;
    List* task_queue;
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

void do_computation(Task* task) {
    // TODO
}

void* consumer(void* arg) {
    while (1) {
        pthread_mutex_lock(&pool.queue_lock);
        while (pool.num_task == 0) {
            pthread_cond_wait(&pool.queue_not_empty, &pool.queue_lock);
        }
        Task* task = (Task*)list_remove_front(pool.task_queue);
        --pool.num_task;
        pthread_cond_signal(&pool.queue_not_full);
        pthread_mutex_unlock(&pool.queue_lock);
        do_computation(task);
    }
}

void thread_pool_init(int numthreads) {
    pool.threads = (pthread_t*)malloc(sizeof(pthread_t) * numthreads);
    pool.num_thread = 0;
    for (int i = 0; i < numthreads; i++) {
        if (pthread_create(&(pool.threads[i]), NULL, consumer, NULL) != 0) {
            perror("pthread_create");
            thread_pool_destroy();
            exit(EXIT_FAILURE);
        }
        ++pool.num_thread;
    }
    pool.task_queue = list_init(QUEUE_CAPACITY);
    pool.num_task = 0;
    pthread_mutex_init(&pool.queue_lock, NULL);
    pthread_cond_init(&pool.queue_not_empty, NULL);
    pthread_cond_init(&pool.queue_not_full, NULL);
}

void thread_pool_destroy() {
    for (int i = 0; i < pool.num_thread; i++) {
        pthread_join(pool.threads[i], NULL);
    }
    free(pool.threads);
    pthread_mutex_destroy(&pool.queue_lock);
    pthread_cond_destroy(&pool.queue_not_empty);
    pthread_cond_destroy(&pool.queue_not_full);
}

void thread_pool_wait() {
    // TODO
}

void thread_pool_submit(Task* task) {
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
