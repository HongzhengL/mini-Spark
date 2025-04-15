#include "minispark.h"

#include <bits/time.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "list.h"
#include "thread_pool.h"

#define LIST_INIT_CAPACITY 10
#define CONTENT_INIT_CAPACITY 1024

const char *LOG_FILE = "metrics.log";

MetricQueue *metric_queue;

pthread_t metric_thread;

// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile.
void print_formatted_metric(TaskMetric *metric, FILE *fp) {
    fprintf(fp,
            "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled "
            "%10jd.%06ld, execution (usec) %ld\n",
            metric->rdd, metric->pnum, metric->rdd->trans,
            metric->created.tv_sec, metric->created.tv_nsec / 1000,
            metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
            metric->duration);
}

int max(int a, int b) {
    return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...) {
    RDD *rdd = malloc(sizeof(RDD));
    if (rdd == NULL) {
        printf("error mallocing new rdd\n");
        exit(1);
    }

    va_list args;
    va_start(args, fn);

    int maxpartitions = 0;
    for (int i = 0; i < numdeps; i++) {
        RDD *dep = va_arg(args, RDD *);
        rdd->dependencies[i] = dep;
        if (dep) maxpartitions = max(maxpartitions, dep->numpartitions);
    }
    va_end(args);

    rdd->numdependencies = numdeps;
    rdd->trans = t;
    rdd->fn = fn;
    rdd->partitions = NULL;
    rdd->numpartitions = maxpartitions;
    rdd->numComputed = 0;
    pthread_mutex_init(&(rdd->partitionListLock), NULL);
    return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn) {
    // fprintf(stderr, "from minispark.c@66: %p\n",
    // get_nth_elem(dep->partitions, 0));
    return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx) {
    RDD *rdd = create_rdd(1, FILTER, fn, dep);
    rdd->ctx = ctx;
    return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx) {
    RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
    rdd->partitions = list_init(numpartitions);
    rdd->numpartitions = numpartitions;
    rdd->ctx = ctx;
    return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx) {
    RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
    rdd->ctx = ctx;
    return rdd;
}

/* A special mapper */
void *identity(void *arg) {
    return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles) {
    RDD *rdd = malloc(sizeof(RDD));
    rdd->partitions = list_init(numfiles);

    for (int i = 0; i < numfiles; i++) {
        FILE *fp = fopen(filenames[i], "r");
        if (fp == NULL) {
            perror("fopen");
            exit(1);
        }
        list_add_elem(rdd->partitions, fp);
    }

    rdd->numdependencies = 0;
    rdd->trans = FILE_BACKED;
    rdd->fn = (void *)identity;
    rdd->numpartitions = numfiles;
    rdd->numComputed = 0;
    pthread_mutex_init(&(rdd->partitionListLock), NULL);
    return rdd;
}

static List *get_tasks_list(RDD *rdd) {
    List *queue = list_init(LIST_INIT_CAPACITY);
    List *rdd_list = list_init(LIST_INIT_CAPACITY);

    list_add_elem(queue, rdd);
    list_add_elem(rdd_list, rdd);
    while (get_size(queue) != 0) {
        RDD *curr = list_remove_front(queue);
        for (int i = 0; i < curr->numdependencies; ++i) {
            list_add_elem(queue, curr->dependencies[i]);
            list_add_elem(rdd_list, curr->dependencies[i]);
        }
    }

    List *rev = list_reverse(rdd_list);
    free_list(rdd_list);

    return rev;
}

void execute(RDD *rdd) {
    List *rdds = get_tasks_list(rdd);
    seek_to_start(rdds);
    RDD *rdd_ptr = NULL;
    while ((rdd_ptr = next(rdds)) != NULL) {
        for (int i = 0; i < rdd_ptr->numpartitions; ++i) {
            Task *task = (Task *)malloc(sizeof(Task));
            TaskMetric *metric = (TaskMetric *)malloc(sizeof(TaskMetric));
            task->rdd = rdd_ptr;
            task->pnum = i;
            task->metric = metric;
            metric->pnum = i;
            metric->rdd = rdd_ptr;
            clock_gettime(CLOCK_MONOTONIC, &metric->created);
            thread_pool_submit(task);
        }
    }
}

void *metric_thread_func(void *arg) {
    FILE *fp = (FILE *)arg;
    while (1) {
        pthread_mutex_lock(&metric_queue->queue_lock);
        while (metric_queue->queue->size == 0) {
            pthread_cond_wait(&metric_queue->queue_not_empty,
                              &metric_queue->queue_lock);
        }
        TaskMetric *metric =
            (TaskMetric *)list_remove_front(metric_queue->queue);
        pthread_cond_signal(&metric_queue->queue_not_full);
        pthread_mutex_unlock(&metric_queue->queue_lock);
        if (metric->rdd == NULL) {
            fclose(fp);
            // free(metric);
            break;
        } else {
            print_formatted_metric(metric, fp);
            // free(metric);
        }
    }
    return NULL;
}

void MS_Run() {
    thread_pool_init(get_num_threads());
    metric_queue = (MetricQueue *)malloc(sizeof(MetricQueue));
    metric_queue->queue = list_init(METRIC_QUEUE_CAPACITY);
    pthread_mutex_init(&metric_queue->queue_lock, NULL);
    pthread_cond_init(&metric_queue->queue_not_empty, NULL);
    pthread_cond_init(&metric_queue->queue_not_full, NULL);

    FILE *fp = fopen(LOG_FILE, "w");
    if (fp == NULL) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }
    if (pthread_create(&metric_thread, NULL, metric_thread_func, fp) != 0) {
        perror("pthread_create");
        fclose(fp);
        exit(EXIT_FAILURE);
    }
}

void MS_TearDown() {
    thread_pool_destroy();
    TaskMetric *metric = (TaskMetric *)malloc(sizeof(TaskMetric));
    metric->rdd = NULL;
    pthread_mutex_lock(&metric_queue->queue_lock);
    while (metric_queue->queue->size == METRIC_QUEUE_CAPACITY) {
        pthread_cond_wait(&metric_queue->queue_not_full,
                          &metric_queue->queue_lock);
    }
    list_add_elem(metric_queue->queue, metric);
    pthread_cond_signal(&metric_queue->queue_not_empty);
    pthread_mutex_unlock(&metric_queue->queue_lock);
    pthread_join(metric_thread, NULL);
}

static void free_RDD_resource(RDD *rdd) {
    List *curr = NULL;
    seek_to_start(rdd->partitions);
    while ((curr = (List *)next(rdd->partitions)) != NULL) {
        void *item = NULL;
        seek_to_start(curr);
        while ((item = next(curr)) != NULL) {
            // free(item);
        }
        free_list(curr);
    }
    free_list(rdd->partitions);
}

static void free_All_RDD(RDD *rdd) {
    List *rdds = get_tasks_list(rdd);
    seek_to_start(rdds);
    RDD *rdd_ptr = NULL;
    while ((rdd_ptr = next(rdds)) != NULL) {
        if (rdd_ptr->numdependencies != 0) {
            free_RDD_resource(rdd_ptr);
        }
        free(rdd_ptr);
    }
    free_list(rdds);
}

int count(RDD *rdd) {
    execute(rdd);
    thread_pool_wait();

    int count = 0;
    List *curr = NULL;
    seek_to_start(rdd->partitions);
    while ((curr = (List *)next(rdd->partitions)) != NULL) {
        count += curr->size;
    }

    // free_All_RDD(rdd);

    return count;
}

void print(RDD *rdd, Printer p) {
    // fprintf(stderr, "from minispark.c@240: %p\n",
    // get_nth_elem(rdd->dependencies[0]->partitions, 0));
    execute(rdd);
    // fprintf(stderr, "from minispark.c@242: %p\n",
    // get_nth_elem(rdd->dependencies[0]->partitions, 0));
    thread_pool_wait();

    // print all the items in rdd
    // aka... `p(item)` for all items in rdd
    List *curr = NULL;
    seek_to_start(rdd->partitions);
    while ((curr = (List *)next(rdd->partitions)) != NULL) {
        void *item = NULL;
        seek_to_start(curr);
        while ((item = next(curr)) != NULL) {
            p(item);
        }
    }

    // free_All_RDD(rdd);
}
