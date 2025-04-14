#include "minispark.h"

#include <bits/time.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "list.h"
#include "thread_pool.h"

#define LIST_INIT_CAPACITY 10
#define CONTENT_INIT_CAPACITY 1024

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
        maxpartitions = max(maxpartitions, dep->partitions->size);
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
        // fprintf(stderr, "from minispark.c@102 FP Addr: %s.|.%p\n",
        // filenames[i], fp);
        if (fp == NULL) {
            perror("fopen");
            exit(1);
        }
        list_add_elem(rdd->partitions, fp);
        // fprintf(stderr, "from minispark.c@108 FP Addr: %s.|.%p\n",
        // filenames[i], get_nth_elem(rdd->partitions, i));
    }

    rdd->numdependencies = 0;
    rdd->trans = FILE_BACKED;
    rdd->fn = (void *)identity;
    rdd->numpartitions = numfiles;
    return rdd;
}

void execute(RDD *rdd) {
    if (rdd->numdependencies == 2) {
        List *l1 = list_init(LIST_INIT_CAPACITY);
        List *l2 = list_init(LIST_INIT_CAPACITY);
        RDD *part1 = rdd->dependencies[0];
        while (part1->numdependencies != 0) {
            list_add_elem(l1, part1);
            part1 = part1->dependencies[0];
        }
        list_add_elem(l1, part1);
        RDD *part2 = rdd->dependencies[1];
        while (part2->numdependencies != 0) {
            list_add_elem(l2, part2);
            part2 = part2->dependencies[0];
        }
        list_add_elem(l2, part2);
        List *all = list_init(l1->size + l2->size);
        while ((part1 = (RDD *)list_remove_front(l1)) != NULL ||
               (part2 = (RDD *)list_remove_front(l2)) != NULL) {
            if (part1) {
                list_add_elem(all, part1);
            }
            if (part2) {
                list_add_elem(all, part2);
            }
        }
        List *rdds = list_reverse(all);
        free_list(all);
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
    } else if (rdd->numdependencies == 1) {
        execute(rdd->dependencies[0]);
        for (int i = 0; i < rdd->numpartitions; ++i) {
            Task *task = (Task *)malloc(sizeof(Task));
            TaskMetric *metric = (TaskMetric *)malloc(sizeof(TaskMetric));
            task->rdd = rdd;
            task->pnum = i;
            task->metric = metric;
            metric->pnum = i;
            metric->rdd = rdd;
            clock_gettime(CLOCK_MONOTONIC, &metric->created);
            thread_pool_submit(task);
        }
    } else if (rdd->numdependencies == 0) {
        for (int i = 0; i < rdd->numpartitions; ++i) {
            Task *task = (Task *)malloc(sizeof(Task));
            TaskMetric *metric = (TaskMetric *)malloc(sizeof(TaskMetric));
            task->rdd = rdd;
            task->pnum = i;
            task->metric = metric;
            metric->pnum = i;
            metric->rdd = rdd;
            clock_gettime(CLOCK_MONOTONIC, &metric->created);
            thread_pool_submit(task);
        }
    }
    return;
}

void MS_Run() {
    // thread_pool_init(get_num_threads());
    thread_pool_init(1);
}

void MS_TearDown() {
    thread_pool_destroy();
}

static void free_RDD_resource(RDD *rdd) {
    List *curr = NULL;
    seek_to_start(rdd->partitions);
    while ((curr = (List *)next(rdd->partitions)) != NULL) {
        void *item = NULL;
        seek_to_start(curr);
        while ((item = next(curr)) != NULL) {
            free(item);
        }
        free_list(curr);
    }
    free_list(rdd->partitions);
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

    free_RDD_resource(rdd);

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

    free_RDD_resource(rdd);
}
