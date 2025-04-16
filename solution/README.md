# COMP SCI 537 Project 5

## Student Information

-   name: Hongzheng Li, Junjie Yan
-   CS Login: hongzheng, jyan
-   Wisc ID: hli2225, jyan244
-   Email: <hongzheng@cs.wisc.edu>, <jyan@cs.wisc.edu>

## Implementation Status

The implementation is **complete** and all core functionalities work as expected.
All provided test cases **pass**.
No known bugs at the time of submission.

## Files Structure in `solution` Folder

-   `list.h` and `list.c`

    -   These files implement a generic dynamic list data structure that supports various operations
    -   `list.h`: Defines the List structure and function prototypes
    -   `list.c`: Implements the list operations including initialization, adding/removing elements,
        traversal, and memory management
    -   The list functions as a ring buffer to efficiently manage elements
    -   Key features: automatic growth when capacity is reached, internal iterator for traversal

-   `minispark.h` and `minispark.c`

    -   These files implement the core MiniSpark framework
    -   `minispark.h`: Defines the RDD (Resilient Distributed Dataset) structure and transformation/action operations
    -   `minispark.c`: Implements RDD operations, execution logic, and metrics collection
    -   Supports key operations:
        -   Transformations: map, filter, join, partitionBy
        -   Actions: count, print
    -   Manages the execution flow and dependencies between RDDs
    -   Handles metrics collection for performance analysis (stored in metrics.log)

-   `thread_pool.h` and `thread_pool.c`

    -   These files implement a thread pool for parallel task execution.
    -   `thread_pool.h`: Defines interface functions for thread pool management
    -   `thread_pool.c`: Implements thread creation, task queuing, and work distribution logic
    -   Key components:
        -   Thread creation and management based on available CPU cores
        -   Task queue for distributing work among threads
        -   Synchronization mechanisms using mutexes and condition variables
        -   Worker threads that execute tasks in parallel
        -   Computation logic for different transformation types (MAP, FILTER, JOIN, PARTITIONBY)
