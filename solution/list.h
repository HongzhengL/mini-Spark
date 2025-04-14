/**
 * @file list.h
 * @author
 * @brief Definition of a dynamic generic list
 * @version 0.1
 * @date 2025-04-09
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __LIST_H__
#define __LIST_H__

#define LIST_GROWTH_FACTOR 2

/**
 * @brief Generic list structure.
 *
 * The List structure maintains an array of void pointers,
 * its current size (number of elements) and its capacity.
 */
typedef struct List {
  int size;     /**< Current number of elements in the list */
  int capacity; /**< Maximum number of elements before needing to grow */
  int pos;      /**< Internal iterator position used for traversal */
  int start;    /**< Index of the front element in the ring buffer. */
  void** data;  /**< Pointer to an array of generic pointers */
} List;

/**
 * @brief Initialize a new list with a given initial capacity.
 *
 * This function allocates memory for a new List structure and initializes its
 * fields.
 *
 * @param capacity The initial capacity for the list.
 * @return List* Pointer to the newly created list.
 */
List* list_init(int capacity) __attribute__((warn_unused_result));

/**
 * @brief Add an element to the list.
 *
 * Inserts a new element into the list. If the list has reached its current
 * capacity, the internal storage is automatically increase by
 * LIST_GROWTH_FACTOR.
 *
 * @param l Pointer to the list.
 * @param elem Generic pointer to the element to add.
 */
void list_add_elem(List* l, void* elem);

/**
 * @brief Remove and return the front element of the list.
 *
 * If the list is empty, returns NULL. Otherwise, removes the front
 * element (the one at `start`) and updates the ring buffer accordingly.
 *
 * @param l Pointer to the list.
 * @return The front element, or NULL if the list is empty.
 */
void* list_remove_front(List* l);

/**
 * @brief Retrieve the current element pointed to by the internal iterator.
 *
 * Returns the element at the current iterator position (`pos`), or NULL if
 * there are no more elements to return (i.e., if `pos >= size`).
 *
 * @param l Pointer to the list.
 * @return void* Pointer to the current element, or NULL if the position is
 * invalid or if list is NULL
 */
void* get_elem(List* l);

/**
 * @brief Retrieve the current element and advance the internal iterator.
 *
 * Returns the element at the current iterator position (`pos`) and then
 * advances `pos` by 1. Returns NULL if there are no more elements.
 *
 * @param l Pointer to the list.
 * @return void* Pointer to the next element, or NULL if the iterator has
 * reached the end, or list is NULL, or list has no element
 */
void* next(List* l);

/**
 * @brief Reset the internal iterator to the start of the list.
 *
 * Sets the internal iterator (`pos`) back to 0, allowing iteration
 * from the beginning of the list using `get_elem()` or `next()`.
 *
 * @param l Pointer to the list.
 */
void seek_to_start(List* l);

/**
 * @brief Free the list structure.
 *
 * Frees the memory allocated for the list's internal data array and the list
 * structure itself. Note that this function does not free any memory pointed to
 * by the individual elements, the caller is responsible for that.
 *
 * @param l Pointer to the list to free.
 */
void free_list(List* l);

void* get_nth_elem(List* l, int n) {
#endif  // !__LIST_H__
