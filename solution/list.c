#include "list.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

List* list_init(int capacity) {
    List* l = malloc(sizeof(List));
    if (l == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    l->capacity = capacity;
    l->size = 0;
    l->data = (void**)malloc(sizeof(void*) * capacity);
    return l;
}

void __grow_capacity(List* l, int capacity) {
    void** new_data = (void**)realloc(l->data, sizeof(void*) * capacity);
    if (new_data == NULL) {
        perror("realloc");
        exit(EXIT_FAILURE);
    }
    l->data = new_data;
    l->capacity = capacity;
}

void list_add_elem(List* l, void* elem) {
    if (l->size == l->capacity) {
        __grow_capacity(l, l->capacity * 2);
    }
    l->data[l->size++] = elem;
}

void* get_elem(List* l) {
    return l->data[l->pos];
}

void* get_next_elem(List* l) {
    l->pos = (l->pos + 1) % l->size;
    return l->data[l->pos];
}

void seek_to_start(List* l) {
    l->pos = 0;
}

void free_list(List* l) {
    free((void*)l->data);
    free((void*)l);
}
