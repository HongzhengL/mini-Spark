#include "list.h"

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
  l->start = 0;
  l->pos = 0;
  l->data = (void**)malloc(sizeof(void*) * capacity);
  return l;
}

void __grow_capacity(List* l, int capacity) {
  void** new_data = (void**)malloc(sizeof(void*) * capacity);
  if (new_data == NULL) {
    perror("malloc");
    exit(EXIT_FAILURE);
  }
  seek_to_start(l);
  void* data = NULL;
  int idx = 0;
  while ((data = next(l)) != NULL) {
    new_data[idx++] = data;
  }
  free((void*)l->data);
  l->data = new_data;
  l->capacity = capacity;
  l->start = 0;
  l->pos = 0;
}

void list_add_elem(List* l, void* elem) {
  if (!l) {
    return;
  }
  if (l->size == l->capacity) {
    __grow_capacity(l, l->capacity * LIST_GROWTH_FACTOR);
  }
  l->data[(l->start + l->size) % l->capacity] = elem;
  ++l->size;
}

void* list_remove_front(List* l) {
  if (!l || l->size == 0) {
    return NULL;
  }
  void* ret = l->data[l->start];
  l->data[l->start] = NULL;
  --l->size;
  l->start = (l->start + 1) % l->capacity;

  return ret;
}

void* get_elem(List* l) {
  if (!l || l->size == 0) {
    return NULL;
  }
  if (l->pos >= l->size) {
    return NULL;
  }
  return l->data[(l->start + l->pos) % l->capacity];
}

void* next(List* l) {
  if (!l || l->size == 0) {
    return NULL;
  }
  if (l->pos >= l->size) {
    return NULL;
  }
  void* elem = l->data[(l->start + l->pos) % l->capacity];
  ++l->pos;
  return elem;
}

void seek_to_start(List* l) {
  if (l) {
    l->pos = 0;
  }
}

void free_list(List* l) {
  if (l->data) {
    free((void*)l->data);
  }
  if (l) {
    free((void*)l);
  }
}

void* get_nth_elem(List* l, int n) {
  return l->data[(l->start + n) % l->capacity];
}
