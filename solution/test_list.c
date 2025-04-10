#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "list.h"

// Test utility functions
#define TEST_PASSED printf("PASS: %s\n", __func__)
#define TEST_FAILED                                     \
    printf("FAIL: %s (line %d)\n", __func__, __LINE__); \
    exit(1)

// Dummy data for testing
typedef struct {
    int value;
} TestData;

TestData* create_test_data(int value) {
    TestData* data = malloc(sizeof(TestData));
    if (data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    data->value = value;
    return data;
}

void test_list_init() {
    // Test list initialization
    List* list = list_init(5);

    assert(list != NULL);
    assert(list->size == 0);
    assert(list->capacity == 5);
    assert(list->start == 0);
    assert(list->data != NULL);

    free_list(list);
    TEST_PASSED;
}

void test_list_add_elem() {
    List* list = list_init(3);
    TestData* d1 = create_test_data(10);
    TestData* d2 = create_test_data(20);

    list_add_elem(list, d1);
    assert(list->size == 1);
    assert(((TestData*)list->data[0])->value == 10);

    list_add_elem(list, d2);
    assert(list->size == 2);
    assert(((TestData*)list->data[1])->value == 20);

    free(d1);
    free(d2);
    free_list(list);
    TEST_PASSED;
}

void test_list_remove_front() {
    List* list = list_init(3);
    TestData* d1 = create_test_data(10);
    TestData* d2 = create_test_data(20);

    list_add_elem(list, d1);
    list_add_elem(list, d2);

    TestData* removed = (TestData*)list_remove_front(list);
    assert(list->size == 1);
    assert(removed->value == 10);
    assert(list->start == 1);

    removed = (TestData*)list_remove_front(list);
    assert(list->size == 0);
    assert(removed->value == 20);
    assert(list->start == 2);

    // Test removing from empty list
    removed = (TestData*)list_remove_front(list);
    assert(removed == NULL);

    free(d1);
    free(d2);
    free_list(list);
    TEST_PASSED;
}

void test_iteration() {
    List* list = list_init(3);
    TestData* d1 = create_test_data(10);
    TestData* d2 = create_test_data(20);
    TestData* d3 = create_test_data(30);

    list_add_elem(list, d1);
    list_add_elem(list, d2);
    list_add_elem(list, d3);

    // Test get_elem
    seek_to_start(list);
    assert(list->size == 3);
    assert(list->start == 0);
    assert(list->pos == 0);
    TestData* current = (TestData*)get_elem(list);
    assert(current->value == 10);

    // Test next
    current = (TestData*)next(list);
    assert(current->value == 10);
    current = (TestData*)next(list);
    assert(current->value == 20);
    current = (TestData*)next(list);
    assert(current->value == 30);
    current = (TestData*)next(list);
    assert(current == NULL);  // Past the end

    // Test seek_to_start
    seek_to_start(list);
    current = (TestData*)next(list);
    assert(current->value == 10);

    free(d1);
    free(d2);
    free(d3);
    free_list(list);
    TEST_PASSED;
}

void test_capacity_growth() {
    List* list = list_init(2);
    TestData* d1 = create_test_data(10);
    TestData* d2 = create_test_data(20);
    TestData* d3 = create_test_data(30);

    list_add_elem(list, d1);
    list_add_elem(list, d2);

    // This should trigger growth
    list_add_elem(list, d3);

    assert(list->capacity == 4);  // Should double the capacity
    assert(list->size == 3);

    // Verify that all elements are still accessible
    seek_to_start(list);
    TestData* current = (TestData*)next(list);
    assert(current->value == 10);
    current = (TestData*)next(list);
    assert(current->value == 20);
    current = (TestData*)next(list);
    assert(current->value == 30);

    free(d1);
    free(d2);
    free(d3);
    free_list(list);
    TEST_PASSED;
}

void test_circular_buffer() {
    List* list = list_init(3);
    TestData* d1 = create_test_data(10);
    TestData* d2 = create_test_data(20);
    TestData* d3 = create_test_data(30);
    TestData* d4 = create_test_data(40);

    // Fill the list
    list_add_elem(list, d1);
    list_add_elem(list, d2);
    list_add_elem(list, d3);

    // Remove from front and add to back to test circular behavior
    TestData* removed = (TestData*)list_remove_front(list);
    assert(removed->value == 10);

    list_add_elem(list, d4);

    // Check the order of elements
    seek_to_start(list);
    TestData* current = (TestData*)next(list);
    assert(current->value == 20);
    current = (TestData*)next(list);
    assert(current->value == 30);
    current = (TestData*)next(list);
    assert(current->value == 40);

    free(d1);
    free(d2);
    free(d3);
    free(d4);
    free_list(list);
    TEST_PASSED;
}

void test_empty_list_operations() {
    List* list = list_init(3);

    // Test operations on empty list
    assert(list_remove_front(list) == NULL);

    seek_to_start(list);
    assert(get_elem(list) == NULL);
    assert(next(list) == NULL);

    free_list(list);
    TEST_PASSED;
}

void test_complex_scenario() {
    List* list = list_init(3);
    TestData* d[10];

    // Create test data
    for (int i = 0; i < 10; i++) {
        d[i] = create_test_data(i * 10);
    }

    // Add elements to trigger multiple growths
    for (int i = 0; i < 8; i++) {
        list_add_elem(list, d[i]);
    }

    // Remove a few from the front
    for (int i = 0; i < 3; i++) {
        TestData* removed = (TestData*)list_remove_front(list);
        assert(removed->value == i * 10);
    }

    // Add more elements
    list_add_elem(list, d[8]);
    list_add_elem(list, d[9]);

    // Verify all elements in order
    seek_to_start(list);
    for (int i = 3; i < 10; i++) {
        TestData* current = (TestData*)next(list);
        assert(current->value == i * 10);
    }

    // Clean up
    for (int i = 0; i < 10; i++) {
        free(d[i]);
    }
    free_list(list);
    TEST_PASSED;
}

int main() {
    printf("Running list unit tests...\n");

    test_list_init();
    test_list_add_elem();
    test_list_remove_front();
    test_iteration();
    test_capacity_growth();
    test_circular_buffer();
    test_empty_list_operations();
    test_complex_scenario();

    printf("All tests passed!\n");
    return 0;
}
