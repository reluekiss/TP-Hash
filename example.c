#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#define TP_DTABLE_IMPLEMENTATION
#include "tp_dtable.h"

/* Define a struct to use as the value */
typedef struct {
    uint32_t data;
    char label[16];
} my_value_t;

int main(void) {
    /* Create a table for uint32_t keys and my_value_t values */
    dtable_t *dt = dt_create(sizeof(uint32_t), sizeof(my_value_t));
    if (!dt) {
        fprintf(stderr, "Failed to create table\n");
        return 1;
    }
    printf("Created table with active capacity: %u slots\n", dt->current_capacity);

    /* Insert some key/value pairs */
    char* keys[] = { "1", "2", "hello"};
    my_value_t values[3];
    values[0].data = 123;
    snprintf(values[0].label, sizeof(values[0].label), "val_%s", keys[0]);
    values[1].data = 456;
    snprintf(values[1].label, sizeof(values[1].label), "val_%s", keys[1]);
    values[2].data = 789;
    snprintf(values[2].label, sizeof(values[2].label), "val_%s", keys[2]);

    for (int i = 0; i < 3; i++) {
        uint32_t tp = dt_insert(dt, &keys[i], &values[i]);
        if (tp == UINT32_MAX)
            printf("Insertion failed for key %s\n", keys[i]);
        else
            printf("Inserted key %s with value {data=%u, label=%s}, tiny pointer = %u\n",
                   keys[i], values[i].data, values[i].label, tp);
    }

    /* Lookup the inserted keys */
    for (int i = 0; i < 3; i++) {
        my_value_t found;
        if (dt_lookup(dt, &keys[i], &found))
            printf("Lookup: key %s found with value {data=%u, label=%s}\n",
                   keys[i], found.data, found.label);
        else
            printf("Lookup: key %s not found\n", keys[i]);
    }

    /* Delete key "hello" */
    {
        char* key_del = "hello";
        if (dt_delete(dt, &key_del))
            printf("Deleted key %s successfully\n", key_del);
        else
            printf("Deletion failed for key %s\n", key_del);
    }

    /* Lookup key "hello" again after deletion */
    {
        char* key_check = "hello";
        my_value_t found;
        if (dt_lookup(dt, &key_check, &found))
            printf("Lookup after deletion: key %s found with value {data=%u, label=%s} (unexpected)\n",
                   key_check, found.data, found.label);
        else
            printf("Lookup after deletion: key %s not found (expected)\n", key_check);
    }

    /* Print active memory usage */
    size_t mem_usage = dt_active_memory_usage(dt);
    printf("Active memory usage: %zu bytes\n", mem_usage);

    /* Reset the table */
    dt_reset(dt);
    printf("Table reset. Active capacity is now %u slots.\n", dt->current_capacity);

    /* Verify that previous keys have been removed */
    {
        my_value_t found;
        if (dt_lookup(dt, &keys[0], &found))
            printf("After reset: key %s found with value {data=%u, label=%s} (unexpected)\n",
                   keys[0], found.data, found.label);
        else
            printf("After reset: key %s not found (expected)\n", keys[0]);
    }

    dt_destroy(dt);
    return 0;
}
