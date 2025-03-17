#include <stdio.h>

#define TP_DTABLE_IMPLEMENTATION
#include "tp_dtable.h"

int main(void) {
    dtable_t *dt = dt_create();
    if (!dt) {
        fprintf(stderr, "Failed to create table\n");
        return 1;
    }
    printf("Created table with active capacity: %u slots\n", dt->current_capacity);

    // Insert some key/value pairs.
    uint32_t keys[]   = {42, 100, 2021};
    uint32_t values[] = {123, 456, 789};
    for (int i = 0; i < 3; i++) {
        uint32_t tp = dt_insert(dt, keys[i], values[i]);
        if (tp == UINT32_MAX)
            printf("Insertion failed for key %u\n", keys[i]);
        else
            printf("Inserted key %u with value %u, tiny pointer = %u\n", keys[i], values[i], tp);
    }

    // Lookup the inserted keys.
    for (int i = 0; i < 3; i++) {
        uint32_t found;
        if (dt_lookup(dt, keys[i], &found))
            printf("Lookup: key %u found with value %u\n", keys[i], found);
        else
            printf("Lookup: key %u not found\n", keys[i]);
    }

    // Delete key 100.
    if (dt_delete(dt, 100))
        printf("Deleted key %u successfully\n", 100);
    else
        printf("Deletion failed for key %u\n", 100);

    // Lookup key 100 again after deletion.
    uint32_t found;
    if (dt_lookup(dt, 100, &found))
        printf("Lookup after deletion: key %u found with value %u (unexpected)\n", 100, found);
    else
        printf("Lookup after deletion: key %u not found (expected)\n", 100);

    // Reset the table.
    dt_reset(dt);
    printf("Table reset. Active capacity is now %u slots.\n", dt->current_capacity);

    // Verify that previous keys have been removed.
    if (dt_lookup(dt, 42, &found))
        printf("After reset: key %u found with value %u (unexpected)\n", 42, found);
    else
        printf("After reset: key %u not found (expected)\n", 42);

    dt_destroy(dt);
    return 0;
}
