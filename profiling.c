#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <time.h>
#include <sys/time.h>

#define TP_DTABLE_IMPLEMENTATION
#include "tp_dtable.h"

#define NOPS 10000000              // Number of stress-test operations.
#define MAX_INSERTED_KEYS 10000000
char *inserted_keys[MAX_INSERTED_KEYS];
size_t inserted_count = 0;

char* random_string(size_t length) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    char* str = malloc(length + 1);
    if (str) {
        for (size_t i = 0; i < length; i++) {
            str[i] = charset[rand() % (sizeof(charset) - 1)];
        }
        str[length] = '\0';
    }
    return str;
}

typedef struct {
    int num;
    char* string;
} my_type_t;

int main(void) {
    dtable_t *dt = dt_create(sizeof(char*), sizeof(my_type_t));
    if (!dt) {
        perror("dt_create failed");
        return 1;
    }
    srand((unsigned)time(NULL));
    uint64_t total_ptr_bits = 0;
    my_type_t random, lookup;
    char* lookup_key;
    char* delete_key;
    char str[16];
    
    clock_t start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        char* key = random_string(10);
        my_type_t value = { rand(), random_string(15) };
        uint32_t ret = dt_insert(dt, &key, &value);
        if (ret != UINT32_MAX) {
            inserted_keys[inserted_count++] = key;
        }
    }
    clock_t end = clock();  // End timing
    double avg_time_per_insert = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;

    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        if (inserted_count > 0) {
            lookup_key = inserted_keys[rand() % inserted_count];
        }
        if (dt_lookup(dt, lookup_key, &lookup)) {
            fprintf(stderr, "failed to lookeup key: %s", lookup_key);
        }
    }
    end = clock();  // End timing
    double avg_time_per_lookup = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        if (inserted_count > 0) {
            delete_key = inserted_keys[rand() % inserted_count];
        }
        if (dt_delete(dt, delete_key)) {
            fprintf(stderr, "failed to lookeup key: %s", delete_key);
        }
    }
    end = clock();  // End timing
    double avg_time_per_delete = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;

    // for (int i = 0; i < dt->count; i++) {
    //     dt->values[i].string;
    // }
    size_t final_usage = dt_active_memory_usage(dt);
    
    // Compute pointer overhead.
    // In this profiling the "pointer overhead" is taken as the size of the dtable_t structure.
    size_t dt_struct_size = sizeof(dtable_t);
    // Calculate proportion (in percentage) of active memory usage that is taken up by the dt structure.
    double pointer_proportion = ((double) dt_struct_size / final_usage) * 100.0;
    
    // Print summary at end.
    printf("Stress Test Completed:\n");
    printf("  Operations performed: %u\n", NOPS);
    printf("  Average insert time per op: %.6f nanoseconds\n", avg_time_per_insert);
    printf("  Average lookup time per op: %.6f nanoseconds\n", avg_time_per_lookup);
    printf("  Average delete time per op: %.6f nanoseconds\n", avg_time_per_delete);
    printf("  Final active capacity: %u slots\n", dt->capacity);
    printf("  Final active memory usage: %zu bytes\n", final_usage);
    printf("  Pointer overhead (size of dt structure): %zu bytes\n", dt_struct_size);
    printf("  Pointer overhead proportion: %.20f%% of total active memory usage\n", pointer_proportion);
    
    dt_destroy(dt);
    return 0;
}
