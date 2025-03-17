#include <assert.h>
#include <sys/resource.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>

#define TP_DTABLE_IMPLEMENTATION
#include "tp_dtable.h"

#define NOPS 1000000              // Number of stress-test operations.

int main(void) {
    dtable_t *dt = dt_create(sizeof(uint32_t), sizeof(uint32_t));
    if (!dt) {
        perror("dt_create failed");
        return 1;
    }

    srand((unsigned)time(NULL));
    struct timeval start, end;
    gettimeofday(&start, NULL);
    
    // Variables to track memory usage over time.
    uint64_t mem_usage_sum = 0;
    uint32_t mem_usage_count = 0;
    uint32_t mem_usage_min = UINT32_MAX;
    uint32_t mem_usage_max = 0;
    
    // Variables to track effective overhead per item (in bits).
    // We compare our active memory usage with a standard raw key/value array.
    uint64_t overhead_sum = 0;
    uint32_t overhead_samples = 0;
    uint32_t overhead_min = UINT32_MAX;
    uint32_t overhead_max = 0;
    
    // Auxiliary array to store keys for deletion/lookup.
    uint32_t *aux = malloc(NOPS * sizeof(uint32_t));
    uint32_t aux_count = 0;
    
    for (uint32_t op = 0; op < NOPS; op++) {
        // Sample current memory usage.
        size_t current_usage = dt_active_memory_usage(dt);
        mem_usage_sum += current_usage;
        mem_usage_count++;
        if (current_usage < mem_usage_min)
            mem_usage_min = current_usage;
        if (current_usage > mem_usage_max)
            mem_usage_max = current_usage;
        
        // If there are stored items, compute effective overhead.
        // Raw usage for a standard array storing key and value per item:
        //   raw = count * (sizeof(uint32_t) * 2)
        if (dt->count > 0) {
            size_t raw_usage = dt->count * (2 * sizeof(uint32_t));
            int extra_bytes = (current_usage > raw_usage) ? (current_usage - raw_usage) : 0;
            uint32_t overhead_bits = extra_bytes * 8;
            overhead_sum += overhead_bits;
            overhead_samples++;
            if (overhead_bits < overhead_min)
                overhead_min = overhead_bits;
            if (overhead_bits > overhead_max)
                overhead_max = overhead_bits;
        }
        
        int r = rand() % 100;
        if (r < 50) { 
            // 50% insertion.
            uint32_t key = rand();
            uint32_t value = rand();
            uint32_t tp = dt_insert(dt, &key, &value);
            if (tp != UINT32_MAX)
                aux[aux_count++] = key;
        } else if (r < 80) {
            // 30% lookup.
            if (aux_count > 0) {
                uint32_t idx = rand() % aux_count;
                uint32_t key = aux[idx];
                uint32_t found;
                (void) dt_lookup(dt, &key, &found);
            }
        } else {
            // 20% deletion.
            if (aux_count > 0) {
                uint32_t idx = rand() % aux_count;
                uint32_t key = aux[idx];
                if (dt_delete(dt, &key)) {
                    aux[idx] = aux[aux_count - 1];
                    aux_count--;
                }
            }
        }
    }
    gettimeofday(&end, NULL);
    double elapsed = (end.tv_sec - start.tv_sec) +
                     (end.tv_usec - start.tv_usec) / 1e6;
    size_t final_usage = dt_active_memory_usage(dt);
    
    // Compute pointer overhead.
    // In this profiling the "pointer overhead" is taken as the size of the dtable_t structure.
    size_t dt_struct_size = sizeof(dtable_t);
    // Calculate proportion (in percentage) of active memory usage that is taken up by the dt structure.
    double pointer_proportion = ((double) dt_struct_size / final_usage) * 100.0;
    
    // Print summary at end.
    printf("Stress Test Completed:\n");
    printf("  Operations performed: %u\n", NOPS);
    printf("  Elapsed time: %.4f seconds\n", elapsed);
    printf("  Average time per op: %.6f microseconds\n", elapsed * 1e6 / NOPS);
    printf("  Final active capacity: %u slots\n", dt->current_capacity);
    printf("  Final active memory usage: %zu bytes\n", final_usage);
    printf("  Memory usage (active region): %u bytes (max sampled)\n", mem_usage_max);
    printf("  Effective overhead per item (over raw key/value data): %u bits/item\n", overhead_max);
    printf("  Pointer overhead (size of dt structure): %zu bytes\n", dt_struct_size);
    printf("  Pointer overhead proportion: %.20f%% of total active memory usage\n", pointer_proportion);
    
    free(aux);
    dt_destroy(dt);
    return 0;
}
