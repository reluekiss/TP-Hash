#include <math.h>
#include <stdint.h>
#define TP_DT_IMPLEMENTATION
#include "tp_dtable.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define NOPS 1000000

static inline uint32_t ceil_log2(uint32_t x) {
    uint32_t bits = 0;
    uint32_t power = 1;
    while (power < x) {
        power <<= 1;
        bits++;
    }
    return bits;
}

static inline uint32_t dt_total_ptr_bits(void) {
    uint32_t num_buckets_primary = (MAX_CAPACITY + PRIMARY_BUCKET_SIZE - 1) / PRIMARY_BUCKET_SIZE;
    uint32_t primary_bucket_bits = ceil_log2(num_buckets_primary);
    uint32_t primary_slot_bits = ceil_log2(PRIMARY_BUCKET_SIZE);
    uint32_t primary_total = 1 + primary_bucket_bits + primary_slot_bits; // 1 bit for table id

    uint32_t num_buckets_secondary = (MAX_CAPACITY + SECONDARY_BUCKET_SIZE - 1) / SECONDARY_BUCKET_SIZE;
    uint32_t secondary_bucket_bits = ceil_log2(num_buckets_secondary);
    uint32_t secondary_slot_bits = ceil_log2(SECONDARY_BUCKET_SIZE);
    uint32_t secondary_total = 1 + secondary_bucket_bits + secondary_slot_bits;

    return (primary_total > secondary_total) ? primary_total : secondary_total;
}

int main(void) {
    dt_t *dt = dt_create(sizeof(uint32_t), sizeof(uint32_t));
    if (!dt) {
        perror("dt_create failed");
        return 1;
    }
    srand((unsigned)time(NULL));
    uint32_t *aux_keys = malloc(NOPS * sizeof(uint32_t));
    tiny_ptr_t *aux_ptrs = malloc(NOPS * sizeof(tiny_ptr_t));
    uint32_t aux_count = 0;
    uint64_t total_ptr_bits = 0;
    uint64_t insert_count = 0, lookup_count = 0, delete_count = 0;
    
    clock_t start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        int r = rand() % 1000;
        if (r < 500) { // Insert
            uint32_t key = rand();
            uint32_t value = rand();
            tiny_ptr_t tp = dt_insert(dt, &key, &value);
            if (tp.table_id != 0xFF) {
                aux_keys[aux_count] = key;
                aux_ptrs[aux_count] = tp;
                aux_count++;
                total_ptr_bits += dt_total_ptr_bits();
                insert_count++;
            }
        } else if (r < 800) { // Lookup
            if (aux_count > 0) {
                uint32_t idx = rand() % aux_count;
                dt_lookup(dt, &aux_keys[idx], aux_ptrs[idx], NULL);
                lookup_count++;
            }
        } else if (r < 999) { // Delete
            if (aux_count > 0) {
                uint32_t idx = rand() % aux_count;
                if (dt_delete(dt, &aux_keys[idx], aux_ptrs[idx])) {
                    aux_keys[idx] = aux_keys[aux_count - 1];
                    aux_ptrs[idx] = aux_ptrs[aux_count - 1];
                    aux_count--;
                    delete_count++;
                }
            }
        } else {
            dt_reset(dt);
        }
    }

    clock_t end = clock();  // End timing
    double elapsed_seconds = (double)(end - start) / CLOCKS_PER_SEC;
    double avg_time_per_op_us = (elapsed_seconds / NOPS) * 1e6;

    double max_capacity = (double)(1 << 20);
    double safe_log_max = (max_capacity > 2 ? log(max_capacity) : 1.0);
    double safe_log_safe_log_max = (safe_log_max > 2 ? log(safe_log_max) : 1.0);
    double delta = 1.0 / fmax(safe_log_safe_log_max, 1.0);
    double ideal_pointer_bits = log2(log2(log2(max_capacity))) + log2(1.0 / delta);

    printf("Profiling Completed:\n");
    printf("  Operations performed: %u\n", NOPS);
    printf("  Inserts: %lu\n", insert_count);
    printf("  Lookups: %lu\n", lookup_count);
    printf("  Deletes: %lu\n", delete_count);
    printf("  Total elapsed time: %.6f seconds\n", elapsed_seconds);
    printf("  Average time per op: %.6f microseconds\n", avg_time_per_op_us);
    printf("  Inserts: %lu\n", insert_count);
    printf("  Lookups: %lu\n", lookup_count);
    printf("  Deletes: %lu\n", delete_count);
    printf("  Average pointer length: %.2f bits\n", (double)total_ptr_bits / insert_count);
    printf("  (Expected ideal: ~O(log log log n + log(1/DELTA)) : %.10f bits)\n", ideal_pointer_bits);

    printf("\nPrimary Table:\n");
    printf("  Active slots: %u\n", dt->primary->current_capacity);
    printf("  Slots per bucket: %u\n", dt->primary->slots_per_bucket);
    printf("  Buckets: %u\n", dt->primary->num_buckets);
    printf("\nSecondary Table:\n");
    printf("  Active slots: %u\n", dt->secondary->current_capacity);
    printf("  Slots per bucket: %u\n", dt->secondary->slots_per_bucket);
    printf("  Buckets: %u\n", dt->secondary->num_buckets);

    dt_destroy(dt);
    free(aux_keys);
    free(aux_ptrs);
    return 0;
}
