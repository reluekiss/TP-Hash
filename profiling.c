#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#define HT_IMPLEMENTATION
#include "ht.h"

#define TP_DTABLE_IMPLEMENTATION
#include "tp_dtable.h"

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#define NOPS 10000000
#define MAX_INSERTED_KEYS 10000000

char *inserted_keys_dt[MAX_INSERTED_KEYS];
char *inserted_keys_stb[MAX_INSERTED_KEYS];
char *inserted_keys_generic[MAX_INSERTED_KEYS];
size_t inserted_count_dt = 0, inserted_count_stb = 0, inserted_count_generic = 0;

char* random_string(size_t length) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    char* str = malloc(length + 1);
    if (str) {
        for (size_t i = 0; i < length; i++)
            str[i] = charset[rand() % (sizeof(charset) - 1)];
        str[length] = '\0';
    } return str;
}

typedef struct {
    float num;
    int num2;
    char *string;
} my_type_t;

typedef struct {
    char *key;
    my_type_t value;
} stb_entry_t;

int main(void) {
    srand((unsigned)time(NULL));
    clock_t start, end;
    double avg_time_insert, avg_time_lookup, avg_time_delete;
    my_type_t lookup_val;
    char *lookup_key, *delete_key;

    // --- DTable Profiling ---
    dtable_t *dt = dt_create(sizeof(char*), sizeof(my_type_t));
    if (!dt) { perror("dt_create failed"); return 1; }
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        char *key = random_string(10);
        my_type_t value = { rand(), random_string(15) };
        if (dt_insert(dt, &key, &value) != UINT32_MAX)
            inserted_keys_dt[inserted_count_dt++] = key;
    }
    end = clock();
    avg_time_insert = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        if (inserted_count_dt)
            lookup_key = inserted_keys_dt[rand() % inserted_count_dt];
        if (dt_lookup(dt, lookup_key, &lookup_val))
            fprintf(stderr, "failed to lookup key: %s\n", lookup_key);
    }
    end = clock();
    avg_time_lookup = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        if (inserted_count_dt)
            delete_key = inserted_keys_dt[rand() % inserted_count_dt];
        if (dt_delete(dt, delete_key))
            fprintf(stderr, "failed to delete key: %s\n", delete_key);
    }
    end = clock();
    avg_time_delete = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    printf("DTable Stress Test Completed:\n");
    printf("  Avg insert: %.6f ns\n", avg_time_insert);
    printf("  Avg lookup: %.6f ns\n", avg_time_lookup);
    printf("  Avg delete: %.6f ns\n", avg_time_delete);
    dt_destroy(dt);
    
    // --- stb_ds Hashtable Profiling ---
    stb_entry_t *stb_map = NULL;
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        char *key = random_string(10);
        my_type_t value = { rand(), random_string(15) };
        hmput(stb_map, key, value);
        if (hmgeti(stb_map, key) >= 0)
            inserted_keys_stb[inserted_count_stb++] = key;
    }
    end = clock();
    avg_time_insert = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        char *key = inserted_count_stb ? inserted_keys_stb[rand() % inserted_count_stb] : NULL;
        if (key) { my_type_t val = hmget(stb_map, key); (void)val; }
    }
    end = clock();
    avg_time_lookup = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    start = clock();
    for (uint32_t op = 0; op < NOPS; op++) {
        char *key = inserted_count_stb ? inserted_keys_stb[rand() % inserted_count_stb] : NULL;
        if (key) hmdel(stb_map, key);
    }
    end = clock();
    avg_time_delete = ((double)(end - start) / CLOCKS_PER_SEC / NOPS) * 1e9;
    
    printf("stb_ds Hashtable Stress Test Completed:\n");
    printf("  Avg insert: %.6f ns\n", avg_time_insert);
    printf("  Avg lookup: %.6f ns\n", avg_time_lookup);
    printf("  Avg delete: %.6f ns\n", avg_time_delete);
    printf("  Final element count: %td\n", hmlen(stb_map));
    hmfree(stb_map);
    
    // --- Generic Hash Table Profiling ---
    // Using the improved dt-style generic hash table (ht_t)
    HashTable *ght = malloc(sizeof(HashTable));
    if (!ght) { perror("malloc failed"); return 1; }
    if (!ght) { perror("ht_create failed"); return 1; }
    clock_t op_start, op_end;
    double total_insert = 0.0, total_lookup = 0.0, total_delete = 0.0;
    
    for (uint32_t op = 0; op < NOPS; op++) {
        char *key = random_string(10);
        my_type_t value = { rand(), rand(), random_string(15) };
        op_start = clock();
        ht_insert(ght, key, &value);
        op_end = clock();
        total_insert += ((double)(op_end - op_start) / CLOCKS_PER_SEC) * 1e9;
        inserted_keys_generic[inserted_count_generic++] = key;
    }
    
    for (uint32_t op = 0; op < NOPS; op++) {
        const char *lookup_key = inserted_count_generic ? inserted_keys_generic[rand() % inserted_count_generic] : NULL;
        op_start = clock();
        if (!ht_find(ght, lookup_key))
            fprintf(stderr, "failed to lookup key: %s\n", lookup_key);
        op_end = clock();
        total_lookup += ((double)(op_end - op_start) / CLOCKS_PER_SEC) * 1e9;
    }
    
    for (uint32_t op = 0; op < NOPS; op++) {
        const char *delete_key = inserted_count_generic ? inserted_keys_generic[rand() % inserted_count_generic] : NULL;
        op_start = clock();
        ht_delete(ght, delete_key);
        op_end = clock();
        total_delete += ((double)(op_end - op_start) / CLOCKS_PER_SEC) * 1e9;
    }
    
    avg_time_insert = total_insert / NOPS;
    avg_time_lookup = total_lookup / NOPS;
    avg_time_delete = total_delete / NOPS;
    
    printf("Generic Hash Table Stress Test Completed:\n");
    printf("  Avg insert: %.6f ns\n", avg_time_insert);
    printf("  Avg lookup: %.6f ns\n", avg_time_lookup);
    printf("  Avg delete: %.6f ns\n", avg_time_delete);
    
    ht_destroy(ght);
    return 0;
}
