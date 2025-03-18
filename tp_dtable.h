#ifndef TP_DTABLE_H
#define TP_DTABLE_H

#include <stdint.h>
#include <stddef.h>
#include <limits.h>

typedef struct dtable_t {
    uint32_t capacity;    /* Total number of slots */
    uint32_t count;       /* Stored items */
    uint32_t numBuckets;  /* capacity / BUCKET_SIZE */
    size_t key_size;
    size_t value_size;
    char *keys;
    char *values;
    uint8_t *bitmap;      /* One byte per bucket */
} dtable_t;

dtable_t *dt_create(size_t key_size, size_t value_size);
void dt_destroy(dtable_t *dt);
void dt_reset(dtable_t *dt);
uint32_t dt_insert(dtable_t *dt, const void *key, const void *value);
int dt_lookup(dtable_t *dt, const void *key, void *value_out);
int dt_delete(dtable_t *dt, const void *key);
size_t dt_active_memory_usage(dtable_t *dt);
uint32_t hash_key(const void *key, size_t key_size);

#endif /* TP_DTABLE_H */

#ifdef TP_DTABLE_IMPLEMENTATION

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#ifndef BUCKET_SIZE
#define BUCKET_SIZE 8
#endif

#ifndef INITIAL_CAPACITY
#define INITIAL_CAPACITY 64
#endif

#ifndef MAX_CAPACITY
#define MAX_CAPACITY (1 << 20)  /* 1M slots */
#endif

#ifndef LOAD_FACTOR_THRESHOLD
#define LOAD_FACTOR_THRESHOLD 0.7
#endif

uint32_t hash_key(const void *key, size_t key_size) {
    const unsigned char *p = key;
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < key_size; i++) {
        hash ^= p[i];
        hash *= 16777619;
    }
    return hash;
}

static int allocate_arrays(dtable_t *dt, uint32_t capacity) {
    dt->keys = calloc(capacity, dt->key_size);
    if (!dt->keys) return 0;
    dt->values = calloc(capacity, dt->value_size);
    if (!dt->values) { free(dt->keys); return 0; }
    dt->bitmap = calloc(capacity / BUCKET_SIZE, sizeof(uint8_t));
    if (!dt->bitmap) { free(dt->keys); free(dt->values); return 0; }
    return 1;
}

dtable_t *dt_create(size_t key_size, size_t value_size) {
    if (INITIAL_CAPACITY % BUCKET_SIZE != 0)
        return NULL;
    dtable_t *dt = malloc(sizeof(dtable_t));
    if (!dt) return NULL;
    dt->capacity = INITIAL_CAPACITY;
    dt->count = 0;
    dt->numBuckets = dt->capacity / BUCKET_SIZE;
    dt->key_size = key_size;
    dt->value_size = value_size;
    if (!allocate_arrays(dt, dt->capacity)) {
        free(dt);
        return NULL;
    }
    return dt;
}

void dt_destroy(dtable_t *dt) {
    if (!dt) return;
    free(dt->keys);
    free(dt->values);
    free(dt->bitmap);
    free(dt);
}

void dt_reset(dtable_t *dt) {
    if (!dt) return;
    dt->count = 0;
    memset(dt->keys, 0, dt->capacity * dt->key_size);
    memset(dt->values, 0, dt->capacity * dt->value_size);
    memset(dt->bitmap, 0, (dt->capacity / BUCKET_SIZE) * sizeof(uint8_t));
}

static int dt_rehash(dtable_t *dt) {
    if (dt->capacity >= MAX_CAPACITY)
        return 0;
    uint32_t old_capacity = dt->capacity;
    uint32_t new_capacity = dt->capacity * 2;
    if (new_capacity > MAX_CAPACITY)
        new_capacity = MAX_CAPACITY;
    uint32_t new_numBuckets = new_capacity / BUCKET_SIZE;

    char *old_keys = dt->keys;
    char *old_values = dt->values;
    uint8_t *old_bitmap = dt->bitmap;
    uint32_t old_numBuckets = dt->numBuckets;

    char *new_keys = calloc(new_capacity, dt->key_size);
    char *new_values = calloc(new_capacity, dt->value_size);
    uint8_t *new_bitmap = calloc(new_capacity / BUCKET_SIZE, sizeof(uint8_t));
    if (!new_keys || !new_values || !new_bitmap) {
        free(new_keys); free(new_values); free(new_bitmap);
        return 0;
    }
    
    dt->capacity = new_capacity;
    dt->numBuckets = new_numBuckets;
    dt->keys = new_keys;
    dt->values = new_values;
    dt->bitmap = new_bitmap;
    uint32_t old_count = dt->count;
    dt->count = 0;
    
    for (uint32_t bucket = 0; bucket < old_numBuckets; bucket++) {
        uint8_t bm = old_bitmap[bucket];
        for (uint32_t i = 0; i < BUCKET_SIZE; i++) {
            if (bm & (1 << i)) {
                uint32_t pos_old = bucket * BUCKET_SIZE + i;
                void *key = old_keys + pos_old * dt->key_size;
                void *value = old_values + pos_old * dt->value_size;
                dt_insert(dt, key, value);
            }
        }
    }
    free(old_keys);
    free(old_values);
    free(old_bitmap);
    /* dt->count should equal old_count */
    assert(dt->count == old_count);
    return 1;
}

uint32_t dt_insert(dtable_t *dt, const void *key, const void *value) {
    if ((double)dt->count / dt->capacity > LOAD_FACTOR_THRESHOLD) {
        if (!dt_rehash(dt))
            return UINT32_MAX;
    }
    uint32_t hash = hash_key(key, dt->key_size);
    uint32_t bucket = hash % dt->numBuckets;
    for (uint32_t i = 0; i < BUCKET_SIZE; i++) {
        if (i + 1 < BUCKET_SIZE)
            __builtin_prefetch(dt->keys + ((bucket * BUCKET_SIZE + i + 1) * dt->key_size));
        uint32_t pos = bucket * BUCKET_SIZE + i;
        if (!(dt->bitmap[bucket] & (1 << i))) {
            dt->bitmap[bucket] |= (1 << i);
            memcpy(dt->keys + pos * dt->key_size, key, dt->key_size);
            memcpy(dt->values + pos * dt->value_size, value, dt->value_size);
            dt->count++;
            return i;
        }
    }
    if (dt->capacity < MAX_CAPACITY) {
        if (!dt_rehash(dt))
            return UINT32_MAX;
        return dt_insert(dt, key, value);
    }
    return UINT32_MAX;
}

int dt_lookup(dtable_t *dt, const void *key, void *value_out) {
    uint32_t hash = hash_key(key, dt->key_size);
    uint32_t bucket = hash % dt->numBuckets;
    for (uint32_t i = 0; i < BUCKET_SIZE; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
        __builtin_prefetch(dt->keys + ((bucket * BUCKET_SIZE + i + 1) * dt->key_size));
        if (dt->bitmap[bucket] & (1 << i)) {
            if (memcmp(dt->keys + pos * dt->key_size, key, dt->key_size) == 0) {
                if (value_out)
                    memcpy(value_out, dt->values + pos * dt->value_size, dt->value_size);
                return 1;
            }
        }
    }
    return 0;
}

int dt_delete(dtable_t *dt, const void *key) {
    uint32_t hash = hash_key(key, dt->key_size);
    uint32_t bucket = hash % dt->numBuckets;
    for (uint32_t i = 0; i < BUCKET_SIZE; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
        if (dt->bitmap[bucket] & (1 << i)) {
            if (memcmp(dt->keys + pos * dt->key_size, key, dt->key_size) == 0) {
                dt->bitmap[bucket] &= ~(1 << i);
                memset(dt->keys + pos * dt->key_size, 0, dt->key_size);
                memset(dt->values + pos * dt->value_size, 0, dt->value_size);
                dt->count--;
                return 1;
            }
        }
    }
    return 0;
}

size_t dt_active_memory_usage(dtable_t *dt) {
    size_t keys_usage   = dt->capacity * dt->key_size;
    size_t values_usage = dt->capacity * dt->value_size;
    size_t bitmap_usage = (dt->capacity / BUCKET_SIZE) * sizeof(uint8_t);
    return sizeof(dtable_t) + keys_usage + values_usage + bitmap_usage;
}

#endif /* TP_DTABLE_IMPLEMENTATION */
