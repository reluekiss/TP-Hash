#ifndef TP_DTABLE_H
#define TP_DTABLE_H

#include <stdint.h>
#include <stddef.h>
#include <limits.h>

/*-----------------------------------------------------------------------------
    Public API
-----------------------------------------------------------------------------*/

/* dtable_t now stores keys and values of arbitrary types.
   key_size and value_size are specified at creation.
*/
typedef struct dtable_t {
    uint32_t current_capacity;  /* Active number of slots (must be multiple of BUCKET_SIZE) */
    uint32_t count;             /* Number of stored items */
    uint32_t numBuckets;        /* Fixed number of buckets (current_capacity / BUCKET_SIZE) */
    size_t key_size;            /* Size of each key */
    size_t value_size;          /* Size of each value */
    char    *keys;              /* Allocated as MAX_CAPACITY * key_size */
    char    *values;            /* Allocated as MAX_CAPACITY * value_size */
    uint8_t *bitmap;            /* One byte per bucket; each bit indicates occupancy */
} dtable_t;

/* Create a new table for keys of size key_size and values of size value_size.
   Returns NULL on failure.
*/
dtable_t *dt_create(size_t key_size, size_t value_size);

/* Free all memory used by dt. */
void dt_destroy(dtable_t *dt);

/* Reset dt to an empty state with active capacity INITIAL_CAPACITY.
   Memory remains mapped.
*/
void dt_reset(dtable_t *dt);

/* Insert a key/value pair into dt.
   key and value are pointers to the key and value to store.
   Returns a “tiny pointer” (the offset within the bucket) on success,
   or UINT32_MAX on failure.
*/
uint32_t dt_insert(dtable_t *dt, const void *key, const void *value);

/* Look up key in dt.
   If found, copies the associated value into value_out (if non-NULL) and returns nonzero.
   Otherwise, returns 0.
*/
int dt_lookup(dtable_t *dt, const void *key, void *value_out);

/* Delete key from dt.
   Returns nonzero if key was found and deleted.
*/
int dt_delete(dtable_t *dt, const void *key);

/* Return the active memory usage (in bytes) of dt (including metadata). */
size_t dt_active_memory_usage(dtable_t *dt);

/* A generic hash function; use as:
      hash_key(key, sizeof(key))
*/
uint32_t hash_key(const void *key, size_t key_size);

#endif /* TP_DTABLE_H */


/*-----------------------------------------------------------------------------
    IMPLEMENTATION
-----------------------------------------------------------------------------*/
#ifdef TP_DTABLE_IMPLEMENTATION

#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

/* Configuration macros */
#ifndef BUCKET_SIZE
#define BUCKET_SIZE 8             /* Each bucket holds 8 slots */
#endif

#ifndef INITIAL_CAPACITY
#define INITIAL_CAPACITY 64       /* Initially active slots (must be multiple of BUCKET_SIZE) */
#endif

#ifndef MAX_CAPACITY
#define MAX_CAPACITY (1 << 20)    /* Reserve space for up to 1M slots */
#endif

/*-----------------------------------------------------------------------------
    Helper Functions
-----------------------------------------------------------------------------*/

/* A simple generic FNV‑1a hash function */
uint32_t hash_key(const void *key, size_t key_size) {
    const unsigned char *p = (const unsigned char *) key;
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < key_size; i++) {
        hash ^= p[i];
        hash *= 16777619;
    }
    return hash;
}

/* xmap() allocates memory using mmap */
static inline void *xmap(size_t size) {
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    return (p == MAP_FAILED) ? NULL : p;
}

/* Helper: returns number of active slots per bucket based on current_capacity */
static uint32_t active_slots(uint32_t current_capacity) {
    uint32_t slots = (current_capacity * BUCKET_SIZE) / INITIAL_CAPACITY;
    return (slots > BUCKET_SIZE) ? BUCKET_SIZE : slots;
}

/*-----------------------------------------------------------------------------
    API Implementation
-----------------------------------------------------------------------------*/

dtable_t *dt_create(size_t key_size, size_t value_size) {
    if (INITIAL_CAPACITY % BUCKET_SIZE != 0)
        return NULL;
    dtable_t *dt = xmap(sizeof(dtable_t));
    if (!dt) return NULL;
    dt->current_capacity = INITIAL_CAPACITY;
    dt->count = 0;
    dt->numBuckets = INITIAL_CAPACITY / BUCKET_SIZE;
    dt->key_size = key_size;
    dt->value_size = value_size;
    
    dt->keys = xmap(MAX_CAPACITY * key_size);
    dt->values = xmap(MAX_CAPACITY * value_size);
    dt->bitmap = xmap((MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
    if (!dt->keys || !dt->values || !dt->bitmap) {
        if (dt->keys) munmap(dt->keys, MAX_CAPACITY * key_size);
        if (dt->values) munmap(dt->values, MAX_CAPACITY * value_size);
        if (dt->bitmap) munmap(dt->bitmap, (MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
        munmap(dt, sizeof(dtable_t));
        return NULL;
    }
    /* mmap'd memory is zeroed */
    return dt;
}

void dt_destroy(dtable_t *dt) {
    if (!dt) return;
    munmap(dt->keys, MAX_CAPACITY * dt->key_size);
    munmap(dt->values, MAX_CAPACITY * dt->value_size);
    munmap(dt->bitmap, (MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
    munmap(dt, sizeof(dtable_t));
}

void dt_reset(dtable_t *dt) {
    if (!dt) return;
    dt->count = 0;
    dt->current_capacity = INITIAL_CAPACITY;
    memset(dt->keys, 0, MAX_CAPACITY * dt->key_size);
    memset(dt->values, 0, MAX_CAPACITY * dt->value_size);
    memset(dt->bitmap, 0, (MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
}

size_t dt_active_memory_usage(dtable_t *dt) {
    size_t keys_usage   = dt->current_capacity * dt->key_size;
    size_t values_usage = dt->current_capacity * dt->value_size;
    size_t bitmap_usage = (dt->current_capacity / BUCKET_SIZE) * sizeof(uint8_t);
    return sizeof(dtable_t) + keys_usage + values_usage + bitmap_usage;
}

uint32_t dt_insert(dtable_t *dt, const void *key, const void *value) {
    uint32_t totalBuckets = MAX_CAPACITY / BUCKET_SIZE;
    uint32_t hash = hash_key(key, dt->key_size);
    uint32_t bucket = hash % totalBuckets;
    uint32_t slots = active_slots(dt->current_capacity);
    
    for (uint32_t i = 0; i < slots; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
        if (!(dt->bitmap[bucket] & (1 << i))) {
            dt->bitmap[bucket] |= (1 << i);
            memcpy(dt->keys + pos * dt->key_size, key, dt->key_size);
            memcpy(dt->values + pos * dt->value_size, value, dt->value_size);
            dt->count++;
            return i;  /* Tiny pointer: offset within bucket */
        }
    }
    /* If active region full, double current_capacity (up to MAX_CAPACITY) and try again */
    if (dt->current_capacity < MAX_CAPACITY) {
        uint32_t new_capacity = dt->current_capacity * 2;
        if (new_capacity > MAX_CAPACITY)
            new_capacity = MAX_CAPACITY;
        dt->current_capacity = new_capacity;
        return dt_insert(dt, key, value);
    }
    return UINT32_MAX;  /* Table full */
}

int dt_lookup(dtable_t *dt, const void *key, void *value_out) {
    uint32_t totalBuckets = MAX_CAPACITY / BUCKET_SIZE;
    uint32_t hash = hash_key(key, dt->key_size);
    uint32_t bucket = hash % totalBuckets;
    uint32_t slots = active_slots(dt->current_capacity);
    
    for (uint32_t i = 0; i < slots; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
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
    uint32_t totalBuckets = MAX_CAPACITY / BUCKET_SIZE;
    uint32_t hash = hash_key(key, dt->key_size);
    uint32_t bucket = hash % totalBuckets;
    uint32_t slots = active_slots(dt->current_capacity);
    
    for (uint32_t i = 0; i < slots; i++) {
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

#endif /* TP_DTABLE_IMPLEMENTATION */
