#ifndef TP_DTABLE_H
#define TP_DTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <limits.h>

/*-----------------------------------------------------------------------------
    Public API
-----------------------------------------------------------------------------*/

/* dtable_t is a structure representing a dynamically sized tiny‑pointer
   dereference table. The table reserves a large virtual region but only uses
   an active region that grows over time.
*/
typedef struct dtable_t {
    uint32_t current_capacity;  /* Active number of slots in use */
    uint32_t count;             /* Number of stored items */
    uint32_t numBuckets;        /* Fixed number of buckets (set at creation based on INITIAL_CAPACITY) */
    /* Arrays allocated over MAX_CAPACITY slots */
    uint32_t *keys;             /* Stored keys */
    uint32_t *values;           /* Stored values */
    uint8_t  *bitmap;           /* One byte per bucket (each bit indicates occupancy) */
} dtable_t;

/* dt_create() creates and returns a new table.
   Returns NULL on failure.
*/
dtable_t *dt_create(void);

/* dt_destroy(dt) frees all memory used by dt.
*/
void dt_destroy(dtable_t *dt);

/* dt_reset(dt) resets dt to an empty state, setting its active capacity back
   to INITIAL_CAPACITY without unmapping any memory.
*/
void dt_reset(dtable_t *dt);

/* dt_insert(dt, key, value) inserts a key/value pair into dt.
   Returns the tiny pointer (an offset within the bucket) on success,
   or UINT32_MAX on failure.
*/
uint32_t dt_insert(dtable_t *dt, uint32_t key, uint32_t value);

/* dt_lookup(dt, key, value_out) looks up key in dt.
   If found, stores the associated value in *value_out (if non-NULL) and returns nonzero.
   Otherwise, returns 0.
*/
int dt_lookup(dtable_t *dt, uint32_t key, uint32_t *value_out);

/* dt_delete(dt, key) deletes key from dt.
   Returns nonzero if key was found and deleted.
*/
int dt_delete(dtable_t *dt, uint32_t key);

/* dt_active_memory_usage(dt) returns the current active memory usage (in bytes)
   of the table (including internal metadata) based on its active capacity.
*/
size_t dt_active_memory_usage(dtable_t *dt);

/* hash_key(key) is a helper hash function.
*/
uint32_t hash_key(uint32_t key);

#ifdef __cplusplus
}
#endif

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
#define INITIAL_CAPACITY 64       /* Initially active slots (must be a multiple of BUCKET_SIZE) */
#endif

#ifndef MAX_CAPACITY
#define MAX_CAPACITY (1 << 20)    /* Reserve space for up to 1M slots */
#endif

#ifndef LOAD_FACTOR_UPPER
#define LOAD_FACTOR_UPPER 0.75    /* (Not used directly here; growth is triggered on bucket fullness) */
#endif

/*-----------------------------------------------------------------------------
    Helper Functions
-----------------------------------------------------------------------------*/

/* hash_key() is a simple 32-bit hash function. */
uint32_t hash_key(uint32_t key) {
    key ^= key >> 16;
    key *= 0x85ebca6b;
    key ^= key >> 13;
    key *= 0xc2b2ae35;
    key ^= key >> 16;
    return key;
}

/* xmap() allocates memory using mmap */
static inline void *xmap(size_t size) {
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    return (p == MAP_FAILED) ? NULL : p;
}

/*-----------------------------------------------------------------------------
    API Implementation
-----------------------------------------------------------------------------*/

dtable_t *dt_create(void) {
    if (INITIAL_CAPACITY % BUCKET_SIZE != 0)
        return NULL;
    dtable_t *dt = xmap(sizeof(dtable_t));
    if (!dt) return NULL;
    dt->current_capacity = INITIAL_CAPACITY;
    dt->count = 0;
    dt->numBuckets = INITIAL_CAPACITY / BUCKET_SIZE;  /* Fixed bucket count */
    
    dt->keys = xmap(MAX_CAPACITY * sizeof(uint32_t));
    dt->values = xmap(MAX_CAPACITY * sizeof(uint32_t));
    /* Reserve bitmap for MAX_CAPACITY/BUCKET_SIZE buckets */
    dt->bitmap = xmap((MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
    if (!dt->keys || !dt->values || !dt->bitmap) {
        if (dt->keys) munmap(dt->keys, MAX_CAPACITY * sizeof(uint32_t));
        if (dt->values) munmap(dt->values, MAX_CAPACITY * sizeof(uint32_t));
        if (dt->bitmap) munmap(dt->bitmap, (MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
        munmap(dt, sizeof(dtable_t));
        return NULL;
    }
    /* Memory from MAP_ANONYMOUS is zeroed */
    return dt;
}

void dt_destroy(dtable_t *dt) {
    if (!dt) return;
    munmap(dt->keys, MAX_CAPACITY * sizeof(uint32_t));
    munmap(dt->values, MAX_CAPACITY * sizeof(uint32_t));
    munmap(dt->bitmap, (MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
    munmap(dt, sizeof(dtable_t));
}

void dt_reset(dtable_t *dt) {
    if (!dt) return;
    dt->count = 0;
    dt->current_capacity = INITIAL_CAPACITY;
    memset(dt->keys, 0, MAX_CAPACITY * sizeof(uint32_t));
    memset(dt->values, 0, MAX_CAPACITY * sizeof(uint32_t));
    memset(dt->bitmap, 0, (MAX_CAPACITY / BUCKET_SIZE) * sizeof(uint8_t));
}

size_t dt_active_memory_usage(dtable_t *dt) {
    size_t keys_usage = dt->current_capacity * sizeof(uint32_t);
    size_t values_usage = dt->current_capacity * sizeof(uint32_t);
    size_t bitmap_usage = (dt->current_capacity / BUCKET_SIZE) * sizeof(uint8_t);
    return sizeof(dtable_t) + keys_usage + values_usage + bitmap_usage;
}

uint32_t dt_insert(dtable_t *dt, uint32_t key, uint32_t value) {
    /* Use a fixed total bucket count based on MAX_CAPACITY */
    uint32_t totalBuckets = MAX_CAPACITY / BUCKET_SIZE;
    uint32_t bucket = hash_key(key) % totalBuckets;
    /* Determine active slots per bucket:
       active_slots = current_capacity * BUCKET_SIZE / INITIAL_CAPACITY, capped at BUCKET_SIZE.
    */
    uint32_t active_slots = (dt->current_capacity * BUCKET_SIZE) / INITIAL_CAPACITY;
    if (active_slots > BUCKET_SIZE)
        active_slots = BUCKET_SIZE;
    
    for (uint32_t i = 0; i < active_slots; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
        if (!(dt->bitmap[bucket] & (1 << i))) {
            dt->bitmap[bucket] |= (1 << i);
            dt->keys[pos] = key;
            dt->values[pos] = value;
            dt->count++;
            return i;  /* Tiny pointer: offset within bucket */
        }
    }
    /* If the bucket in the active region is full, grow if possible */
    if (dt->current_capacity < MAX_CAPACITY) {
        uint32_t new_capacity = dt->current_capacity * 2;
        if (new_capacity > MAX_CAPACITY)
            new_capacity = MAX_CAPACITY;
        dt->current_capacity = new_capacity;
        /* No rehashing is required since bucket count remains fixed */
        return dt_insert(dt, key, value);
    }
    return UINT32_MAX;  /* Table full */
}

int dt_lookup(dtable_t *dt, uint32_t key, uint32_t *value_out) {
    uint32_t totalBuckets = MAX_CAPACITY / BUCKET_SIZE;
    uint32_t bucket = hash_key(key) % totalBuckets;
    uint32_t active_slots = (dt->current_capacity * BUCKET_SIZE) / INITIAL_CAPACITY;
    if (active_slots > BUCKET_SIZE)
        active_slots = BUCKET_SIZE;
    for (uint32_t i = 0; i < active_slots; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
        if (dt->bitmap[bucket] & (1 << i)) {
            if (dt->keys[pos] == key) {
                if (value_out)
                    *value_out = dt->values[pos];
                return 1;
            }
        }
    }
    return 0;
}

int dt_delete(dtable_t *dt, uint32_t key) {
    uint32_t totalBuckets = MAX_CAPACITY / BUCKET_SIZE;
    uint32_t bucket = hash_key(key) % totalBuckets;
    uint32_t active_slots = (dt->current_capacity * BUCKET_SIZE) / INITIAL_CAPACITY;
    if (active_slots > BUCKET_SIZE)
        active_slots = BUCKET_SIZE;
    for (uint32_t i = 0; i < active_slots; i++) {
        uint32_t pos = bucket * BUCKET_SIZE + i;
        if (dt->bitmap[bucket] & (1 << i)) {
            if (dt->keys[pos] == key) {
                dt->bitmap[bucket] &= ~(1 << i);
                dt->keys[pos] = 0;
                dt->values[pos] = 0;
                dt->count--;
                return 1;
            }
        }
    }
    return 0;
}

#endif /* TP_DTABLE_IMPLEMENTATION */
#endif /* TP_DTABLE_H */
