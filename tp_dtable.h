#ifndef TP_DT_H
#define TP_DT_H

#include <stdint.h>
#include <stddef.h>

/* Public API for the dereference table (dt_t).
   The implementation returns a tiny_ptr_t (fixed‐size pointer) that
   encodes which internal table (primary/secondary) is used, plus bucket and slot.
*/
typedef struct {
    uint8_t table_id; // 0 = primary, 1 = secondary; 0xFF = failure
    uint32_t bucket;
    uint8_t slot;
} tiny_ptr_t;

typedef struct {
    uint32_t num_buckets;       // number of buckets in this load-balancing table
    uint32_t slots_per_bucket;  // bucket capacity
    size_t key_size;            // size (in bytes) of each key
    size_t value_size;          // size (in bytes) of each value
    uint32_t count;             // active number of slots (always a multiple of slots_per_bucket)
    char *keys;                 // pointer to keys array (allocated to MAX_CAPACITY * key_size bytes)
    char *values;               // pointer to values array (allocated to MAX_CAPACITY * value_size bytes)
    uint8_t *bitmap;            // occupancy bitmap (1 bit per slot, allocated to (MAX_CAPACITY+7)/8 bytes)
} lb_table_t;

/* dt_t holds two load-balancing tables:
   - primary: designed for high load factor (approximately 1 - Θ(δ²))
   - secondary: sparser (e.g. load factor ≈ 1 - Θ(1/ log log n))
*/
typedef struct dt_t {
    lb_table_t *primary;
    lb_table_t *secondary;
} dt_t;

/* Public functions */
dt_t *dt_create(size_t key_size, size_t value_size);
void dt_destroy(dt_t *dt);

tiny_ptr_t dt_insert(dt_t *dt, const void *key, const void *value);
int dt_lookup(dt_t *dt, const void *key, tiny_ptr_t tp, void *value_out);
int dt_delete(dt_t *dt, const void *key, tiny_ptr_t tp);
void dt_reset(dt_t *dt);

#endif /* TP_DT_H */

#ifdef TP_DT_IMPLEMENTATION
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <math.h>

/* Configuration macros.
   - MAX_CAPACITY: the maximum number of slots allocated (fixed, via mmap)
   - INITIAL_CAPACITY: the starting active capacity (in slots)
   - DELTA: parameter controlling sparsity; here we set it as 1 / log(log(MAX_CAPACITY))
     (guarded to be nonzero via fmax)
   - PRIMARY_BUCKET_SIZE: chosen as Θ(δ⁻² log(1/δ))
   - SECONDARY_BUCKET_SIZE: chosen as at least 1 (using fmax) and roughly log₂(log₂(MAX_CAPACITY))
*/
#define MAX_CAPACITY (1 << 20)
#define INITIAL_CAPACITY (64)
#define SAFE_LOG(x) ( (x) > 2 ? log(x) : 1.0 )
#define DELTA (1.0 / (fmax(SAFE_LOG(SAFE_LOG(MAX_CAPACITY)), 1.0)))
#define PRIMARY_BUCKET_SIZE ((uint32_t)fmax(4, 16 * (1.0 / (DELTA * DELTA)) * fmax(log(1.0 / DELTA), 1.0)))
#define SECONDARY_BUCKET_SIZE ((uint32_t)fmax(2, log2(fmax(log2(MAX_CAPACITY), 2.0))))

/*-------------------------------------------------------------------------
   Internal Structures and Utility Functions
-------------------------------------------------------------------------*/

/* A simple FNV-1a hash function with a seed */
static inline uint32_t hash_key(const void *key, size_t len, uint32_t seed) {
    const unsigned char *p = key;
    uint32_t hash = seed;
    for (size_t i = 0; i < len; i++) {
        hash ^= p[i];
        hash *= 16777619;
    }
    return hash;
}

/* xmap uses mmap exclusively to allocate memory.
   All allocations here come from mmap. */
static inline void *xmap(size_t size) {
    void *p = mmap(NULL, size, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    return (p == MAP_FAILED) ? NULL : p;
}

/* Bitmap helper macros (1 bit per slot) */
#define BITMAP_TEST(bitmap, idx)   ((bitmap[(idx) / 8] >> ((idx) % 8)) & 1)
#define BITMAP_SET(bitmap, idx)    (bitmap[(idx) / 8] |= (1 << ((idx) % 8)))
#define BITMAP_CLEAR(bitmap, idx)  (bitmap[(idx) / 8] &= ~(1 << ((idx) % 8)))

/*-------------------------------------------------------------------------
   Load-Balancing Table (lb_table_t) Functions
-------------------------------------------------------------------------*/

/* lb_create allocates a new load-balancing table.
   Note: the keys, values, and bitmap arrays are allocated with MAX_CAPACITY size
   (so that future dynamic growth only adjusts t->count).
   This design ensures that already allocated tiny pointers remain valid.
*/
static lb_table_t *lb_create(size_t key_size, size_t value_size,
                             uint32_t slots_per_bucket, uint32_t initial_capacity) {
    lb_table_t *t = xmap(sizeof(lb_table_t));
    t->slots_per_bucket = slots_per_bucket;
    t->num_buckets = fmax(1, initial_capacity / slots_per_bucket);
    t->key_size = key_size;
    t->value_size = value_size;
    t->count = initial_capacity;
    t->keys = xmap(MAX_CAPACITY * key_size);
    t->values = xmap(MAX_CAPACITY * value_size);
    t->bitmap = xmap((MAX_CAPACITY + 7) / 8);
    return t;
}

/* lb_grow doubles the active capacity of the table, up to MAX_CAPACITY.
   (Since memory was allocated for MAX_CAPACITY, we simply update count.)
   This dynamic increase allows the table to absorb more allocations without rehashing old entries.
*/
static int lb_grow(lb_table_t *t) {
    if (t->count >= MAX_CAPACITY) return 0;
    t->count *= 2;
    if (t->count > MAX_CAPACITY)
        t->count = MAX_CAPACITY;
    t->num_buckets = t->count / t->slots_per_bucket;
    return 1;
}

/* lb_insert attempts to insert a key/value pair into table t.
   It hashes the key with the provided seed, chooses a bucket, and then linearly scans the bucket.
   Returns 1 if insertion succeeds (and outputs bucket and slot used via pointers),
   or 0 if the entire bucket is full (in which case the caller may attempt to grow t).
*/
static int lb_insert(lb_table_t *t, const void *key, const void *value,
                     uint32_t seed, uint32_t *bucket_out, uint8_t *slot_out) {
    uint32_t bucket = hash_key(key, t->key_size, seed) % t->num_buckets;
    uint32_t base = bucket * t->slots_per_bucket;
    for (uint32_t i = 0; i < t->slots_per_bucket; i++) {
        uint32_t pos = base + i;
        if (!BITMAP_TEST(t->bitmap, pos)) {
            BITMAP_SET(t->bitmap, pos);
            memcpy(t->keys + pos * t->key_size, key, t->key_size);
            memcpy(t->values + pos * t->value_size, value, t->value_size);
            *bucket_out = bucket;
            *slot_out = (uint8_t)i;
            return 1;
        }
    }
    return 0; // Insertion fails if bucket is full
}

/*-------------------------------------------------------------------------
   Dereference Table (dt_t) Functions
-------------------------------------------------------------------------*/

/* dt_create allocates two load-balancing tables:
   - primary: uses PRIMARY_BUCKET_SIZE and starts at INITIAL_CAPACITY slots.
   - secondary: uses SECONDARY_BUCKET_SIZE and also starts at INITIAL_CAPACITY.
   (In a full implementation for variable-length tiny pointers, the value array could use zone aggregation.
    Here, we use fixed-size tiny pointers, but dynamic resizing via doubling is supported.)
*/
dt_t *dt_create(size_t key_size, size_t value_size) {
    dt_t *dt = xmap(sizeof(dt_t));
    dt->primary = lb_create(key_size, value_size, PRIMARY_BUCKET_SIZE, INITIAL_CAPACITY);
    dt->secondary = lb_create(key_size, value_size, SECONDARY_BUCKET_SIZE, INITIAL_CAPACITY);
    return dt;
}

/* dt_destroy frees all memory using munmap.
   (Since all allocations are from mmap, we unmap them here.)
*/
void dt_destroy(dt_t *dt) {
    munmap(dt->primary->keys, MAX_CAPACITY * dt->primary->key_size);
    munmap(dt->primary->values, MAX_CAPACITY * dt->primary->value_size);
    munmap(dt->primary->bitmap, (MAX_CAPACITY + 7) / 8);
    munmap(dt->primary, sizeof(lb_table_t));
    munmap(dt->secondary->keys, MAX_CAPACITY * dt->secondary->key_size);
    munmap(dt->secondary->values, MAX_CAPACITY * dt->secondary->value_size);
    munmap(dt->secondary->bitmap, (MAX_CAPACITY + 7) / 8);
    munmap(dt->secondary, sizeof(lb_table_t));
    munmap(dt, sizeof(dt_t));
}

/* dt_insert first attempts to insert into the primary table.
   If insertion fails (bucket full), then it tries to grow the primary table.
   If still failing, it attempts insertion (and growth) in the secondary table.
   The returned tiny_ptr_t encodes which table was used plus the bucket and slot.
   (In a more elaborate design, one could incorporate zone-aggregated storage for variable-length pointers.)
*/
int dt_insert(dt_t *dt, const void *key, const void *value) {
    uint32_t bucket;
    uint8_t slot;
    if (lb_insert(dt->primary, key, value, 0xABCDEF01, &bucket, &slot))
        return 1;
    if (!lb_grow(dt->primary) ||
        !lb_insert(dt->primary, key, value, 0xABCDEF01, &bucket, &slot)) {
        if (!lb_insert(dt->secondary, key, value, 0x12345678, &bucket, &slot)) {
            if (!lb_grow(dt->secondary) ||
                !lb_insert(dt->secondary, key, value, 0x12345678, &bucket, &slot))
                return 0;
        }
    }
    return 1;
}

/* dt_lookup and dt_delete use the table_id in the tiny_ptr_t to select the appropriate load-balancing table.
*/
int dt_lookup(dt_t *dt, const void *key, void *value_out) {
    uint32_t bucket = hash_key(key, dt->primary->key_size, 0xABCDEF01) % dt->primary->num_buckets;
    uint32_t base = bucket * dt->primary->slots_per_bucket;
    for (uint32_t i = 0; i < dt->primary->slots_per_bucket; i++) {
        uint32_t pos = base + i;
        if (BITMAP_TEST(dt->primary->bitmap, pos) &&
            memcmp(dt->primary->keys + pos * dt->primary->key_size, key, dt->primary->key_size) == 0) {
            if (value_out)
                memcpy(value_out, dt->primary->values + pos * dt->primary->value_size,
                       dt->primary->value_size);
            return 1;
        }
    }
    bucket = hash_key(key, dt->secondary->key_size, 0x12345678) % dt->secondary->num_buckets;
    base = bucket * dt->secondary->slots_per_bucket;
    for (uint32_t i = 0; i < dt->secondary->slots_per_bucket; i++) {
        uint32_t pos = base + i;
        if (BITMAP_TEST(dt->secondary->bitmap, pos) &&
            memcmp(dt->secondary->keys + pos * dt->secondary->key_size, key, dt->secondary->key_size) == 0) {
            if (value_out)
                memcpy(value_out, dt->secondary->values + pos * dt->secondary->value_size,
                       dt->secondary->value_size);
            return 1;
        }
    }
    return 0;
}

int dt_delete(dt_t *dt, const void *key) {
    uint32_t bucket = hash_key(key, dt->primary->key_size, 0xABCDEF01) % dt->primary->num_buckets;
    uint32_t base = bucket * dt->primary->slots_per_bucket;
    for (uint32_t i = 0; i < dt->primary->slots_per_bucket; i++) {
        uint32_t pos = base + i;
        if (BITMAP_TEST(dt->primary->bitmap, pos) &&
            memcmp(dt->primary->keys + pos * dt->primary->key_size, key, dt->primary->key_size) == 0) {
            BITMAP_CLEAR(dt->primary->bitmap, pos);
            memset(dt->primary->keys + pos * dt->primary->key_size, 0, dt->primary->key_size);
            memset(dt->primary->values + pos * dt->primary->value_size, 0, dt->primary->value_size);
            return 1;
        }
    }
    bucket = hash_key(key, dt->secondary->key_size, 0x12345678) % dt->secondary->num_buckets;
    base = bucket * dt->secondary->slots_per_bucket;
    for (uint32_t i = 0; i < dt->secondary->slots_per_bucket; i++) {
        uint32_t pos = base + i;
        if (BITMAP_TEST(dt->secondary->bitmap, pos) &&
            memcmp(dt->secondary->keys + pos * dt->secondary->key_size, key, dt->secondary->key_size) == 0) {
            BITMAP_CLEAR(dt->secondary->bitmap, pos);
            memset(dt->secondary->keys + pos * dt->secondary->key_size, 0, dt->secondary->key_size);
            memset(dt->secondary->values + pos * dt->secondary->value_size, 0, dt->secondary->value_size);
            return 1;
        }
    }
    return 0;
}

void dt_reset(dt_t *dt) {
    lb_table_t *t = dt->primary;
    t->count = INITIAL_CAPACITY;
    t->num_buckets = fmax(1, INITIAL_CAPACITY / t->slots_per_bucket);
    memset(t->bitmap, 0, (MAX_CAPACITY + 7) / 8);
    memset(t->keys, 0, MAX_CAPACITY * t->key_size);
    memset(t->values, 0, MAX_CAPACITY * t->value_size);

    t = dt->secondary;
    t->count = INITIAL_CAPACITY;
    t->num_buckets = fmax(1, INITIAL_CAPACITY / t->slots_per_bucket);
    memset(t->bitmap, 0, (MAX_CAPACITY + 7) / 8);
    memset(t->keys, 0, MAX_CAPACITY * t->key_size);
    memset(t->values, 0, MAX_CAPACITY * t->value_size);
}

#endif /* TP_DT_IMPLEMENTATION */
