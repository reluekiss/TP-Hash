#ifndef HT_H
#define HT_H
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#define INITIAL_CAPACITY 1024

typedef struct {
    size_t capacity;     /* Must be power of two */
    size_t size;
    size_t key_size;
    size_t value_size;
    size_t entry_size;   /* = 1 + key_size + value_size */
    unsigned char *entries;
} fhm;

fhm *fhm_create(size_t key_size, size_t value_size);
void fhm_destroy(fhm *map);
void fhm_grow(fhm *map);
void *fhm_get(fhm *map, const void *key);
int fhm_insert(fhm *map, const void *key, const void *value);
void fhm_remove(fhm *map, const void *key);
void fhm_reset(fhm *map);

#endif /* HT_H */

#ifdef HT_IMPLEMENTATION
/* FNV-1a 64-bit hash for arbitrary data */
static size_t default_hash(const void *key, size_t key_size) {
    const unsigned char *data = key;
    size_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < key_size; i++) {
        hash ^= data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

/* Entry layout:
   [0]: int8_t dist  (-1 means empty)
   [1 .. key_size]: key bytes
   [1+key_size .. entry_size-1]: value bytes */
#define ENTRY_DIST(e, entry_size) (*((int8_t*)(e)))
#define ENTRY_KEY(e) ((e) + 1)
#define ENTRY_VALUE(e, key_size) ((e) + 1 + (key_size))

/* Allocate new map with initial_capacity entries (will round up to power-of-two) */
fhm *fhm_create(size_t key_size, size_t value_size) {
    fhm *map = malloc(sizeof(fhm));
    if (!map)
        return NULL;
    size_t cap = 1;
    while (cap < INITIAL_CAPACITY)
        cap *= 2;
    map->capacity = cap;
    map->size = 0;
    map->key_size = key_size;
    map->value_size = value_size;
    map->entry_size = 1 + key_size + value_size;
    map->entries = malloc(cap * map->entry_size);
    if (!map->entries) {
        free(map);
        return NULL;
    }
    for (size_t i = 0; i < cap; i++) {
        unsigned char *e = map->entries + i * map->entry_size;
        ENTRY_DIST(e, map->entry_size) = -1;
    }
    return map;
}

void fhm_destroy(fhm *map) {
    if (map) {
        free(map->entries);
        free(map);
    }
}

/* Returns index in table given hash */
static inline size_t index_for_hash(size_t hash, size_t capacity) {
    return hash & (capacity - 1);
}

/* Grow the table to new_capacity = old_capacity * 2 */
void fhm_grow(fhm *map) {
    size_t old_cap = map->capacity;
    unsigned char *old_entries = map->entries;

    size_t new_cap = old_cap * 2;
    unsigned char *new_entries = malloc(new_cap * map->entry_size);
    if (!new_entries)
        exit(EXIT_FAILURE);
    for (size_t i = 0; i < new_cap; i++) {
        unsigned char *e = new_entries + i * map->entry_size;
        ENTRY_DIST(e, map->entry_size) = -1;
    }
    /* Reinsert existing entries */
    for (size_t i = 0; i < old_cap; i++) {
        unsigned char *old_e = old_entries + i * map->entry_size;
        if (ENTRY_DIST(old_e, map->entry_size) < 0)
            continue;
        unsigned char key_buf[map->key_size];
        unsigned char value_buf[map->value_size];
        memcpy(key_buf, ENTRY_KEY(old_e), map->key_size);
        memcpy(value_buf, ENTRY_VALUE(old_e, map->key_size), map->value_size);
        size_t hash = default_hash(key_buf, map->key_size);
        size_t idx = index_for_hash(hash, new_cap);
        int d = 0;
        while (1) {
            unsigned char *new_e = new_entries + idx * map->entry_size;
            if (ENTRY_DIST(new_e, map->entry_size) < 0) {
                memcpy(ENTRY_KEY(new_e), key_buf, map->key_size);
                memcpy(ENTRY_VALUE(new_e, map->key_size), value_buf, map->value_size);
                ENTRY_DIST(new_e, map->entry_size) = d;
                break;
            } else {
                int cur_d = ENTRY_DIST(new_e, map->entry_size);
                if (cur_d < d) {
                    /* swap */
                    unsigned char tmp_key[map->key_size];
                    unsigned char tmp_value[map->value_size];
                    memcpy(tmp_key, ENTRY_KEY(new_e), map->key_size);
                    memcpy(tmp_value, ENTRY_VALUE(new_e, map->key_size), map->value_size);
                    int tmp_d = cur_d;
                    memcpy(ENTRY_KEY(new_e), key_buf, map->key_size);
                    memcpy(ENTRY_VALUE(new_e, map->key_size), value_buf, map->value_size);
                    ENTRY_DIST(new_e, map->entry_size) = d;
                    memcpy(key_buf, tmp_key, map->key_size);
                    memcpy(value_buf, tmp_value, map->value_size);
                    d = tmp_d;
                }
            }
            d++;
            idx = (idx + 1) & (new_cap - 1);
        }
    }
    free(old_entries);
    map->entries = new_entries;
    map->capacity = new_cap;
}

/* Insert (or update) key/value pair.
   Returns 1 if a new key was inserted, 0 if updated.
   key and value are pointers to raw bytes. */
int fhm_insert(fhm *map, const void *key, const void *value) {
    if (map->size + 1 > map->capacity / 2)
        fhm_grow(map);

    size_t hash = default_hash(key, map->key_size);
    size_t idx = index_for_hash(hash, map->capacity);
    int d = 0;
    while (1) {
        unsigned char *e = map->entries + idx * map->entry_size;
        int entry_d = ENTRY_DIST(e, map->entry_size);
        if (entry_d < 0) {
            memcpy(ENTRY_KEY(e), key, map->key_size);
            memcpy(ENTRY_VALUE(e, map->key_size), value, map->value_size);
            ENTRY_DIST(e, map->entry_size) = d;
            map->size++;
            return 1;
        } else if (memcmp(ENTRY_KEY(e), key, map->key_size) == 0) {
            memcpy(ENTRY_VALUE(e, map->key_size), value, map->value_size);
            return 0;
        } else if (entry_d < d) {
            /* Swap with the resident element */
            unsigned char tmp_key[map->key_size];
            unsigned char tmp_value[map->value_size];
            memcpy(tmp_key, ENTRY_KEY(e), map->key_size);
            memcpy(tmp_value, ENTRY_VALUE(e, map->key_size), map->value_size);
            int tmp_d = entry_d;
            memcpy(ENTRY_KEY(e), key, map->key_size);
            memcpy(ENTRY_VALUE(e, map->key_size), value, map->value_size);
            ENTRY_DIST(e, map->entry_size) = d;
            key = tmp_key;
            value = tmp_value;
            d = tmp_d;
        }
        d++;
        idx = (idx + 1) & (map->capacity - 1);
    }
}

/* Retrieve pointer to value associated with key, or NULL if not found */
void *fhm_get(fhm *map, const void *key) {
    size_t hash = default_hash(key, map->key_size);
    size_t idx = index_for_hash(hash, map->capacity);
    int d = 0;
    while (1) {
        unsigned char *e = map->entries + idx * map->entry_size;
        int entry_d = ENTRY_DIST(e, map->entry_size);
        if (entry_d < 0 || d > entry_d)
            return NULL;
        if (memcmp(ENTRY_KEY(e), key, map->key_size) == 0)
            return ENTRY_VALUE(e, map->key_size);
        d++;
        idx = (idx + 1) & (map->capacity - 1);
    }
}

void fhm_remove(fhm *map, const void *key) {
    size_t hash = default_hash(key, map->key_size);
    size_t idx = index_for_hash(hash, map->capacity);
    int d = 0;
    while (1) {
        unsigned char *e = map->entries + idx * map->entry_size;
        int entry_d = ENTRY_DIST(e, map->entry_size);
        if (entry_d < 0 || d > entry_d)
            return; // key not found.
        if (memcmp(ENTRY_KEY(e), key, map->key_size) == 0) {
            /* Mark this slot empty */
            ENTRY_DIST(e, map->entry_size) = -1;
            memset(ENTRY_KEY(e), 0, map->key_size + map->value_size);
            map->size--;
            /* Shift subsequent entries backward until an empty slot or a slot with zero distance is encountered */
            size_t next = (idx + 1) & (map->capacity - 1);
            while (1) {
                unsigned char *nxt = map->entries + next * map->entry_size;
                if (ENTRY_DIST(nxt, map->entry_size) <= 0)
                    break;
                /* Move the next entry to the current slot */
                memcpy(e, nxt, map->entry_size);
                /* Decrement its distance */
                ENTRY_DIST(e, map->entry_size)--;
                /* Mark the next slot empty */
                ENTRY_DIST(nxt, map->entry_size) = -1;
                memset(ENTRY_KEY(nxt), 0, map->key_size + map->value_size);
                idx = next;
                next = (next + 1) & (map->capacity - 1);
                e = map->entries + idx * map->entry_size;
            }
            return;
        }
        d++;
        idx = (idx + 1) & (map->capacity - 1);
    }
}

/* Reset the entire hash table by marking all entries empty and zeroing out key/value data.
   The table's memory is preserved and map->size is reset to 0. */
void fhm_reset(fhm *map) {
    for (size_t i = 0; i < map->capacity; i++) {
        unsigned char *e = map->entries + i * map->entry_size;
        ENTRY_DIST(e, map->entry_size) = -1;
        memset(ENTRY_KEY(e), 0, map->key_size + map->value_size);
    }
    map->size = 0;
}
#endif // HT_IMPLEMENTATION
