#ifndef HT_H
#define HT_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define INITIAL_CAPACITY 8
#define LOAD_FACTOR 0.5

typedef struct {
    int dist; // probe distance; -1 indicates empty slot
    char data[]; // key then value
} Entry;

typedef struct {
    size_t key_size;
    size_t value_size;
    size_t capacity;
    size_t size;
    Entry *entries; // contiguous array of capacity entries
} HashTable;

int ht_init(HashTable *ht, size_t key_size, size_t value_size);
int ht_insert(HashTable *ht, const void *key, const void *value);
int ht_resize(HashTable *ht, size_t new_capacity);
void *ht_find(HashTable *ht, const void *key);
int ht_delete(HashTable *ht, const void *key);
int ht_destroy(HashTable *ht);
int ht_reset(HashTable *ht);

#endif /* HT_H */

#ifdef HT_IMPLEMENTATION
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define INITIAL_CAPACITY 8
#define LOAD_FACTOR 0.5

/* Helper: entry size = header + key + value */
static inline size_t entry_size(HashTable *ht) {
    return sizeof(Entry) + ht->key_size + ht->value_size;
}

/* Helper: pointer to key and value inside an entry */
static inline void *entry_key(Entry *e, size_t key_size) {
    return e->data;
}
static inline void *entry_value(Entry *e, size_t key_size) {
    return e->data + key_size;
}

/* Generic FNV-1a hash function over key bytes */
static inline uint32_t fnv1(const void *key, size_t key_size) {
    const unsigned char *p = key;
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < key_size; i++) {
        hash ^= p[i];
        hash *= 16777619;
    }
    return hash;
}

static size_t next_power_of_two(size_t n) {
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

/* Internal reinsertion routine used during resizing.
   Copies the key and value from a given entry into the new table. */
static void ht_reinsert(HashTable *ht, const void *key, const void *value) {
    size_t es = entry_size(ht);
    uint32_t hash = fnv1(key, ht->key_size);
    size_t idx = hash & (ht->capacity - 1);
    int probe = 0;
    char key_buf[ht->key_size], value_buf[ht->value_size];
    memcpy(key_buf, key, ht->key_size);
    memcpy(value_buf, value, ht->value_size);
    while (1) {
        Entry *e = (Entry *)((char*)ht->entries + idx * es);
        if (e->dist == -1) {
            e->dist = probe;
            memcpy(entry_key(e, ht->key_size), key_buf, ht->key_size);
            memcpy(entry_value(e, ht->key_size), value_buf, ht->value_size);
            ht->size++;
            return;
        }
        if (memcmp(entry_key(e, ht->key_size), key_buf, ht->key_size) == 0) {
            memcpy(entry_value(e, ht->key_size), value_buf, ht->value_size);
            return;
        }
        if (e->dist < probe) {
            int tmp_dist = e->dist;
            char temp_key[ht->key_size], temp_value[ht->value_size];
            memcpy(temp_key, entry_key(e, ht->key_size), ht->key_size);
            memcpy(temp_value, entry_value(e, ht->key_size), ht->value_size);
            e->dist = probe;
            memcpy(entry_key(e, ht->key_size), key_buf, ht->key_size);
            memcpy(entry_value(e, ht->key_size), value_buf, ht->value_size);
            memcpy(key_buf, temp_key, ht->key_size);
            memcpy(value_buf, temp_value, ht->value_size);
            probe = tmp_dist;
        }
        idx = (idx + 1) & (ht->capacity - 1);
        probe++;
    }
}

/* Initialize the table with a requested initial capacity (will be rounded up) */
int ht_init(HashTable *ht, size_t key_size, size_t value_size) {
    ht->key_size = key_size;
    ht->value_size = value_size;
    ht->capacity = next_power_of_two(INITIAL_CAPACITY);
    ht->size = 0;
    size_t es = entry_size(ht);
    ht->entries = malloc(ht->capacity * es);
    if (!ht->entries)
        return 1;
    for (size_t i = 0; i < ht->capacity; i++) {
        Entry *e = (Entry *)((char*)ht->entries + i * es);
        e->dist = -1;
    }
    return 0;
}

/* Free table resources */
int ht_destroy(HashTable *ht) {
    free(ht->entries);
    ht->entries = NULL;
    ht->capacity = ht->size = 0;
    return 0;
}

/* Reset table: zeros out everything */
int ht_reset(HashTable *ht) {
    if (!ht || !ht->entries)
        return 1;
    size_t es = entry_size(ht);
    memset(ht->entries, 0, ht->capacity * es);
    for (size_t i = 0; i < ht->capacity; i++) {
        Entry *e = (Entry *)((char*)ht->entries + i * es);
        e->dist = -1;
    }
    ht->size = 0;
    return 0;
}

/* Resize table to new_capacity and reinsert all entries */
int ht_resize(HashTable *ht, size_t new_capacity) {
    size_t old_capacity = ht->capacity;
    size_t es_old = entry_size(ht);
    Entry *old_entries = ht->entries;
    
    ht->capacity = next_power_of_two(new_capacity);
    ht->size = 0;
    size_t es = entry_size(ht);
    ht->entries = malloc(ht->capacity * es);
    if (!ht->entries) {
        ht->entries = old_entries;
        ht->capacity = old_capacity;
        return 1;
    }
    for (size_t i = 0; i < ht->capacity; i++) {
        Entry *e = (Entry *)((char*)ht->entries + i * es);
        e->dist = -1;
    }
    for (size_t i = 0; i < old_capacity; i++) {
        Entry *e = (Entry *)((char*)old_entries + i * es_old);
        if (e->dist >= 0) {
            ht_reinsert(ht, entry_key(e, ht->key_size), entry_value(e, ht->key_size));
        }
    }
    free(old_entries);
    return 0;
}

/* Insert key/value pair into the table.
   If key already exists its value is updated. */
int ht_insert(HashTable *ht, const void *key, const void *value) {
    if ((double)(ht->size + 1) > ht->capacity * LOAD_FACTOR)
        if (ht_resize(ht, ht->capacity * 2))
            return 1;
    size_t es = entry_size(ht);
    uint32_t hash = fnv1(key, ht->key_size);
    size_t idx = hash & (ht->capacity - 1);
    int probe = 0;
    char key_buf[ht->key_size], value_buf[ht->value_size];
    memcpy(key_buf, key, ht->key_size);
    memcpy(value_buf, value, ht->value_size);
    while (1) {
        Entry *e = (Entry *)((char*)ht->entries + idx * es);
        if (e->dist == -1) {
            e->dist = probe;
            memcpy(entry_key(e, ht->key_size), key_buf, ht->key_size);
            memcpy(entry_value(e, ht->key_size), value_buf, ht->value_size);
            ht->size++;
            return 0;
        }
        if (memcmp(entry_key(e, ht->key_size), key_buf, ht->key_size) == 0) {
            memcpy(entry_value(e, ht->key_size), value_buf, ht->value_size);
            return 0;
        }
        if (e->dist < probe) {
            int tmp_dist = e->dist;
            char temp_key[ht->key_size], temp_value[ht->value_size];
            memcpy(temp_key, entry_key(e, ht->key_size), ht->key_size);
            memcpy(temp_value, entry_value(e, ht->key_size), ht->value_size);
            e->dist = probe;
            memcpy(entry_key(e, ht->key_size), key_buf, ht->key_size);
            memcpy(entry_value(e, ht->key_size), value_buf, ht->value_size);
            memcpy(key_buf, temp_key, ht->key_size);
            memcpy(value_buf, temp_value, ht->value_size);
            probe = tmp_dist;
        }
        idx = (idx + 1) & (ht->capacity - 1);
        probe++;
    }
}

/* Look up a key. Returns pointer to the stored value or NULL if not found. */
void *ht_find(HashTable *ht, const void *key) {
    size_t es = entry_size(ht);
    uint32_t hash = fnv1(key, ht->key_size);
    size_t idx = hash & (ht->capacity - 1);
    int probe = 0;
    while (1) {
        Entry *e = (Entry *)((char*)ht->entries + idx * es);
        if (e->dist == -1 || probe > e->dist)
            return NULL;
        if (memcmp(entry_key(e, ht->key_size), key, ht->key_size) == 0)
            return entry_value(e, ht->key_size);
        idx = (idx + 1) & (ht->capacity - 1);
        probe++;
    }
}

/* Delete a key from the table */
int ht_delete(HashTable *ht, const void *key) {
    size_t es = entry_size(ht);
    uint32_t hash = fnv1(key, ht->key_size);
    size_t idx = hash & (ht->capacity - 1);
    int probe = 0;
    while (1) {
        Entry *e = (Entry *)((char*)ht->entries + idx * es);
        if (e->dist == -1 || probe > e->dist)
            return 0; // key not found is not an error
        if (memcmp(entry_key(e, ht->key_size), key, ht->key_size) == 0) {
            e->dist = -1;
            ht->size--;
            size_t hole = idx;
            size_t next = (idx + 1) & (ht->capacity - 1);
            while (1) {
                Entry *ne = (Entry *)((char*)ht->entries + next * es);
                if (ne->dist <= 0)
                    break;
                memcpy((char*)ht->entries + hole * es, ne, es);
                ((Entry *)((char*)ht->entries + hole * es))->dist--;
                hole = next;
                next = (next + 1) & (ht->capacity - 1);
            }
            ((Entry *)((char*)ht->entries + hole * es))->dist = -1;
            return 0;
        }
        idx = (idx + 1) & (ht->capacity - 1);
        probe++;
    }
}
#endif // HT_IMPLEMENTATION
