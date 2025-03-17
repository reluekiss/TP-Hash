# tp_dtable

**tp_dtable** is a header‑only C library that implements a dynamically resized tiny‑pointer dereference table. It reserves a large virtual memory region using `mmap` and grows the "active" portion in place without needing to rehash or reallocate existing items. The library supports constant‑time (amortized) insertion, lookup, and deletion operations, and includes a `dt_reset()` function to clear the table without unmapping memory.

This is meant to be a basic implementation of the proposal given in [this](https://arxiv.org/abs/2111.12800) arxiv paper, and has no guarentee of portability, reliability or usage outside of the given example.

## Features

- **Dynamic Resizing**: Reserves memory for up to 1M slots (by default) and grows the active capacity as needed.
- **Tiny Pointers**: Each inserted key/value pair is stored using a "tiny pointer" (an offset within a fixed‑size bucket).
- **Memory Management via mmap**: Uses `mmap` for allocation and is as such not portable to non Linux systems for now.
- **API Functions**:
  - `dt_create()`: Create a new table.
  - `dt_destroy()`: Free all memory used by the table.
  - `dt_reset()`: Reset the table to an empty state (active capacity reset to the initial capacity) without unmapping memory.
  - `dt_insert()`: Insert a key/value pair.
  - `dt_lookup()`: Lookup a key.
  - `dt_delete()`: Delete a key.
  - `dt_active_memory_usage()`: Report active memory usage.
  - `hash_key()`: A simple helper hash function.

## Usage

### Including the Library

Place the header file (`tp_dtable.h`) in your project directory. In one source file, define `TP_DTABLE_IMPLEMENTATION` **before** including the header:

```c
#define TP_DTABLE_IMPLEMENTATION
#include "tp_dtable.h"
```

To run the example:
```console
$ cc -O3 -o example example.c
$ ./example
```
