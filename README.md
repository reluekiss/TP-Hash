ok after a lot of bs i have made a decently fast hashtable namely ht.h
```console
$ cc -ggdb -O5 profiling.c -o profiling -lm
$ ./profiling 
Generic Hash Table Stress Test Completed:
  Avg insert: 203.285400 ns
  Avg lookup: 37.156100 ns
  Avg delete: 51.741000 ns
```
