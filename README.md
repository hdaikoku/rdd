# RDD++

A C++ implementation of [RDD](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf) (Resilient Distributed Datasets).

## Requirements ##

* clang or gcc/g++ 4.8+ or Intel Compiler
* CMake 2.8+
* [jubatus-msgpack-rpc](https://github.com/jubatus/jubatus-msgpack-rpc/tree/master/cpp)
* [Intel Threading Building Blocks](https://www.threadingbuildingblocks.org/)
* [google-sparsehash](https://github.com/sparsehash/sparsehash)

## Operators Currently Available ##

* MapReduce

## How to Build ##
    
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

## Usage ##

1. Start worker(s)

    ```
    $ ./bin/RDDWorker [job_port]
    ```
    
2.  Start your master application (e.g. WordCount)

    ```
    $ ./bin/WordCount [path_to_workers.conf] [path_to_text_file] [path_to_Mapper.so] [path_to_Reducer.so]
    ```
