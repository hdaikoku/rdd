# RDD++

A C++ implementation of [RDD](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf) (Resilient Distributed Datasets).

## Requirements ##

* clang or gcc/g++ 4.8+
* CMake 2.8+
* [jubitus-msgpack-rpc](https://github.com/jubatus/jubatus-msgpack-rpc/tree/master/cpp)

## Operators Currently Available ##

* MapReduce

## How to Build ##

```
$ cmake .
$ make
```

## Usage ##

1. Start slave(s)
```
$ ./slave/RDDSlave [job_port] [data_port]
```
2.  Start your master application (e.g. WordCount)
```
$ ./examples/WordCount [path_to_slaves.conf] [path_to_text_file] [path_to_Mapper.so] [path_to_Reducer.so]
```

