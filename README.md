# C++RDD 

A C++ implementation of [RDD](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf) (Resilient Distributed Datasets).

## Requirements ##

* gcc/g++ 4.8 or above
* CMake 2.8 or above
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
$ ./SlaveRDD [job_port] [data_port]
```
2.  Start master
```
$ ./MasterRDD [path_to_slaves.conf] [...]
```

## Example MapReduce Task ##
