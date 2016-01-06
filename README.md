# RDD++

A C++ implementation of [RDD](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf) (Resilient Distributed Datasets).

## Requirements ##

* clang or gcc/g++ 4.8+ or Intel Compiler
* CMake 2.8+
* [jubatus-msgpack-rpc](https://github.com/jubatus/jubatus-msgpack-rpc/tree/master/cpp)
* [Intel Threading Building Blocks](https://www.threadingbuildingblocks.org/)

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

## How to Try on Docker ##

1. Build image

    ```
    $ cd docker
    $ docker build -t rdd .
    ```

2. Start slaves

    ```
    $ docker-compose --x-networking scale slave=[num_of_slaves]
    ```

3. Edit slaves.conf to match with [num_of_slaves]

        docker_slave_1 50090
        docker_slave_2 50090
        .
        .
        .
        docker_slave_[num_of_slaves] 50090
    
4. copy the target text file as "word_count.txt"

    ```
    $ cp [target_text_file] ./word_count.txt
    ```

4. Start master

    ```
    $ docker-compose --x-networking up master
    ```

6. To see the outputs from slaves:

    ```
    $ docker-compose logs
    ```