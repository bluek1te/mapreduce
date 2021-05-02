CS-6210 Project 4
Richard Gaitan and Phillip Tran

# cs6210Project4
MapReduce Infrastructure

## Project Instructions

[Project Description](description.md)

[Code walk through](structure.md)


### How to setup the project  
To build, do the following (from the root of the project directory): 
    1) mkdir build && cd build
    2) cmake ..
    3) make -j

The build process creates two applications: worker (mr_worker) and master (mr_demo). Both the worker and master applications 
are run from the build/bin directory created when cmake finishes building the applications.

  1) The workers are run with the command: ./mr_worker <ipaddr:port> &
  2) The master application is run with ./mrdemo <location of config.ini>
  
In addition, the input files need to be copied over to the build/bin directory and saved under a folder called input. The initial set
of input files are located in the test/input directory if navigating from the root of the repository.


### Summary
The purpose of this project is to implement a map/reduce framework leveraging the gRPC library. This framework is designed to take 
as input text files that will then be divided into "shards" that will serve as the unit of computation for the mappers. The mappers 
will then generate intermediate files that the reducers will consume. The reducers will reduce those intermediate files into output 
files that are the final product for the user.

### Config File
The configuration file config.ini controls the behavior of both the workers and the master application. As stated in the 
structure.md file, cmake` creates a make rule to install a symbolic link to this file into the build/bin folder. The user
can edit this file to configure several options described below:

    - n_wokers            - specifies the number of active workers in the framework. This effective controls the parallelism of
                            the mappers and reducers.
    - worker_ipaddr_ports - specifies the ip address and port number where the workers are listening for tasks.
    - input_files         - details the input files used for parsing. The path specified uses the build/bin directory the starting
                            location.
    - output_dir          - specifies the location of where the ouput files are written. The path specified uses the build/bin
                            directory as starting location.
    - n_output_files      - specifies the number of output files generated, also controls number of reducers spawned
    - map_kilobytes       - specifies the shard chunk size 
    - user_id             - specifies the user id used to register with the task factory to generate mappers/reducers


### Theory of Operation
The project is divided into two sections that work together to implement all of the functionality necessary: worker (mr_worker) 
and master (mrdemo). 

#### Worker
From the perspective of the worker, the framework leverages the gRPC library as a means to call map or reduce remotely from the master 
as it assigns tasking to either a mapper or reducer. The worker instantiates a MapReduceImpl object that functions as a gRPC server
and register a user using a specified "user_id " in the configuration file, config.ini. The worker will wait for RPC calls from the 
master and service the RPC request. It has both a map_impl or reduce_impl method that it can access when servicing an RPC call. The 
method invoked depends on the RPC stub received. The actual mapper or reducer is obtained through a task factory that will return the 
appropriate worker based on which method is called in the RPC stub.

#### Master
From the perspective of the master, the framework leverage the gRPC library to call either map or reduce from a worker as 
a local method call. The master parses the configuration file and creates the file shards based on the input files supplied,
the chunk size (determined by the "map_kilobytes" configuration parameter) and the total size in bytes of the input files.
The master will then create the mappers and assign each mapper a file shard asynchonously. The master will then check the 
return values for each of the mappers and will restart any tasks that failed to run successfully. Each mapper will take
a file shard "chunk" and map that text with <key,value> pair and write this out to an intermediate file saved in the interm
directory in the build/bin directory.  After all mapper have successfully returned, the master will spawn reducers asynchronously 
to service the intermeidate files produced by the mappers. The number of reducers instantiated is determined by the number of output
files specified by the user ("n_output_files" configuration parameter). The master will then check each reducer for status and
respawn any reducers that fail once again and wait for all reducers to successfully complete.


### Division of Work
Richard Gaitan was responsible for developing the file sharding algorithm as well as parsing the configuration file, help
troubleshoot issues, help write the map/reduce handler implementation objects, and write the documentation for the project.

Phillip Tran was responsible for intergrating the map/reduce algorithms, develop the asynchronous gRPC calls, develop the
gRPC stubs and write the map/reduce handler implementation objects, and write the documentation for the project.

### Resources Used
https://grpc.io/docs/languages/cpp/quickstart/


