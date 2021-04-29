#pragma once
#define DEBUG_WORKER 1
#define DELETE_INTER 1

#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <vector>
#include <string>
#include <stdio.h>
#include <cstdio>

#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerBuilder;

using masterworker::map_reduce;
using masterworker::map_data_in;
using masterworker::map_data_out;
using masterworker::reduce_data_in;
using masterworker::reduce_data_out;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
  This is a big task for this project, will test your understanding of map reduce */
class Worker {

  public:
    /* DON'T change the function signature of this constructor */
    Worker(std::string ip_addr_port);

    /* DON'T change this function's signature */
    bool run();

  private:
    std::unique_ptr<Server> server;
    std::string addr;

    class MapReduceImpl final : public map_reduce::Service {
      public:
        // <\brief> Mapper implementation, refer to masterworker.proto for datatype definitions.
        // <\param> ctx : ServerContext stub for gRPC call.
        // <\param> req : Request input for gRPC call.
        // <\param> resp : Response output for gRPC call. (I don't really use this at all.)
        Status map_impl(ServerContext* ctx, const map_data_in* req, map_data_out* resp) {
          // std::cout << "Map Implementation, out_dir=" + req->out_dir() + "\n";
          // Get the mapper from the task factory.
          // The mapper holds the map function that is linked to the user id.
          std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(req->user_id());
          mapper->impl_->n_outputs = req->n_output_files();
          mapper->impl_->mapper_id = req->mapper_id();
          mapper->impl_->out_dir = req->out_dir();
          // Open file handles as part of the class object, so they stay open.
          // Opening and reopening file handles for each <key, val> pair takes forever.
          mapper->impl_->create_file_handles();
          mapper->impl_->initialize_file_index();
          size_t len = 0;

          // A shard can contain multiple file references. For each file in the shard, we read the length provided
          // to us by the offsets, starting at the beginning offset. This gets stored into a buffer and then passed to
          // the user's map function.
          std::string out_buffer;
          for (auto file_info : req->fileinfos_rpc()) {
#if DEBUG_WORKER
            std::cout << "ID: " << std::to_string(req->mapper_id()) << "-" << "Processing file " 
              << file_info.file_name() + "(" + std::to_string(file_info.first()) + "|" + std::to_string(file_info.last()) + ")\n";
#endif
            std::ifstream input_file {file_info.file_name(), std::ios::binary | std::ios::ate };
            len = file_info.last() - file_info.first();
            char* read_buffer = new char[len + 1];
            memset(read_buffer, 0, len + 1);
            input_file.seekg(file_info.first(), std::ios::beg);
            input_file.read(read_buffer, len);
            std::stringstream item(read_buffer);
            while(std::getline(item, out_buffer))
            {   
              mapper->map(out_buffer);
            }
  
            delete[] read_buffer;
          }
          std::cout << mapper->impl_->counter << std::endl;
          return Status::OK;
        }

        // <\brief> Reducer implementation, refer to masterworker.proto for datatype definitions.
        // <\param> ctx : ServerContext stub for gRPC call.
        // <\param> req : Request input for gRPC call.
        // <\param> resp : Response output for gRPC call. (I don't really use this at all.)
        Status reduce_impl(ServerContext* ctx, const reduce_data_in* req, reduce_data_out* resp) {
          // std::cout << "Reduce Implementation, out_dir=" + req->out_dir() + " n_mappers=" + std::to_string(req->n_mappers()) + "\n";
          std::map<std::string, std::vector<std::string>> tally;
          std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory(req->user_id());
          reducer->impl_->reducer_id = req->reducer_id();
          reducer->impl_->out_dir = req->out_dir();
          reducer->impl_->create_file_handle();
          std::string tmp;
          std::string token;
          // The reducers have to process an input from each mapper, so we loop through the number of mappers. The input file parsed
          // is named <mapper_id>_<reducer_id>.txt
          std::ofstream debug_file {"debug.txt", std::ios::binary};
          for (size_t i = 0; i < req->n_mappers(); i++) {
            std::string input_file_str = "interm/" + std::to_string(i) + "_" + std::to_string(req->reducer_id()) + ".txt";
            std::ifstream input_file {input_file_str, 
              std::ios::binary | std::ios::ate};
            std::cout << "Reduce: Processing " + input_file_str + "\n";
              input_file.seekg(0, std::ios::beg);
            // Parse line by line and create a map with <key, values> (note that "values" is plural here).
            // The intermediate buffers that get passed to the reducer function here, for example, will be.
            // <"potato" : "1", "3", "1", "5">, <"cat" : "1", "1", "1" ,"1">, ...
            while(getline(input_file, tmp)) {
              if (tmp.find_first_not_of(" ") == std::string::npos) // If it's only whitespace, skip.
                continue;
              std::stringstream item(tmp);
              getline(item, token, ' ');
              std::string key = token;
              getline(item, token, ' ');
              std::string val = token;
              auto iter = tally.find(key);
              if (iter != tally.end()) {
                iter->second.push_back(val);
              }
              else {
                std::vector<std::string> vals;
                tally.emplace(key, vals);
                tally.find(key)->second.push_back(val);
              }
            }
            
            input_file.close();
#if DELETE_INTER // Delete Intermediate Values. We have to do this because Gradescope scans the output director
                 // for completed files and cannot tell the difference between an intermediate file and a finished one. (Default == 1)
            // Have to pass this in as a cstring or C++ compiler will think you want a diff function..
            remove(input_file_str.c_str());
#endif
          }
          // Pass each pair to reduce function. Corellating output to previous input will be  <"potato" : "10">, <"cat" : "4">, ...
          for (auto const& pair : tally)
            reducer->reduce(pair.first, pair.second);
          
          reducer->impl_->output_file.close();
          return Status::OK;
        }
    };
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
  this->addr = ip_addr_port;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	ServerBuilder builder;
	MapReduceImpl service;
	builder.AddListeningPort(this->addr, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<Server> server {builder.BuildAndStart()};
	server->Wait();
	return true;
}
