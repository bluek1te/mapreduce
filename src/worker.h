#pragma once
#define DEBUG_WORKER 1

#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include <iostream>
#include <fstream>
#include <unistd.h>

#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerBuilder;

using masterworker::map_reduce;
using masterworker::map_data_in;
using masterworker::map_data_out;

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
        Status map_impl(ServerContext* ctx, const map_data_in* req, map_data_out* resp) {
          // std::cout << "Map Implementation, out_dir=" + req->out_dir() + "\n";
          std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(req->user_id());

          for (auto file_info : req->fileinfos_rpc()) {
#if DEBUG_WORKER
              std::cout << "ID: " << std::to_string(req->mapper_id()) << "-" << "Processing file " 
                << file_info.file_name() + "(" + std::to_string(file_info.first()) + "|" + std::to_string(file_info.last()) + ")\n";
#endif
              std::ifstream input_file {file_info.file_name(), std::ios::binary | std::ios::ate };
              size_t len = file_info.last() - file_info.first();
              char* out_buffer = new char[len];
              input_file.seekg(file_info.first(), std::ios::beg);
              input_file.read(out_buffer, len);
              delete[] out_buffer;
              
              std::cout << "n_output_files: " << req->n_output_files() << std::endl;
              mapper->impl_->n_outputs = req->n_output_files();
              mapper->impl_->mapper_id = req->mapper_id();
              mapper->impl_->out_dir = req->out_dir();
              mapper->impl_->create_file_handles();
              mapper->map(out_buffer);
          }
          
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
