#pragma once

#define DEBUG 1

#include <grpc++/grpc++.h>
#include <iostream>
#include <fstream>
#include <mr_task_factory.h>

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
		std::string addr;
		std::unique_ptr<Server> server;

		class MapReduceImpl final : public map_reduce::Service {
		public:
			Status map_impl(ServerContext* ctx, const map_data_in* req, map_data_out* resp) {
#if DEBUG
				std::cout << "Mapping!" << std::endl;
#endif
				std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(req->id());
				
				for (auto shard : req->file_shards()) {

					int len = shard.last() - shard.first();
					char* out_buffer = new char[len];
	
					std::ifstream file;
					file.open(shard.file_name());
					file.seekg(shard.first(), std::ios::beg);
					file.read(out_buffer, len);

					delete[] out_buffer;

					std::size_t file_pos = shard.file_name().find_last_of("/");
					std::string file_name_stripped = shard.file_name().substr(file_pos, shard.file_name().length());

					std::string out_filepath = req->out_dir() 
												+ file_name_stripped + "_" 
												+ std::to_string(shard.first()) + "_" 
												+ std::to_string(shard.last());

					mapper->impl_->file_name = out_filepath;
#if DEBUG
					std::cout << "Mapping in for loop" << std::endl;
#endif
					mapper->map(out_buffer);
					resp->add_map_path(out_filepath);
				}

				return Status::OK;
			}
		};
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

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
