#pragma once
#define DEBUG_MASTER 1

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

#include <grpc++/grpc++.h>

using masterworker::map_reduce;
using masterworker::filedata;
using masterworker::map_data_in;
using masterworker::map_data_out;
using masterworker::reduce_data_in;
using masterworker::reduce_data_out;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
    MapReduceSpec mr_spec;
    std::vector<FileShard> file_shards;
    std::map<int, int> mappers;
    std::map<int, int> reducers;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
  this->mr_spec = mr_spec;
  this->file_shards = file_shards;

#if DEBUG_MASTER
	for (auto addr : this->mr_spec.worker_addrs)
	  std::cout << "IP:Port " << addr << "\n";
	for (auto& shard : this->file_shards) {
    for (auto& file : shard.filedata) {
	    std::cout << file.name << "(";
      std::cout << file.first << "|";
      std::cout << file.last << ")\n";
    }
  }
#endif
}

class MapReduceHandler {
  public:
    MapReduceSpec mr_spec;
    std::string addr;
    std::shared_ptr<masterworker::map_reduce::Stub>service_stub;
    MapReduceHandler(std::string addr, const MapReduceSpec& mr_spec) {
      this->addr = addr;
      this->mr_spec = mr_spec;
      this->service_stub = masterworker::map_reduce::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    }
    int handle_map(std::vector<fileinfo>);
};

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
#if DEBUG_MASTER
  std::cout << "Running Master" << std::endl;
#endif
	return true;
}