#pragma once
#define DEBUG_MASTER 1

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <future>
#include <grpc++/grpc++.h>

using masterworker::map_reduce;
using masterworker::fileinfo_rpc;
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
    int handle_map(FileShard shard, size_t mapper_id) {
      // std::cout << "Handling map" << std::endl;
      map_data_in req;
      req.set_user_id(this->mr_spec.user_id);
      req.set_out_dir(this->mr_spec.output_dir);
      req.set_mapper_id(mapper_id);
      req.set_n_output_files(this->mr_spec.n_output_files);
      for (auto file_info : shard.filedata) {
        fileinfo_rpc* file_input = req.add_fileinfos_rpc();
        file_input->set_file_name(file_info.name);
        file_input->set_first(file_info.first);
        file_input->set_last(file_info.last);
        file_input->set_size(file_info.size);
      }

      map_data_out reply;
      ClientContext ctx;
      Status status;

      status = this->service_stub->map_impl(&ctx, req, &reply);
      std::cout << "Status " << status.ok() << std::endl;

      return 0;
    };
};

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
#if DEBUG_MASTER
  std::cout << "Running Master" << std::endl;
#endif
  // Create Output Directory if it doesn't exist
	struct stat info;
  if (stat("output", &info) != 0)
    system("mkdir -p output");

  std::vector<std::future<int>*> rets;
  std::vector<MapReduceHandler*> map_reducers;

  size_t worker_index = 0;
  size_t mapper_index = 0;
  for (auto shard : this->file_shards) {
    MapReduceHandler* map_reducer = new MapReduceHandler(this->mr_spec.worker_addrs[worker_index], this->mr_spec);
    map_reducers.push_back(map_reducer);
    std::future<int>* ret = new std::future<int>;
    // std::cout << "Launching async call\n";
    *ret = std::async(&MapReduceHandler::handle_map, map_reducer, shard, mapper_index);
    rets.push_back(ret);
    mapper_index++;
    worker_index++;
    if (worker_index >= this->mr_spec.n_workers)
      worker_index = 0;
  }

  for (auto ret : rets) {
    ret->get();
    delete ret;
  }
  
	return true;
}