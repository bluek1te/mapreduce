#pragma once
#define DEBUG_MASTER 1

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <future>
#include <grpc++/grpc++.h>
#include <unistd.h>
#include <chrono>

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
      // std::cout << "Handling map for user " << this->mr_spec.user_id << " out_dir: " << this->mr_spec.output_dir << std::endl;
      map_data_in req;
      req.set_user_id(this->mr_spec.user_id);
      req.set_out_dir(this->mr_spec.output_dir);
      //std::cout << "Setting map id to " << mapper_id << std::endl;
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
      std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() +
        std::chrono::seconds(1000);
      ctx.set_deadline(deadline);
      Status status;

      status = this->service_stub->map_impl(&ctx, req, &reply);

      return status.ok(); 
    }

    int handle_reduce(size_t n_mappers, size_t reducer_id) {
      // std::cout << "Handling reduce for user " << this->mr_spec.user_id << ", out_dir: " << this->mr_spec.output_dir << std::endl;
      reduce_data_in req;
      req.set_user_id(this->mr_spec.user_id);
      req.set_out_dir(this->mr_spec.output_dir);
      req.set_reducer_id(reducer_id);
      req.set_n_mappers(n_mappers);

      reduce_data_out reply;
      ClientContext ctx;
      std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() +
        std::chrono::seconds(1000);
      ctx.set_deadline(deadline);
      Status status;

      status = this->service_stub->reduce_impl(&ctx, req, &reply);

      return status.ok(); 
    }
};

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
#if DEBUG_MASTER
  std::cout << "Running Master" << std::endl;
#endif
  // Launch mapper calls asynchronously. This is done by creating a handler object, and launching the handle_map
  // method using C++ futures. The return value of the future will be 1 on SUCCESS, 0 on FAILURE.
  std::vector<std::future<int>*> mapper_rets;
  std::vector<MapReduceHandler*> mappers;

  size_t worker_index = 0; // This is used because we can have less workers than mappers requested, so some workers may have
                           // to process more than one shard
  size_t mapper_index = 0; // Unique identifier for mapper
  for (auto shard : this->file_shards) {
    // Can probably turn this to a managed ptr.
    MapReduceHandler* mapper = new MapReduceHandler(this->mr_spec.worker_addrs[worker_index], this->mr_spec);
    mappers.push_back(mapper);
    std::future<int>* ret = new std::future<int>;
    *ret = std::async(&MapReduceHandler::handle_map, mapper, shard, mapper_index);
    mapper_rets.push_back(ret);
    mapper_index++;
    worker_index++;
    if (worker_index >= this->mr_spec.n_workers)
      worker_index = 0;
  }

  std::cout << "Mapper Ret Status: ";
  for (auto ret : mapper_rets) {
    int ret_val = ret->get();
    std::cout << "[" << ((ret_val) ? "O" : "X") << "]";
    delete ret;
  }
  std::cout << "\n";

  // Launch reducer calls asynchronously. Same gist as mappers.
  // The return value of the future will be 1 on SUCCESS, 0 on FAILURE.
  std::vector<std::future<int>*> reduce_rets;
  std::vector<MapReduceHandler*> reducers;
  worker_index = 0;
  for (int i = 0; i < this->mr_spec.n_output_files; i++) {
    std::future<int>* ret = new std::future<int>;
    MapReduceHandler* reducer = new MapReduceHandler(this->mr_spec.worker_addrs[worker_index], this->mr_spec);
    reducers.push_back(reducer);
    *ret = std::async(&MapReduceHandler::handle_reduce, reducer, this->file_shards.size(), i);
    reduce_rets.push_back(ret);
    worker_index++;
    if (worker_index >= this->mr_spec.n_workers)
      worker_index = 0;
  }

  std::cout << "Reducer Ret Status: ";
  for (auto ret : reduce_rets) {
    int ret_val = ret->get();
    std::cout << "[" << ((ret_val) ? "O" : "X") << "]";
    delete ret;
  }
  std::cout << "\n";
  
  return true;
}