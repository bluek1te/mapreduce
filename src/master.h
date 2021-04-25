#pragma once

#define DEBUG 0

#include <vector>
#include <grpc++/grpc++.h>
#include <sstream>
#include <future>
#include <unistd.h>

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

using masterworker::map_reduce;
using masterworker::shard;
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
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		size_t num_workers;
		std::vector<std::string> workers;
		std::vector<std::string> filenames;
    std::vector<std::string> node_shards;
		std::string out_dir;
		std::string user_id;
		std::map<int, int> mappers;
		std::map<int, int> reducers;
};

static void split_str_into_vector(std::vector<std::string>* io_vector, std::string input_str)
{
	std::stringstream s_stream(input_str);
	while(s_stream.good()) {
		std::string substring;
		getline(s_stream, substring, ',');
		io_vector->push_back(substring);
	}
}

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	this->num_workers = std::stoi(mr_spec.configs.find("n_workers")->second);
	split_str_into_vector(&(this->workers), mr_spec.configs.find("worker_ipaddr_ports")->second);
	for(auto elem : mr_spec.configs)
	{
		std::cout << elem.first << " " << elem.second << std::endl;
	}
	this->filenames = mr_spec.inputFiles;
  this->node_shards = mr_spec.shards;
	this->out_dir = mr_spec.configs.find("output_dir")->second;
	this->user_id = mr_spec.configs.find("user_id")->second;
}

class MapReduce {
public:
	std::string user_id;
	std::string out_dir;
	MapReduce(std::string address, 
		std::string user_id,
		std::string out_dir)
	{
		this->user_id = user_id;
		this->out_dir = out_dir;
		this->create_stub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
	}
	void create_stub(std::shared_ptr<Channel> channel);
	std::shared_ptr<masterworker::map_reduce::Stub>service_stub;
	int map(std::string my_shards, std::vector<std::string>* output_paths)
	{
		map_data_in req;
		req.set_id(this->user_id);
		req.set_out_dir(this->out_dir);
		std::vector<std::string> shards;
		split_str_into_vector(&shards, my_shards);
    
		for(auto elem : shards)
		{
			shard* input_shard = req.add_file_shards();
			std::vector<std::string> file_info;
			std::stringstream ss(elem);
			while(ss.good()) {
				std::string substring;
				getline(ss, substring, ' ');
				file_info.push_back(substring);
			}
			input_shard->set_file_name(file_info[0]);
      input_shard->set_first(stoi(file_info[1]));
      input_shard->set_last(stoi(file_info[2]));
#if DEBUG
      std::cout << "Setting File Name to: " << file_info[0] << std::endl;
      std::cout << "Setting File Offset 1 to: " << file_info[1] << std::endl;
      std::cout << "Setting File Offset 2 to: " << file_info[2] << std::endl;
#endif
		}
    return 0;
    map_data_out reply;
    ClientContext ctx;
    Status status;
    
    this->service_stub->map(&ctx, req, &reply);
		
    for (const std::string& path : reply.map_paths())
      output_paths->push_back(path);
    std::cout << "Returning" << std::endl;
    return 0;
	}
};

void MapReduce::create_stub(std::shared_ptr<Channel> channel) {
	this->service_stub = masterworker::map_reduce::NewStub(channel);
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
  std::vector<std::future<int>*> rets;
  std::vector<MapReduce*> map_reducers;
  std::vector<std::vector<std::string>*> ret_path_lists;

  size_t i = 0;
  size_t worker_index = 0;

  /*for (int i = 8; i < 9; i++) {
    MapReduce* map_reducer = new MapReduce(this->workers[0], this->user_id, this->out_dir);
    std::vector<std::string>* ret_path_list = new std::vector<std::string>;
    std::cout << i << std::endl;
    std::cout << this->node_shards[i] << std::endl;
    auto fut = std::async(&MapReduce::map, map_reducer, this->node_shards[i], ret_path_list);
  }*/
  
  // Launch synchronous gRPC calls asynchronously (Lazy so we don't have to mess w completion queue)
  for (auto shard : this->node_shards) {
#if DEBUG
    std::cout << i << std::endl;
    std::cout << "shard: " <<  shard << std:: endl;
    std::cout << "worker index: " << worker_index << std::endl;
#endif
    MapReduce* map_reducer = new MapReduce(this->workers[0], this->user_id, this->out_dir);
    map_reducers.push_back(map_reducer);
    std::vector<std::string>* ret_path_list = new std::vector<std::string>;
    ret_path_lists.push_back(ret_path_list);
    std::future<int>* ret = new std::future<int>;
    *ret = std::async(&MapReduce::map, map_reducer, shard, ret_path_list);
    rets.push_back(ret);
    i++;
    worker_index++;
    if (worker_index >= this->num_workers)
      worker_index = 0;
  }

  // Block for async calls to return
  for (auto ret : rets) {
    ret->get();
  }

	return true;
}