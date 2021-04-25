#pragma once

#include <vector>
#include <grpc++/grpc++.h>
#include <sstream>

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
		std::vector<std::string> filedata;
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
	this->filedata = mr_spec.inputFiles;
	this->out_dir = mr_spec.configs.find("output_dir")->second;
	this->user_id = mr_spec.configs.find("user_id")->second;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	return true;
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
protected:
	std::unique_ptr<masterworker::map_reduce::Stub>service_stub;
	int map(std::string shards_str)
	{
		map_data_in req;
		req.set_id(this->user_id);
		req.set_out_dir(this->out_dir);
		std::vector<std::string> shards;
		split_str_into_vector(&shards, shards_str);
		return 0;
	}
};

void MapReduce::create_stub(std::shared_ptr<Channel> channel) {
	this->service_stub = masterworker::map_reduce::NewStub(channel);
}