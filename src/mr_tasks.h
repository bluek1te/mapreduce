#pragma once
#define DEBUG_TASKS 0

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <stdio.h>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

  /* DON'T change this function's signature */
  BaseMapperInternal();

  /* DON'T change this function's signature */
  void emit(const std::string& key, const std::string& val);

  /* NOW you can add below, data members and member functions as per the need of your implementation*/
  size_t n_outputs;
  size_t mapper_id;
  size_t index;
  size_t counter;
  size_t flush_limit;
  std::string out_dir;
  std::vector<std::ofstream> output_files;
  void cleanup_files(void);
  void create_file_handles(void);
  void initialize_file_index(void);
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

inline void BaseMapperInternal::create_file_handles() {
#if DEBUG_TASKS
  std::cout << "Creating File Handles in directory: " + this->out_dir + "\n";
#endif
  for (int i = 0; i < this->n_outputs; i++) {
  this->output_files.push_back(std::ofstream{"interm/" + std::to_string(this->mapper_id) + "_" + 
      std::to_string(i) + ".txt", std::ios::binary | std::ios::ate});
  }
  this->flush_limit = this->n_outputs * 5;
}

inline void BaseMapperInternal::initialize_file_index(){
  this->index = 0;
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
  this->counter++;
  if (this->counter > this->flush_limit) {
    for (int i = 0; i < this->n_outputs; i++) 
      //this->output_files[i].flush();
    this->counter = 0;
  }
  this->output_files[this->index] << key << " " << val << std::endl;
  this->output_files[this->index].flush();
  this->index++;
  if (index >= this->n_outputs)
    index = 0;
}




/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();
    ~BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		size_t n_mappers;
    size_t reducer_id;
    size_t counter;
    std::string out_dir;
    std::ofstream output_file;

    void create_file_handle();
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
  
}

inline BaseReducerInternal::~BaseReducerInternal() {
  this->output_file.close();
}

inline void BaseReducerInternal::create_file_handle() {
  this->output_file = std::ofstream{this->out_dir + "/" + std::to_string(this->reducer_id) + "_output.txt", std::ios::binary};
  this->counter = 0;
}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
  this->counter++;
  if (this->counter > 5) {
    this->counter = 0;
  }
  this->output_file << key << " " << val << std::endl;
}
