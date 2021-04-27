#pragma once
#define DEBUG_TASKS 0

#include <string>
#include <iostream>
#include <fstream>
#include <vector>

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
    std::string out_dir;
	  std::vector<std::ofstream> output_files;
    void cleanup_files(void);
    void create_file_handles(void);
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

inline void BaseMapperInternal::create_file_handles() {
#if DEBUG_TASKS
  std::cout << "Creating File Handles in directory: " + this->out_dir + "\n";
#endif
	for (int i = 0; i < this->n_outputs; i++) {
		this->output_files.push_back(std::ofstream{this->out_dir + "/" + std::to_string(this->mapper_id) + "_" + 
      std::to_string(i) + ".txt", std::ios::binary | std::ios::ate});
	}
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
  static int index = 0;
  this->output_files[index] << key << " " << val << std::endl;
  index++;
  if (index >= this->n_outputs)
    index = 0;
}




/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		size_t n_mappers;
    size_t reducer_id;
    std::string out_dir;

};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
  
}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::ofstream output_file {out_dir + std::to_string(reducer_id), std::ios_base::app};
  std::cout << "insert!\n";
  output_file << key << " " << val;
}
