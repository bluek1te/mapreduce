#pragma once
#define DEBUG_SHARD 1

#include <vector>
#include "mapreduce_spec.h"

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct fileinfo
{
  std::string name;
	size_t first;
  size_t last;
};

struct FileShard {
  std::vector<fileinfo> filedata;
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
#if DEBUG_SHARD
  std::cout << "n_chunks: " << mr_spec.n_chunks << std::endl;
#endif
  std::vector<std::string> input_files = mr_spec.input_files;
  FileShard file_shard;
  size_t bytes_read = 0;
  while (!input_files.empty()) {
    if (bytes_read > mr_spec.chunk_size) {
      fileShards.push_back(file_shard);
#if DEBUG_SHARD
      for (auto fileinfo : file_shard.filedata) {
        std::cout << fileinfo.name << "(" << fileinfo.first << "|" << fileinfo.last << ")" << std::endl;
      }
#endif
      file_shard.filedata.clear();
    }

    ifstream ;
    strm.open ( ... );
    strm.seekg (x);

  }
	return true;
}
