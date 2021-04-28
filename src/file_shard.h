#pragma once
#define DEBUG_SHARD 0

#include <vector>
#include <iostream>
#include <fstream>

#include "mapreduce_spec.h"

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct fileinfo
{
  std::string name;
	size_t first;
  size_t last;
  size_t size;
};

struct FileShard {
  std::vector<fileinfo> filedata;
};

// <\brief> Helper function to get the filesize of a file.
// <\param> filename : the filename
static std::ifstream::pos_type filesize(std::string filename)
{
    std::ifstream file(filename, std::ifstream::binary | std::ifstream::ate);
    file.seekg(0, std::ios::end);
    return file.tellg();
}

// <\brief> Helper function to slightly adjust the offset so that they do not bisect words.
// <\param> fileShards : input fileShard vector.
static void _sync_to_newlines(std::vector<FileShard>& fileShards) {
  std::string tmp;
  int pos = 0;
  for (auto& shard : fileShards) {
    for (auto& file_info : shard.filedata) {
      std::ifstream file(file_info.name, std::ifstream::binary | std::ifstream::ate);
      file.seekg(file_info.first, file.beg);
      std::getline(file, tmp);
      pos = file.tellg();
      file_info.first = file_info.first == 0 ? file_info.first : pos;
      file.seekg(file_info.last, file.beg);
      std::getline(file, tmp);
      pos = file.tellg();
      file_info.last = pos > 0 ? pos : file_info.size;
    }
  }
}

// <\brief> Sharding implementation
// <\param> mr_spec : Specification structure built from configuration file
// <\param> fileShards : Output file shard vector
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
  std::map<std::string, int> filesizes;

  for (auto filename : mr_spec.input_files) {
    size_t input_size = filesize(filename);
    filesizes.insert(std::pair<std::string, int>(filename, input_size));
#if DEBUG_SHARD
    std::cout << filename << "|" << input_size << std::endl;
    std::cout << "n_chunks: " << mr_spec.n_chunks << std::endl;
    std::cout << "chunk_size: " << mr_spec.chunk_size << std::endl;
#endif
  }

  size_t first = 0;
  size_t last = 0;
  size_t carry = 0;
  size_t partition_size = 0;
  size_t diff;
  FileShard file_shard;

  for (auto filesize : filesizes) {
    size_t bytes_left = filesize.second;
    while (bytes_left > 0) {
      fileinfo file_info;
      file_info.name = filesize.first;
      file_info.size = filesize.second;
      file_info.first = first;

      if (carry != 0 && carry < bytes_left)
        diff = carry;
      else if (mr_spec.chunk_size < bytes_left) {
        diff = mr_spec.chunk_size;
      }
      else {
        diff = bytes_left;
        carry = mr_spec.chunk_size - bytes_left;
      }

      last = file_info.first + diff;
      file_info.last = last;
      file_shard.filedata.push_back(file_info);
      bytes_left -= diff;

      partition_size += diff;
      if (partition_size >= mr_spec.chunk_size) {
#if DEBUG_SHARD
        std::cout << file_info.name << "(" << file_info.first;
        std::cout << "|" << file_info.last << ")" << std::endl;
#endif
        fileShards.push_back(file_shard);
        file_shard.filedata.clear();
      }

      if (bytes_left == 0)
        first = 0;
      else
        first = filesize.second - bytes_left;
    }
  }
  _sync_to_newlines(fileShards);
  return true;
}
