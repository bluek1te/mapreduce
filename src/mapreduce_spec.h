#pragma once
#define DEBUG_SPEC 0

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <cmath>
#include <sstream>
#include <sys/stat.h>

struct MapReduceSpec 
{
  std::string user_id;
  std::string output_dir;
  size_t n_chunks;
  size_t chunk_size;
  size_t n_workers;
  size_t n_output_files;
  std::vector<std::string> worker_addrs;
  std::vector<std::string> input_files;
};

inline void parse_multivalue_field(std::string field, std::vector<std::string>& container)
{
  std::stringstream stream(field);
    while(stream.good()) 
  {
    std::string substr;
    getline(stream, substr, ',');
    container.push_back(substr);
    }
}

inline int getTotalFileSize(std::vector<std::string>& files)
{
  long double totalSize = 0;
  for (auto file: files)
  {
    std::cout << "DEBUG: File: " << file << "\n";
    std::ifstream fileHandle(file, std::ios::binary);
    fileHandle.seekg(0, std::ios::end);
    std::cout << fileHandle.tellg() << "\n";
    totalSize += fileHandle.tellg();
  }

  std::cout << "DEBUG: Total File Size: " << totalSize << "\n";
  return totalSize;
}

inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) 
{
  std::ifstream configFile(config_filename);
  std::map<std::string, std::string> configs;
  if (!configFile.is_open())
    return false;
    
  // RG: Let's parse our config file and print out for good measure
  while(configFile)
  {
    std::string line = "";
    getline(configFile, line);

    if (line.length() < 1)
      continue;

    // RG: Calculate our delimiter position, then parse line
    int delimiter_position = line.find("=");
    std::string key  = line.substr(0, delimiter_position);
    std::string value = line.substr(delimiter_position+1, line.length());
    configs[key] = value;
  }

  configFile.close();

  std::cout << std::endl;
  parse_multivalue_field(configs["worker_ipaddr_ports"], mr_spec.worker_addrs);
  parse_multivalue_field(configs["input_files"], mr_spec.input_files);

  int totalFileSize = getTotalFileSize(mr_spec.input_files);
  int numOfShards = ceil(totalFileSize / (std::stoi(configs["map_kilobytes"]) * 1024));
  // std::cout << "DEBUG Shards size: " << mr_spec.configs["map_kilobytes"] << "\n";
  // std::cout << "DEBUG Number of shards: " << numOfShards << "\n";
  mr_spec.n_workers = std::stoi(configs["n_workers"]);
  mr_spec.n_output_files = std::stoi(configs["n_output_files"]);
  mr_spec.n_chunks = numOfShards;
  mr_spec.chunk_size = std::stoi(configs["map_kilobytes"]) * 1024;
  mr_spec.output_dir = configs["output_dir"];
  mr_spec.user_id = configs["user_id"];

  return true;
}

inline bool validate_mr_spec(const MapReduceSpec& mr_spec) 
{
  std::cout << mr_spec.output_dir << "\n";
  mkdir(const_cast<char *>(mr_spec.output_dir.c_str()), 0777);
  mkdir(const_cast<char *>("interm"), 0777);
#if DEBUG_SPEC
  for (auto addr : mr_spec.worker_addrs)
    std::cout << "IP:Port " << addr << "\n";

  for (auto& shard : mr_spec.shards)
    for (auto& file : shard) {
      std::cout << file.filename << "(";
      std::cout << file.first << "|";
      std::cout << file.last << ")\n";
    }
#endif
  return true;
}
