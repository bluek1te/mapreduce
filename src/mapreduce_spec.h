#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <cmath>
#include<sstream>


/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec 
{
	std::map<std::string, std::string> configs;
  std::vector<std::string> inputFiles;
	std::vector<std::string> ipPorts;
	std::vector<std::string> shards;
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

inline void calculate_file_shards(const std::vector<std::string>& inputFiles, std::vector<std::string>& shards, const int shardSize)
{
  int shardSizeBytes = shardSize * 1024; // RG: Convert shard size to bytes instead
  int bytesRead = 0;
  int carryover = 0;
  std::string shardString = "";
  for (auto& file : inputFiles)
  {
	std::ifstream fileHandle(file, std::ios::binary);
	std::string line;
	int offset = 0;
	while (std::getline(fileHandle, line))
	{
	  bytesRead += line.length() + 1;
	  if (bytesRead > shardSizeBytes)
	  {
		if (carryover > 0)
		{
		  shardString += ",";
		}

		shardString += file + " " + std::to_string(offset) + " " + std::to_string(offset+bytesRead-carryover);
		const char* temp = shardString.c_str();
		shards.push_back(temp);

		// RG: Reset our state
		offset += bytesRead - carryover;
		bytesRead = 0;
		shardString = "";
		carryover = 0;
	  }
	}

    bytesRead -= 1; // RG: Need to substract one because last line will not have newline
	if (bytesRead > 0)
	{
		shardString = file + " " + std::to_string(offset) + " " + std::to_string(offset + bytesRead);
	  carryover = bytesRead;
	}

	fileHandle.close();
  }

  shards.push_back(shardString);
}

inline int getTotalFileSize(std::vector<std::string>& files)
{
	int totalSize = 0;
	for (auto file: files)
	{
		std::cout << "DEBUG: File: " << file << "\n";
		std::ifstream fileHandle(file, std::ios::binary);
		fileHandle.seekg(0, std::ios::end);
        totalSize += fileHandle.tellg();
	}

    std::cout << "DEBUG: Total File Size: " << totalSize << "\n";
	return totalSize;
}


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) 
{
	std::ifstream configFile(config_filename);
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
		mr_spec.configs[key] = value;
	}

	configFile.close();

  std::cout << std::endl;
	parse_multivalue_field(mr_spec.configs["worker_ipaddr_ports"], mr_spec.ipPorts);
	parse_multivalue_field(mr_spec.configs["input_files"], mr_spec.inputFiles);

  int totalFileSize = getTotalFileSize(mr_spec.inputFiles);
	int numOfShards = ceil(totalFileSize / (std::stoi(mr_spec.configs["map_kilobytes"]) * 1024));
	// std::cout << "DEBUG Shards size: " << mr_spec.configs["map_kilobytes"] << "\n";
	// std::cout << "DEBUG Number of shards: " << numOfShards << "\n";

	calculate_file_shards(mr_spec.inputFiles, mr_spec.shards, std::stoi(mr_spec.configs["map_kilobytes"]));
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) 
{
	// RG: TODO Not sure what to do here...investigate...
	for (auto it = mr_spec.configs.begin(); it != mr_spec.configs.end(); it++)
	{
		std::cout << "DEBUG: " << it->first << "\tValue: " << it->second << "\n";
	}

	for (auto file : mr_spec.inputFiles)
	  std::cout << "Input Files: " << file << "\n";

	for (auto ipPort : mr_spec.ipPorts)
	  std::cout << "IP:Port " << ipPort << "\n";

	for (auto& shard : mr_spec.shards)
	  std::cout << shard << "\n";

	return true;
}
