#include <chrono>
#include <cmath>
#include <iostream>
#include <time.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"


std::string gen_random(const int len) {
  static const char alphanum[] =
      "0123456789";
      // "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      // "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return tmp_s;
}

int write_data(leveldb::DB* db, int num_megabytes, int key_size) {
  for (int i = 0; i < num_megabytes * (1024*1024/key_size); i++) {
    std::string value = gen_random(key_size);
    leveldb::Status status =
        db->Put(leveldb::WriteOptions(), leveldb::Slice(value), "");
    if (!status.ok()) {
      std::cout << "oops" << std::endl;
      std::cout << status.ToString();
      return -1;
    }
  }
  return 0;
}

int read_data(leveldb::DB* db, int num_entries, int key_size, leveldb::ReadOptions read_options) {
  for (int i = 0; i < num_entries; i++) {
    std::string value = gen_random(key_size);
    std::string result;
    leveldb::Status status =
        db->Get(read_options, leveldb::Slice(value), &result);
    if (!(status.ok() || status.IsNotFound())) {
      std::cout << "oops" << std::endl;
      std::cout << status.ToString();
      return -1;
    }
    if (status.ok()) {
      std::cout << "actually found, this should not happen." << std::endl;
    }
  }
  return 0;
}

int main(int argc, char** argv) {
  
  
  leveldb::Options options;
  int key_size = 128;
  int num_megabytes_to_write = 2048;
  // options.compression = leveldb::kNoCompression;
  // options.block_cache = leveldb::NewLRUCache(0);
  options.filter_policy = leveldb::NewBloomFilterPolicy(4);
  int write_seed = 42;
  int read_seed = write_seed + 1;
  leveldb::ReadOptions read_options;
  read_options.fill_cache = false;

  std::string db_name = "/tmp/default_db_4bpk";

  leveldb::DB* db;
  options.create_if_missing = true;

  leveldb::Status status = leveldb::DB::Open(options, db_name, &db);

  srand(write_seed);
  int result = write_data(db, num_megabytes_to_write, key_size);

  
  return 0;
}