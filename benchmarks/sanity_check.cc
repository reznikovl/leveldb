#include <chrono>
#include <cmath>
#include <iostream>
#include <time.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"

std::string gen_random(const int len) {
  static const char alphanum[] = "0123456789";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return tmp_s;
}

/**
 * @brief Writes approximately the given number of megabytes of data
 *
 * @param db database to write to
 * @param num_megabytes how many megabytes to write
 * @param key_size what size key to generate (in bytes)
 * @return int 0 if successful, -1 otherwise
 */
int write_data(leveldb::DB* db, int num_megabytes, int key_size) {
  for (int i = 0; i < num_megabytes * (1024 * 1024 / key_size); i++) {
    std::string value = std::to_string(i);
    leveldb::Status status =
        db->Put(leveldb::WriteOptions(), leveldb::Slice(value), "test");
    if (!status.ok()) {
      std::cout << "oops" << std::endl;
      std::cout << status.ToString();
      return -1;
    }
  }
  return 0;
}

/**
 * @brief Reads num_entries entries of key size key_size
 *
 * @return 0 if successful, -1 otherwise
 */
int read_data(leveldb::DB* db, int num_entries, int key_size) {
  for (int i = 0; i < num_entries; i++) {
    std::string value = gen_random(key_size);
    std::string result;
    leveldb::Status status =
        db->Get(leveldb::ReadOptions(), leveldb::Slice(value), &result);
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
  
  
}