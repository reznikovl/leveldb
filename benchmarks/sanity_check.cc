#include <chrono>
#include <cmath>
#include <iostream>
#include <time.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"

std::string gen_random(const int len) {
  static const char alphanum[] = "abcdefghijklmnopqrstuvwxyz";
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

std::vector<std::pair<leveldb::Slice, std::string>> read_range(
    leveldb::DB* db, leveldb::Slice v1, leveldb::Slice v2) {
  std::vector<std::pair<leveldb::Slice, std::string>> result;
  leveldb::Status status =
      db->GetRange(leveldb::ReadOptions(), v1, v2, &result);
  if (!(status.ok())) {
    std::cout << "oops" << std::endl;
    std::cout << status.ToString();
  }
  return result;
}

int main(int argc, char** argv) {
  
  leveldb::Options options;

  options.base_scaling_factor = 2;
  options.ratio_diff = 1.0 / 3.0;

  // other options to set:
  options.block_size = 4 * 1024;
  options.compression =
      leveldb::kNoCompression;  // changing compression will require alternate
                                // implementation of entries per level
//   int key_size = 128;
//   int num_megabytes_to_write = 512;
//   int bits_per_entry_filter = 2;

//   bool seed_database = strcmp(argv[1], "1") == 0;
//   bool use_monkey = strcmp(argv[2], "1") == 0;
  
//   srand(time(nullptr));

  leveldb::DB* db;
  options.create_if_missing = true;

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  for (int i = 100000; i > 0; i--) {
    leveldb::Status s = db->Put(leveldb::WriteOptions(), leveldb::Slice(std::to_string(i)), leveldb::Slice(std::to_string(i+1)));
    if (!s.ok()) {
        std::cout << "Oops" << std::endl;
    }
  }
  sleep(10);
  db->CompactLevel0Files();
  sleep(10);

  db->ForceFilters();

  for(int i = 100000; i > 0; i--) {
    std::string value;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), leveldb::Slice(std::to_string(i)), &value);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
    }
    if(value != std::to_string(i+1)) {
        std::cout << "oops2" << std::endl;
    }
  }
  std::cout << "We gucci?" << std::endl;
  return 0;
  

}