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

std::vector<std::pair<std::string, std::string>> read_range2(
    leveldb::DB* db, std::string v1, std::string v2) {
  std::vector<std::pair<std::string, std::string>> result;
  auto it = db->NewIterator(leveldb::ReadOptions());
  for(it->Seek(leveldb::Slice(v1)); it->key().ToString() < v2; it->Next()) {
    if (std::stoi(it->key().ToString()) + 1 != std::stoi(it->value().ToString())) {
      std::cout << "bad key value pair" << std::endl;
      std::cout << "Key: " << it->key().ToString() << std::endl;
      std::cout << "Value: " << it->value().ToString() << std::endl;
    }
      result.push_back({it->key().ToString(), it->value().ToString()});
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

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb2", &db);

  for (int i = 1000000; i > 0; i--) {
    leveldb::Status s = db->Put(leveldb::WriteOptions(), leveldb::Slice(std::to_string(i)), leveldb::Slice(std::to_string(i+1)));
    if (!s.ok()) {
        std::cout << "Oops" << std::endl;
    }
  }
  sleep(3);
  db->CompactLevel0Files();
  sleep(3);
  delete db;
  std::vector<long> policy = {2, 0, 1, 1, 1, 1, 1};
  options.filter_policy = leveldb::NewBloomFilterPolicy(policy);
  leveldb::DB::Open(options, "/tmp/testdb2", &db);
  sleep(3);
  db->ForceFilters();
  sleep(3);
  

  for(int i = 1000001; i > 0; i--) {
    std::string value;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), leveldb::Slice(std::to_string(i)), &value);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        std::cout << "Error on key " << i << std::endl;
    }
    if(value != std::to_string(i+1)) {
        std::cout << "oops2" << std::endl;
        std::cout << "Error on key " << i << std::endl;
    }
  }
  std::cout << "We gucci?" << std::endl;
  

  // std::vector<std::pair<leveldb::Slice, std::string>> result;

  // auto s = db->GetRange(leveldb::ReadOptions(), leveldb::Slice("2"), leveldb::Slice("3"), &result);

  auto r = read_range2(db, "2", "23");

  std::cout << "Range of length " << r.size() << std::endl;
  std::cout << "First 5" << std::endl;

  for (int i = 0; i < std::min(r.size(), (size_t)5); i++) {
    std::cout << "Key " << r[i].first << ", Value " << r[i].second << std::endl;
  }
  std::cout << "Last 5" << std::endl;
  for (int i = r.size() - 1; i >= r.size() - 5; i--) {
    std::cout << "Key " << r[i].first << ", Value "
              << r[i].second << std::endl;
  }
  return 0;
}