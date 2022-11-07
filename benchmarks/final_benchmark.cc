#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include <iostream>
#include <chrono>
#include <cmath>
#include <time.h>

std::string gen_random(const int len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return tmp_s;
}

int write_data(leveldb::DB* db, int num_megabytes, int key_size) {
  for (int i = 0; i < num_megabytes * 1024; i++) {
    std::string value = gen_random(key_size);
    leveldb::Status status =
        db->Put(leveldb::WriteOptions(), leveldb::Slice(value), "");
    if (!status.ok()) {
      std::cout << "oops" << std::endl;
      return -1;
    }
  }
  return 0;
}

int read_data(leveldb::DB* db, int num_entries, int key_size) {
  for (int i = 0; i < num_entries; i++) {
    std::string value = gen_random(key_size);
    std::string result;
    leveldb::Status status =
        db->Get(leveldb::ReadOptions(), leveldb::Slice(value), &result);
    if (!(status.ok() || status.IsNotFound())) {
      std::cout << "oops" << std::endl;
      return -1;
    }
    if (status.ok()) {
      std::cout << "actually found" << std::endl;
    }
  }
  return 0;
}

int main(int argc, char **argv) {
    if(argc == 1) {
        std::cout << "Please pass 1 if database should be seeded or 0 otherwise.";
        return -1;
    }
    leveldb::DB* db;
    leveldb::Options options;
    srand(time(nullptr));

    options.create_if_missing = true;
    options.block_size = 1024 * 1024;
    options.compression = leveldb::kNoCompression;
    int key_size = 1024;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

    if (strcmp(argv[1], "1") == 0) {
        std::cout << "Seeding database..." << std::endl;
        int status = write_data(db, 1024, key_size);
        if(status != 0) {
            std::cout << "Error seeding database." << std::endl;
            return -1;
        }
        else {
            std::cout << "Database seeding complete." << std::endl;
        }
    }
    options.filter_policy =
        leveldb::NewBloomFilterPolicy(5);
    delete db;

    status = leveldb::DB::Open(options, "/tmp/testdb", &db);

    std::cout << "Reading..." << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    read_data(db, 12000, key_size);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "Done. Took "<< duration.count() << "ms" << std::endl;
    return 0;
}