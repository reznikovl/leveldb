#include <chrono>
#include <cmath>
#include <iostream>
#include <time.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"

double eval(long run_bits, long runs_entries) {
  return std::exp(run_bits / runs_entries * std::pow(std::log(2), 2) * -1);
}

double TrySwitch(long& run1_entries, long& run1_bits, long& run2_entries,
                 long& run2_bits, long delta, double R) {
  double R_new = R - eval(run1_bits, run1_entries) -
                 eval(run2_bits, run2_entries) +
                 eval(run1_bits + delta, run1_entries) +
                 eval(run2_bits - delta, run2_entries);

  if (R_new < R && run2_bits - delta > 0) {
    run1_bits += delta;
    run2_bits -= delta;
    return R_new;
  } else {
    return R;
  }
}

std::vector<long> run_algorithm_c(std::vector<long> entries_per_level,
                                  int key_size, int bits_per_entry_equivalent) {
  long total_entries = 0;
  for (auto& i : entries_per_level) {
    total_entries += i;
  }
  std::cout << "total bytes is " << total_entries * key_size
            << " which is entries: " << total_entries << std::endl;
  long delta = total_entries * bits_per_entry_equivalent;
  std::vector<long> runs_entries;
  std::vector<long> runs_bits;
  for (long i = 0; i < entries_per_level.size(); i++) {
    runs_entries.push_back(entries_per_level[i]);
    runs_bits.push_back(0);
  }
  runs_bits[0] = delta;
  double R = runs_entries.size() - 1 + eval(runs_bits[0], runs_entries[0]);

  while (delta >= 1) {
    double R_new = R;
    for (int i = 0; i < runs_entries.size(); i++) {
      for (int j = i + 1; j < runs_entries.size(); j++) {
        R_new = TrySwitch(runs_entries[i], runs_bits[i], runs_entries[j],
                          runs_bits[j], delta, R_new);
        R_new = TrySwitch(runs_entries[j], runs_bits[j], runs_entries[i],
                          runs_bits[i], delta, R_new);
      }
    }
    if (R_new == R) {
      delta /= 2;
    }
    R = R_new;
  }
  std::vector<long> result;
  for (int i = 0; i < runs_bits.size(); i++) {
    result.push_back(runs_bits[i] / entries_per_level[i]);
  }
  return result;
}

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
      std::cout << status.ToString();
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
  if (argc != 3) {
    std::cout << "Usage: ./final_benchmark SEED_DATABASE USE_MONKEY where VARS are each either 1 or 0." << std::endl;
    return -1;
  }
  
  leveldb::Options options;

  // other options to set:
  options.block_size = 4 * 1024;
  options.compression = leveldb::kNoCompression;
  int key_size = 1024;
  int num_megabytes_to_write = 1024;
  int bits_per_entry_filter = 1;



  bool seed_database = strcmp(argv[1], "1") == 0;
  bool use_monkey = strcmp(argv[2], "1") == 0;
  leveldb::DB* db;
  srand(time(nullptr));
  options.create_if_missing = true;
  
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  if (seed_database) {
    std::cout << "Seeding database..." << std::endl;
    int status = write_data(db, num_megabytes_to_write, key_size);
    if (status != 0) {
      std::cout << "Error seeding database." << std::endl;
      return -1;
    } else {
      std::cout << "Database seeding complete." << std::endl;
    }
  }

  //reopen db for bloom filter sizes
  delete db;
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  std::cout << "Calculating bloom filters..." << std::endl;

  std::vector<long> bytes_per_level_with_zeros = db->GetBytesPerLevel();
  std::vector<long> entries_per_level;
  for (long i = 0; i < bytes_per_level_with_zeros.size(); i++) {
    if (bytes_per_level_with_zeros[i] == 0) {
      break;
    }
    entries_per_level.push_back(bytes_per_level_with_zeros[i] / 8);
  }

  std::vector<long> bits_per_key_per_level;

  if (use_monkey) {
    std::cout << "Allocating bloom filters based on Monkey algo..." << std::endl;
    bits_per_key_per_level =
        run_algorithm_c(entries_per_level, key_size, bits_per_entry_filter);
  }
  else {
    // make vector of same length with just bits per entry
    for (int i = 0; i < entries_per_level.size(); i++) {
      bits_per_key_per_level.push_back(bits_per_entry_filter);
    }
  }          

  delete db;
  options.filter_policy = leveldb::NewBloomFilterPolicy(bits_per_key_per_level);
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  std::cout << "Forcing filters" << std::endl;
  db->ForceFilters();
  for (int i = 0; i < bits_per_key_per_level.size(); i++) {
    std::cout << "Level " << i << " bits per key is "
              << bits_per_key_per_level[i] << std::endl;
  }
  delete db;
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  std::cout << "Reading..." << std::endl;

  auto start = std::chrono::high_resolution_clock::now();
  read_data(db, 12000, key_size);
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Done. Took " << duration.count() << "ms" << std::endl;
  return 0;
}