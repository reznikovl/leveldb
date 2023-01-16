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

/**
 * @brief Runs Algorithm C from the Monkey paper
 *
 * @param entries_per_level vector of (unadjusted) approximate number of entries
 * seen per level
 * @param key_size size of the keys in bytes
 * @param bits_per_entry_equivalent average bits per key to use for bloom
 * filters
 * @return std::vector<long> optimal bloom filter allocation according to Monkey
 */
std::vector<long> run_algorithm_c(std::vector<long> entries_per_level,
                                  int key_size, int bits_per_entry_equivalent) {
  long total_entries = 0;
  for (auto& i : entries_per_level) {
    total_entries += i;
  }
  std::cout << "Total bytes found is " << total_entries * key_size << std::endl;
  long delta = total_entries * bits_per_entry_equivalent * 0.9;
  std::cout << "Adjusting for metadata, approx number of entries is "
            << delta / bits_per_entry_equivalent << std::endl;
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
    if (abs(R_new - R) < 0.000001) {  // fixed float error
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

int main(int argc, char** argv) {
  leveldb::Options options;

  options.base_scaling_factor = 2;
  options.ratio_diff = 1;

  // other options to set:
  options.block_size = 4 * 1024;
  options.compression =
      leveldb::kNoCompression;  // changing compression will require alternate
                                // implementation of entries per level
                                  int key_size = 3;
                                //   int num_megabytes_to_write = 512;
                                  int bits_per_entry_filter = 2;

    bool seed_database = strcmp(argv[1], "1") == 0;
    bool use_monkey = strcmp(argv[2], "1") == 0;

  //   srand(time(nullptr));

  leveldb::DB* db;
  options.create_if_missing = true;

  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  for (int i = 0; i < 1000000; i++) {
    leveldb::Status s =
        db->Put(leveldb::WriteOptions(), leveldb::Slice(std::to_string(i)),
                leveldb::Slice(std::to_string(i + 1)));
    if (!s.ok()) {
      std::cout << "Oops" << std::endl;
    }
  }
  sleep(10);
  db->CompactLevel0Files();
  sleep(10);
  std::vector<std::vector<long>> bytes_per_level_with_zeros =
      db->GetBytesPerRun();
  sleep(10);
  delete db;
  std::vector<long> entries_per_run;
  for (long i = 0; i < bytes_per_level_with_zeros.size(); i++) {
    for (int j = 0; j < bytes_per_level_with_zeros[i].size(); j++) {
      std::cout << "Level " << i << " run " << j
                << " size: " << bytes_per_level_with_zeros[i][j] << std::endl;
    }

    if (bytes_per_level_with_zeros[i].size() == 0) {
      break;
    }

    for (int j = 0; j < bytes_per_level_with_zeros[i].size(); j++) {
      entries_per_run.push_back(bytes_per_level_with_zeros[i][j] / key_size);
    }
  }

  std::vector<long> bits_per_key_per_level;
//   delete db;

  if (use_monkey) {
    std::cout << "Allocating bloom filters based on Monkey algo..."
              << std::endl;
    std::vector<long> bits_per_key_per_run =
        run_algorithm_c(entries_per_run, key_size, bits_per_entry_filter);

    // average filter size over all level 0 files
    int level0_allocated_bits = 0;
    int num_level0_runs = bytes_per_level_with_zeros[0].size();

    for (int i = 0; i < num_level0_runs; i++) {
      level0_allocated_bits += bits_per_key_per_run[i];
    }
    bits_per_key_per_level.push_back(level0_allocated_bits / num_level0_runs);

    // rest of the levels have one level per run
    bits_per_key_per_level.insert(
        bits_per_key_per_level.end(),
        bits_per_key_per_run.begin() + num_level0_runs,
        bits_per_key_per_run.end());
  } else {
    // make vector with just bits per entry
    for (int i = 0; i < 7; i++) {
      bits_per_key_per_level.push_back(bits_per_entry_filter);
    }
  }

  options.filter_policy = leveldb::NewBloomFilterPolicy(bits_per_key_per_level);
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  db->ForceFilters();

  for (int i = 1000000; i > 0; i--) {
    std::string value;
    leveldb::Status s = db->Get(leveldb::ReadOptions(),
                                leveldb::Slice(std::to_string(i)), &value);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    }
    if (value != std::to_string(i + 1)) {
      std::cout << "oops2" << std::endl;
      std::cout << "value was " << value << " but should be " << i+1 << std::endl;
    }
  }
  std::cout << "We gucci?" << std::endl;
  auto runs = db->GetBytesPerRun();
  for (long i = 0; i < runs.size(); i++) {
    for (int j = 0; j < runs[i].size(); j++) {
      std::cout << "Level " << i << " run " << j << " size: " << runs[i][j] << std::endl;
    }
  }

  return 0;
  }