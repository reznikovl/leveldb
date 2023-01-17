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

std::vector<std::pair<std::string, std::string>> read_range(
    leveldb::DB* db, std::string v1, std::string v2) {
  std::vector<std::pair<std::string, std::string>> result;
  auto it = db->NewIterator(leveldb::ReadOptions());
  for(it->Seek(leveldb::Slice(v1)); it->key().ToString() < v2; it->Next()) {
    result.push_back({it->key().ToString(), it->value().ToString()});
  }
  return result;
}

std::vector<long> run_algorithm_c(std::vector<long> entries_per_level,
                                  int key_size, int bits_per_entry_equivalent) {
  long total_entries = 0;
  for (auto& i : entries_per_level) {
    total_entries += i;
  }
  std::cout << "Total bytes found is " << total_entries * key_size << std::endl;
  long delta = total_entries * bits_per_entry_equivalent * 0.9;
  std::cout << "Adjusting for metadata, approx number of entries is " << delta / bits_per_entry_equivalent << std::endl;
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
    std::cout << "Usage: ./leron_bench1 SEED_DATABASE USE_MONKEY where VARS are each either 1 or 0." << std::endl;
    return -1;
  }
  
  leveldb::Options options;

  // std::vector<int> leveling_factors{10, 10, 2,2,2,2,2,}; // first number is ignored

  // other options to set:
  options.block_size = 4 * 1024;
  options.compression = leveldb::kNoCompression;
  // options.leveling_factors = leveling_factors;
  int key_size = 128;
  int num_megabytes_to_write = 512;
  int bits_per_entry_filter = 1;
  options.base_scaling_factor = 10;
  options.ratio_diff = 1;

  bool seed_database = strcmp(argv[1], "1") == 0;
  bool use_monkey = strcmp(argv[2], "1") == 0;
  leveldb::DB* db;
  srand(time(nullptr)); // TODO: Constant seed
  options.create_if_missing = true;
  
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

  if (seed_database) {
    std::cout << "Seeding database..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    int status = write_data(db, num_megabytes_to_write, key_size);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    if (status != 0) {
      std::cout << "Error seeding database." << std::endl;
      return -1;
    } else {
      std::cout << "Database seeding complete." << std::endl;
      std::cout << "Time to seed db: " << duration.count() << " ms"
                << std::endl;
    }
  }

  

  //reopen db for bloom filter sizes
  delete db;
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  // db->CompactLevel0Files();
  std::cout << "Calculating bloom filters..." << std::endl;

  std::vector<std::vector<long>> bytes_per_level_with_zeros = db->GetBytesPerRun();
  std::vector<long> entries_per_run;
  for (long i = 0; i < bytes_per_level_with_zeros.size(); i++) {
    for (int j = 0; j < bytes_per_level_with_zeros[i].size(); j++) {
      std::cout << "Level " << i << " run " << j
                << " size: " << bytes_per_level_with_zeros[i][j] << std::endl;
    }
    if (bytes_per_level_with_zeros[i].size() == 0) {
      break;
    }

    for(int j = 0; j < bytes_per_level_with_zeros[i].size(); j++) {
      entries_per_run.push_back(bytes_per_level_with_zeros[i][j] / key_size);
    }
  }

  std::vector<long> bits_per_key_per_level;

  if (use_monkey) {
    std::cout << "Allocating bloom filters based on Monkey algo..." << std::endl;
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
        bits_per_key_per_run.begin() + num_level0_runs, bits_per_key_per_run.end());
  }
  else {
    // make vector with just bits per entry
    for (int i = 0; i < 7; i++) {
      bits_per_key_per_level.push_back(bits_per_entry_filter);
    }
  }          

  delete db;
  options.filter_policy = leveldb::NewBloomFilterPolicy(bits_per_key_per_level);
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  std::cout << "Forcing filters" << std::endl;
  // db->ForceFilters();
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
  std::cout << "Done point reading. Took " << duration.count() << "ms"
            << std::endl;

  start = std::chrono::high_resolution_clock::now();
  auto result = read_range(db, "C", "D");
  stop = std::chrono::high_resolution_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
  std::cout << "Range read query done. Took " << duration.count() << "ms"
            << std::endl;
  if (result.empty()) {
    std::cout << "Nothing found..." << std::endl;
  } else {
    std::cout << "Read from " << result[0].first << " to "
              << result[result.size() - 1].first << std::endl;
  }
  return 0;
}