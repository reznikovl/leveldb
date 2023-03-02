#include <chrono>
#include <cmath>
#include <iostream>
#include <time.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"

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
                                  int bits_per_entry_equivalent) {
  long total_entries = 0;
  for (auto& i : entries_per_level) {
    total_entries += i;
  }
  long delta = total_entries * bits_per_entry_equivalent;
  std::cout << "Entries count is " << total_entries << std::endl;
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
    if (std::abs(R_new - R) < 0.000001) {  // fixed float error
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

int main(int argc, char** argv) {
  /**
   * Arg1: 1 to seed, 0 for normal run
   * Arg2: db name
   * Arg3: compact L0
   * Arg4: Megabytes to write
   * Arg5: Base scaling factor
   * Arg6: Ratio Difference
   * Arg7: key size
   */
  bool use_monkey = true;
  bool seed = std::stoi(argv[1]);
  int bpk = 5; // 0 should write nothing
  std::string db_name = argv[2];
  bool compact_l0 = std::stoi(argv[3]);

  leveldb::Options options;
  if (seed) {
    options.ratio_diff = std::stod(argv[6]);
    options.base_scaling_factor = std::stoi(argv[5]);
  }
  
  


  leveldb::DB* db;
  options.create_if_missing = seed; // create if missing only if seeding

  std::cout << "Opening DB: " + db_name << std::endl;
  leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
  if (!status.ok()) {
    std::cout << "db could not be opened" << std::endl;
    std::cout << status.ToString() << std::endl;
    return -1;
  }

  if (seed) {
    int key_size = std::stoi(argv[7]);
    int num_megabytes_to_write = std::stoi(argv[4]);
    int write_seed = 42;
    std::cout << "Seeding database with " << num_megabytes_to_write << " mb"<< std::endl;
    srand(write_seed);
    int result = write_data(db, num_megabytes_to_write, key_size);
    if (result == -1) {
      std::cout << "Error seeding..." << std::endl;
    }
  }
  else {
    std::cout << "Not seeding" << std::endl;
  }

  if (compact_l0) {
    std::cout << "Compacting L0 files..." << std::endl;
    sleep(3);
    db->CompactLevel0Files();
    sleep(3);
  }
  else {
    std::cout << "Not compacting L0" << std::endl;
  }


  if (bpk == 0) {
    std::cout << "Setting options to empty filters..." << std::endl;
    options.filter_policy = nullptr;
  }
  else {
    std::vector<long> bits_per_key_per_level;
    if (use_monkey) {
      std::cout << "Calculating Bloom Filter Allocations for Monkey..."
                << std::endl;
      std::vector<std::vector<long>> entries_per_run_with_levels =
          db->GetExactEntriesPerRun();
      std::vector<long> entries_per_run;
      // flatten entries per run with levels and deal with 0s for Monkey
      for (int i = 0; i < entries_per_run_with_levels.size(); i++) {
        for (int j = 0; j < entries_per_run_with_levels[i].size(); j++) {
          std::cout << "Level " << i << " run " << j
                    << " size: " << entries_per_run_with_levels[i][j]
                    << std::endl;
        }

        if (entries_per_run_with_levels[i].size() == 0) {
          // necessary?
          break;
        }
        entries_per_run.insert(entries_per_run.end(),
                               entries_per_run_with_levels[i].begin(),
                               entries_per_run_with_levels[i].end());
      }
      std::vector<long> bits_per_key_per_run =
          run_algorithm_c(entries_per_run, bpk);
      // average filter size over all level 0 files
      int level0_allocated_bits = 0;
      int num_level0_runs = entries_per_run_with_levels[0].size();

      for (int i = 0; i < num_level0_runs; i++) {
        level0_allocated_bits += bits_per_key_per_run[i];
      }
      bits_per_key_per_level.push_back(level0_allocated_bits / num_level0_runs);

      // rest of the levels have one level per run
      bits_per_key_per_level.insert(
          bits_per_key_per_level.end(),
          bits_per_key_per_run.begin() + num_level0_runs,
          bits_per_key_per_run.end());
    }
    else {
      std::cout << "Not using monkey, assigning " << bpk << " per level" << std::endl;
      for (int i = 0; i < 7; i++) {
        bits_per_key_per_level.push_back(bpk);
      }
    }
    for (int i = 0; i < bits_per_key_per_level.size(); i++) {
      std::cout << "Level " << i << " bits per key is "
                << bits_per_key_per_level[i] << std::endl;
    }
    options.filter_policy = leveldb::NewBloomFilterPolicy(bits_per_key_per_level);
  }


  std::cout << "Forcing filters..." << std::endl;
  delete db;
  status = leveldb::DB::Open(options, db_name, &db);
  sleep(5);
  db->ForceFilters();
  sleep(3);
  delete db;
  return 0;
}