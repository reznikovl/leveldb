#include <iostream>
#include "leveldb/db.h"
#include <vector>

int main(int argc, char **argv) {
    // should STDOUT the number of bytes in the DB, should STDERR the logs
    // first arg should be DB path
    std::string db_name = argv[1];
    
    leveldb::Options options;
    options.create_if_missing = false;
    leveldb::DB *db;
    std::cerr << "Opening DB: " + db_name << std::endl;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    std::cerr << "Status: " << status.ToString() << std::endl;
    std::vector<std::vector<long>> sizes = db->GetBytesPerRun();
    long sum = 0;
    for(auto& level : sizes) {
        for(auto& run_size : level) {
            sum += run_size;
        }
    }
    std::cout << sum << std::endl;
    std::cerr << "Sum: " << sum << std::endl;
    std::cerr << "Done calculating." << std::endl;
    return 0;
}