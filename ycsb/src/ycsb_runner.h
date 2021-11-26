#pragma once

#include "core/core_workload.h"

namespace ycsbc {

class YCSBRunner {
 public:
   YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
              rocksdb::Options options,
              std::string data_dir,
              rocksdb::DB* db);
   void run_all();
 private:
  const int num_threads_;
  std::vector<CoreWorkload*> workloads_;
  rocksdb::Options options_;
  std::string data_dir_;
  rocksdb::DB* db_ = NULL;
};

YCSBRunner::YCSBRunner(const int num_threads, std::vector<CoreWorkload*> workloads,
                       rocksdb::Options options,
                       std::string data_dir,
                       rocksdb::DB* db)
    : num_threads_(num_threads),
      workloads_(workloads),
      options_(options),
      data_dir_(data_dir),
      db_(db) {
}

void YCSBRunner::run_all() {
  for (auto& wl : workloads_) {
    WorkloadProxy wp(wl);
    RocksDBClient rocksdb_client(&wp, num_threads_, options_, data_dir_, db_);
    rocksdb_client.run();
  }
}

}  // namespace ycsbc
