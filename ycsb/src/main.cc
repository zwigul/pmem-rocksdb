#include "rocksdb/options.h"
#include "src/rocksdb_client.h" 
#include "src/config.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h> 
#include "src/ycsb_runner.h"

int main(int argc, char* argv[]){
	utils::Properties common_props;
  std::vector<char> wl_chars;
  parse_command_line_arguments(argc, argv, &common_props, &wl_chars);

  // Workload
  std::vector<CoreWorkload*> workloads;
  for (auto& wl_char : wl_chars) {
    auto wl = new CoreWorkload();
    if (wl_char == 'l') {
      auto wl_props = gen_workload_props('a', common_props);
      wl->Init(wl_props, /*is_load=*/true);
    } else {
      auto wl_props = gen_workload_props(wl_char, common_props);
      wl->Init(wl_props, /*is_load=*/false);
    }
    workloads.push_back(wl);
  }

  // dbpath
  char user_name[100];
  getlogin_r(user_name, 100);
  std::string dbpath("/pmem/rocksdb_");
  dbpath.append(user_name);

  // db options
	rocksdb::Options options;
  options.wal_dir = dbpath + "/wal";
  options.create_if_missing = true;
  // *** DCPMM
  options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());
  // Key-value separation
  // - allocate values with libpmemobj
  options.dcpmm_kvs_enable = true;
  options.dcpmm_kvs_level = 0;
  options.dcpmm_kvs_mmapped_file_fullpath = dbpath + "/kvs";
  options.dcpmm_kvs_mmapped_file_size = 200*(1ull<<30);  // 200 GB
  options.dcpmm_kvs_value_thres = 64;  // minimal size to do kv sep
  options.dcpmm_compress_value = false;
#if 0
  // Optimized mmap read for pmem
  options.use_mmap_reads = true;
  options.cache_index_and_filter_blocks_for_mmap_read = true;
  rocksdb::BlockBasedTableOptions bbto;
  bbto.block_size = 256;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));
#endif

  // Open DB
  std::string data_dir = dbpath + "/db";
  rocksdb::DB* db;
  rocksdb::Status s = rocksdb::DB::Open(options, data_dir, &db);

  // Init and Run Workloads
  int num_threads = std::stoi(common_props.GetProperty("threadcount"));
  YCSBRunner runner(num_threads, workloads, options, data_dir, db);
  runner.run_all();

	fflush(stdout);
	return 0;
}
