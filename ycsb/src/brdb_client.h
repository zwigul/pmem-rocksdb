#ifndef BRDB_CLIENT_H
#define BRDB_CLIENT_H
#include <iterator>
#include "thread"
#include "sys/mman.h"
#include "sys/syscall.h"
#include "unistd.h"
#include "workloadwrapper.h"
#include <sys/time.h>
#include "common.h"
#include "brdb.h"
#include "utils.h"
using namespace ycsbc;

//namespace ycsbc{

#endif

inline void *AllocBuffer(size_t capacity){
	void *buffer = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
	assert(buffer != MAP_FAILED);
	return buffer;
}

inline void DeallocBuffer(void *buffer, size_t capacity){
	assert(munmap(buffer, capacity) == 0);
}

class BrDBClient{
	private:
		brdb* db_ = nullptr;
		DBConf conf_;

		const int loader_threads_;
		const uint64_t load_num_;
		const int worker_threads_;
		const uint64_t request_num_;
		const double warmup_rate_ = 0.3;

		WorkloadProxy *workload_proxy_;
		WorkloadWrapper *workload_wrapper_ = nullptr;

		double wal_time_;
		double wait_time_;
		double complete_memtable_time_;
		double block_read_time_;
		double write_delay_time_;
		double write_memtable_time_;
		std::mutex mutex_;
		uint64_t submit_time_;
		uint64_t warmup_finished_ = 0;
		uint64_t query_processed_ = 0;

		TimeRecord *request_time_  = nullptr;
		TimeRecord *read_time_  = nullptr;
		TimeRecord *update_time_ = nullptr;
		TimeRecord *iops_ = nullptr;
		TimeRecord *insert_failed_ = nullptr;

		std::vector<client_data*> cd_;

	public:
		/*
		BrDBClient(WorkloadProxy *workload_proxy, int loader_threads,
				uint64_t load_num, int worker_threads, uint64_t request_num, int num_regions, int num_shards, int num_cws):
			workload_proxy_(workload_proxy),
			loader_threads_(loader_threads),
			load_num_(load_num),
			worker_threads_(worker_threads),
			request_num_(request_num) {
				conf = get_default_conf();
				conf.num_regions = num_regions;
				conf.num_shards = num_shards;
				conf.num_cws = num_cws;
				conf.mem_size = 8*(1ull<<20);
				conf.use_existing = false;
				db_ = new brdb(conf);

				int threads_max = loader_threads_;
				if (threads_max < worker_threads_) {
					threads_max = worker_threads_;
				}

				for (int i = 0; i < threads_max; i++) {
					cd_.push_back((client_data*) aligned_alloc(64, sizeof(client_data)));
				}
				Reset();
			}
			*/
		
		BrDBClient(WorkloadProxy *workload_proxy) : workload_proxy_(workload_proxy) {
			conf = get_default_conf();
			conf.num_regions = workload_proxy->num_regions;
			conf.num_shards = num_shards;
			conf.num_cws = num_cws;
			conf.mem_size = 8*(1ull<<20);
			conf.use_existing = false;

			// Initialize BRDB
			db_ = new brdb(conf);

			int threads_max = loader_threads_;
			if (threads_max < worker_threads_) {
				threads_max = worker_threads_;
			}

			for (int i = 0; i < threads_max; i++) {
				cd_.push_back((client_data*) aligned_alloc(64, sizeof(client_data)));
			}
			Reset();
		}

		~BrDBClient(){
			if(db_ != nullptr)
				delete db_;
			if(request_time_ != nullptr)
				delete request_time_;
			if(read_time_ != nullptr)
				delete read_time_;
			if(update_time_ != nullptr)
				delete update_time_;
			if(workload_wrapper_ != nullptr){
				delete workload_wrapper_;
			}
		}
		void Open();
		void ProduceWorkload();
		void Load();
		void Work();
		void Warmup();

	private:
		inline void SetAffinity(int coreid){
			coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
			//printf("client coreid: %d\n", coreid);
			cpu_set_t mask;
			CPU_ZERO(&mask);
			CPU_SET(coreid, &mask);
			int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
			assert(rc == 0);
		}

		static std::string GetDayTime(){
			const int kBufferSize = 100;
			char buffer[kBufferSize];
			struct timeval now_tv;
			gettimeofday(&now_tv, nullptr);
			const time_t seconds = now_tv.tv_sec;
			struct tm t;
			localtime_r(&seconds, &t);
			snprintf(buffer, kBufferSize,
					"%04d/%02d/%02d-%02d:%02d:%02d.%06d",
					t.tm_year + 1900,
					t.tm_mon + 1,
					t.tm_mday,
					t.tm_hour,
					t.tm_min,
					t.tm_sec,
					static_cast<int>(now_tv.tv_usec));
			return std::string(buffer);
		}

		void BrDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master);
		void BrDBLoader(uint64_t num, int coreid);
		void Reset();
		void PrintArgs();
};

//}
#endif // BRDB_CLIENT_H
