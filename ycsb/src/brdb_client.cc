#include "ycsb/src/brdb_client.h"
#include "algorithm"
#include "math.h"
#include <numa.h>
#include <chrono>
using namespace ycsbc;


static std::atomic<uint64_t> total_query_processed_;
static std::atomic<uint64_t> total_query_processed_last_;
static std::chrono::time_point<std::chrono::steady_clock> tp_last_;
static std::chrono::time_point<std::chrono::steady_clock> tp_begin_;

//namespace ycsbc{

void BrDBClient::Open() {
	PrintArgs();
	printf("BrDBClient::Open\n");
	// Nothing Implemented yet
}

void BrDBClient::ProduceWorkload() {
	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, request_num_, false);
	}
}

void BrDBClient::Load(){
	Reset();
	assert(update_time_ == nullptr);
	update_time_ = new TimeRecord(load_num_ + 1);
	//int base_coreid = options_.spandb_worker_num + 
	//          options_.env->GetBgThreadCores(leveldb::Env::LOW) + 
	//          options_.env->GetBgThreadCores(leveldb::Env::HIGH);
	int base_coreid = 0;

	//assert(!options_.enable_spdklogging);
	uint64_t num = load_num_ / loader_threads_;
	std::vector<std::thread> threads;
	auto fn = std::bind(&BrDBClient::BrDBLoader, this, 
			std::placeholders::_1, std::placeholders::_2);
	total_query_processed_.store(0);
	total_query_processed_last_.store(0);
	tp_begin_ = std::chrono::steady_clock::now();
	for(int i=0; i<loader_threads_; i++){
		if(i == loader_threads_ - 1)
			num = num + load_num_ % loader_threads_;
		threads.emplace_back(fn, num, (base_coreid + i));
	}
	printf("start time: %s\n", GetDayTime().c_str());
	auto start = TIME_NOW;
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	double time_sec = time / 1000 / 1000;
	printf("end time: %s\n", GetDayTime().c_str());

	const char* env;
	env = std::getenv("BR_CLIENT_WAIT_AFTER_SEC");
	int wait_after_sec = 0;
	if (env) {
		int lando = std::stoi(env);
		if (lando >= 0) {
			wait_after_sec = lando;
		}
	}
	std::this_thread::sleep_for(std::chrono::seconds(wait_after_sec));
	std::string stat_str2;
	//db_->WaitForMemTableCompactionDone();
	db_->wait_compaction();
	//db_->GetProperty("leveldb.stats", &stat_str2);
	printf("\n%s\n", stat_str2.c_str());
	fflush(stdout);

	double write_time_total = 0;
	double write_func_time_total = 0;
	double read_time_total = 0;
	double read_func_time_total = 0;
	for (int i = 0; i < loader_threads_; i++) {
		write_time_total += cd_[i]->write_time_total;
		write_func_time_total += cd_[i]->write_func_time_total;
		read_time_total += cd_[i]->read_time_total;
		read_func_time_total += cd_[i]->read_func_time_total;
	}
	printf("------------------------------------------------------------------\n");
	printf("write_time_total:      %.3lf\n", write_time_total);
	printf("write_func_time_total: %.3lf\n", write_func_time_total);
	printf("read_time_total:       %.3lf\n", read_time_total);
	printf("read_func_time_total:  %.3lf\n", read_func_time_total);
	printf("------------------------------------------------------------------\n");

	assert(update_time_->Size() == load_num_);
	printf("==================================================================\n");
	PrintArgs();
	printf("Load %ld requests in %.3lf seconds.\n", load_num_, time/1000/1000);
	printf("Load latency: %.3lf us\n", update_time_->Sum()/update_time_->Size());
	printf("Load IOPS: %.3lf M\n", load_num_/time*1000*1000/1000/1000);
	printf("Load median latency: %.3lf us\n", update_time_->Tail(0.5));
	printf("Load P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
			update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
			update_time_->Tail(0.90), update_time_->Tail(0.75));
	printf("------------------------------------------------------------------\n");
	printf("THROUGHPUT(ops/sec) %.3lf\n", (double) load_num_ / time_sec);
	printf("LATENCY(usec/op) %.3lf\n", time / num);
	printf("==================================================================\n");

	env = std::getenv("BR_YCSB_FLUSH_LATENCY_DISTRIBUTION");
	if (env && strcmp(env, "")) {
		update_time_->Flush(env);
	}
	//request_time_->Flush("/scratch/wkim/zipperdb2/build/ycsb_results/latency.out");
}

void BrDBClient::Work(){
	Reset();
	assert(request_time_ == nullptr);
	assert(read_time_ == nullptr);
	assert(update_time_ == nullptr);
	request_time_ = new TimeRecord(request_num_ + 1);
	read_time_ = new TimeRecord(request_num_ + 1);
	update_time_ = new TimeRecord(request_num_ + 1);

	//int base_coreid = options_.spandb_worker_num + 
	//          options_.env->GetBgThreadCores(leveldb::Env::LOW) + 
	//          options_.env->GetBgThreadCores(leveldb::Env::HIGH);
	int base_coreid = 0;

	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, request_num_, false);
	}

	uint64_t num = request_num_ / worker_threads_;
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool, bool)> fn;
	fn = std::bind(&BrDBClient::BrDBWorker, this, 
			std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4);
	total_query_processed_.store(0);
	total_query_processed_last_.store(0);
	tp_begin_ = std::chrono::steady_clock::now();
	for(int i=0; i<worker_threads_; i++){
		if(i == worker_threads_ - 1)
			num = num + request_num_ % worker_threads_;
		threads.emplace_back(fn, num, (base_coreid + i), false, i==0);
	}
	printf("start time: %s\n", GetDayTime().c_str());
	auto start = TIME_NOW;
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	double time_sec = time / 1000 / 1000;
	printf("end time: %s\n", GetDayTime().c_str());

	std::string stat_str2;
	//db_->GetProperty("leveldb.stats", &stat_str2);
	printf("\n%s\n", stat_str2.c_str());
	fflush(stdout);

	assert(request_time_->Size() == request_num_);
	printf("==================================================================\n");
	PrintArgs();
	printf("WAL sync time per request: %.3lf us\n", wal_time_/request_num_);
	//printf("WAL sync time per sync: %.3lf us\n", wal_time_/
	//                  options_.statistics->getTickerCount(leveldb::WAL_FILE_SYNCED));
	printf("Wait time: %.3lf us\n", wait_time_/request_num_);
	printf("Complete wait time: %.3lf us\n", complete_memtable_time_/request_num_);
	printf("Write delay time: %.3lf us\n", write_delay_time_/request_num_);
	printf("Write memtable time: %.3lf\n", write_memtable_time_/request_num_);
	printf("Finish %ld requests in %.3lf seconds.\n", request_num_, time/1000/1000);
	if(read_time_->Size() != 0){
		printf("read num: %ld, read avg latency: %.3lf us, read median latency: %.3lf us\n", 
				read_time_->Size(), read_time_->Sum()/read_time_->Size(), read_time_->Tail(0.50));
		printf("read P999: %.3lf us, P99: %.3lf us, P95: %.3lf us, P90: %.3lf us, P75: %.3lf us\n",
				read_time_->Tail(0.999), read_time_->Tail(0.99), read_time_->Tail(0.95),
				read_time_->Tail(0.90), read_time_->Tail(0.75));
	}else{
		printf("read num: 0, read avg latency: 0 us, read median latency: 0 us\n");
		printf("read P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	if(update_time_->Size() != 0){
		printf("update num: %ld, update avg latency: %.3lf us, update median latency: %.3lf us\n", 
				update_time_->Size(), update_time_->Sum()/update_time_->Size(), update_time_->Tail(0.50));
		printf("update P999: %.3lf us, P99: %.3lf us, P95: %.3lfus, P90: %.3lf us, P75: %.3lf us\n",
				update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
				update_time_->Tail(0.90), update_time_->Tail(0.75));
	}else{
		printf("update num: 0, update avg latency: 0 us, update median latency: 0 us\n");
		printf("update P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	printf("Work latency: %.3lf us\n", request_time_->Sum()/request_time_->Size());
	printf("Work IOPS: %.3lf M\n", request_num_/time*1000*1000/1000/1000);
	printf("Work median latency: %.3lf us\n", request_time_->Tail(0.5));
	printf("Work P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
			request_time_->Tail(0.999), request_time_->Tail(0.99), request_time_->Tail(0.95),
			request_time_->Tail(0.90), request_time_->Tail(0.75));
	//printf("Stall: %.3lf us\n", options_.statistics->getTickerCount(leveldb::STALL_MICROS)*1.0);
	//printf("Stall rate: %.3lf \n", options_.statistics->getTickerCount(leveldb::STALL_MICROS)*1.0/time);
	//printf("Block read time: %.3lf us\n", block_read_time_/read_time_->Size());
	//uint64_t block_hit = options_.statistics->getTickerCount(leveldb::BLOCK_CACHE_HIT);
	//uint64_t block_miss = options_.statistics->getTickerCount(leveldb::BLOCK_CACHE_MISS);
	//uint64_t memtable_hit = options_.statistics->getTickerCount(leveldb::MEMTABLE_HIT);
	//uint64_t memtable_miss = options_.statistics->getTickerCount(leveldb::MEMTABLE_MISS);
	//printf("block cache hit ratio: %.3lf (hit: %ld, miss: %ld)\n", 
	//    block_hit*1.0/(block_hit+block_miss), block_hit, block_miss);
	//printf("memtable hit ratio: %.3lf (hit: %ld, miss: %ld)\n",
	//     memtable_hit*1.0/(memtable_hit+memtable_miss), memtable_hit, memtable_miss);
	printf("submit_time: %.3lf\n", submit_time_ / 1000.0 / request_num_);
	printf("------------------------------------------------------------------\n");
	printf("THROUGHPUT(ops/sec) %.3lf\n", (double) request_num_ / time_sec);
	printf("LATENCY(usec/op) %.3lf\n", time / num);
	printf("==================================================================\n");
	fflush(stdout);
	//request_time_->Flush("/scratch/wkim/zipperdb2/build/ycsb_results/latency.out");
}

void BrDBClient::Warmup(){

	//int base_coreid = options_.spandb_worker_num + 
	//          options_.env->GetBgThreadCores(leveldb::Env::LOW) + 
	//          options_.env->GetBgThreadCores(leveldb::Env::HIGH);
	int base_coreid = 0;
	Reset();
	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, (uint64_t)request_num_ * (1+warmup_rate_) + 1, false);
	}

	auto start = TIME_NOW;
	const uint64_t warmup_num = floor(request_num_ * warmup_rate_);
	const uint64_t num = warmup_num / worker_threads_;
	printf("Start warmup (%ld)...\n", num*worker_threads_);
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool, bool)> fn;
	printf("warmup start: %s\n", GetDayTime().c_str());
	fn = std::bind(&BrDBClient::BrDBWorker, this, 
			std::placeholders::_1, std::placeholders::_2,
			std::placeholders::_3, std::placeholders::_4);
	for(int i=0; i<worker_threads_; i++){
		threads.emplace_back(fn, num, base_coreid + i, true, i==0);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	printf("warmup finish: %s\n", GetDayTime().c_str());
	printf("Warmup complete %ld requests in %.3lf seconds.\n", num*worker_threads_, time/1000/1000);
}

void BrDBClient::BrDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master){
	//printf("[thread %d]: BrDBClient::BrDBWorker start\n", coreid);
	const char* env = std::getenv("BR_CLIENT_NUMA_BIND");
	if (env && strcmp(env, "")) {
		int client_numa_bind = std::stoi(env);
		if (client_numa_bind >= 0) {
			numa_run_on_node(client_numa_bind);
		} else if (client_numa_bind == -1) {
			SetAffinity(coreid);
		}
		//numa_run_on_node(coreid % numa_num_configured_nodes());
	}
	//leveldb::SetPerfLevel(leveldb::PerfLevel::kEnableTimeExceptForMutex);
	//leveldb::get_perf_context()->Reset();
	//leveldb::get_iostats_context()->Reset();

	ThreadData td;
	td.cpu = coreid;
	td.numa = numa_node_of_cpu(td.cpu);

	TimeRecord request_time(num + 1);
	TimeRecord read_time(num + 1);
	TimeRecord update_time(num + 1);

	int entries_per_batch = 1;
	env = std::getenv("BR_CLIENT_WRITE_ONLY_BATCH");
	if (env) {
		int batch_size = std::stoi(env);
		if (batch_size > 1) {
			entries_per_batch = batch_size;
		}
	}

	if(!is_warmup && is_master){
		printf("starting requests...\n");
	}

	std::map<int, std::string> w_value_table;
	w_value_table.emplace(8, std::string(8, 'a'));
	w_value_table.emplace(64, std::string(64, 'a'));
	w_value_table.emplace(100, std::string(100, 'a'));
	w_value_table.emplace(256, std::string(256, 'a'));
	w_value_table.emplace(1024, std::string(1024, 'a'));
	std::string r_value;

	printf("zzunny97 : debug1\n");
	if (entries_per_batch > 1) {
		printf("zzunny97 : debug1-1\n");
		//std::vector<std::pair<std::string, std::string>> batch;
		std::vector<std::pair<std::string_view, std::string_view>> batch;
		uint64_t cnt = 0;
		for(uint64_t i=0; i<num; i+= entries_per_batch){
			batch.clear();
			uint64_t queries_batch = 0;
			for (int j = 0; j < entries_per_batch; j++) {
				if (cnt == num) break;
				WorkloadWrapper::Request *req = workload_wrapper_->GetNextRequest();
				ycsbc::Operation opt = req->Type();
				assert(req != nullptr);
				auto start = TIME_NOW;
				if(opt == READ){
					// db_->Get(read_options_, req->Key(), &r_value);
					// ERR(db_->Get(read_options_, req->Key(), &r_value, cd_[coreid]));
					db_->get(&td, req->Key());
					queries_batch++;
				}else if(opt == UPDATE){
					//ERR(db_->Put(write_options_, req->Key(), w_value_table[req->Length()], cd_[coreid]));
					batch.emplace_back(std::move(req->Key()), w_value_table[req->Length()]);
				}else if(opt == INSERT){
					//ERR(db_->Put(write_options_, req->Key(), w_value_table[req->Length()], cd_[coreid]));
					batch.emplace_back(std::move(req->Key()), w_value_table[req->Length()]);
				}else if(opt == READMODIFYWRITE){
					// db_->Get(read_options_, req->Key(), &r_value);
					//ERR(db_->Get(read_options_, req->Key(), &r_value));
					//ERR(db_->Put(write_options_, req->Key(), w_value_table[req->Length()], cd_[coreid]));
					db_->get(&td, req->Key());
					db_->put(&td, req->Key(), w_value_table[req->Length()]);
					queries_batch++;
				} /*else if(opt == SCAN){
					leveldb::Iterator* iter = db_->NewIterator(read_options_);
					iter->Seek(req->Key());
					for (int j = 0; j < req->Length() && iter->Valid(); j++) {
				// Do something with it->key() and it->value().
				iter->Next();
				}
				ERR(iter->status());
				delete iter;
				queries_batch++;
				}*/ else{
					throw utils::Exception("Operation request is not recognized!");
				}
			double time =  TIME_DURATION(start, TIME_NOW);
			request_time.Insert(time);
			if(opt == READ /*|| opt == SCAN*/ ){
				read_time.Insert(time);
			}else if(opt == READMODIFYWRITE){
				update_time.Insert(time);
			}else{
				assert(0);
			}
			cnt++;
			}

			auto start = TIME_NOW;
			//ERR(db_->BatchSortAndWrite(batch));
			db_->put_batch(&td, batch);
			double time = TIME_DURATION(start, TIME_NOW);
			for (int j = 0; j < batch.size(); j++) {
				update_time.Insert(time / batch.size());
			}
			queries_batch+=batch.size();
			uint64_t ret = total_query_processed_.fetch_add(queries_batch);
			if (ret % (100 * entries_per_batch) == 0) {
				auto tp_now = std::chrono::steady_clock::now();
				std::chrono::duration<double> ts = tp_now - tp_last_;

				uint64_t num_queries_this_round = ret + queries_batch - total_query_processed_last_;
				total_query_processed_last_.store(ret + queries_batch);
				std::chrono::duration<double> dur_from_begin = tp_now - tp_begin_;
				//LOG("TIMESTAMP: %.3lf Throughput: %lf ops/sec", dur_from_begin.count(), (double) num_queries_this_round / ts.count());

				tp_last_ = tp_now;
			}
		}
	} else {
		printf("zzunny97 : debug1-2\n");
		uint64_t qcnt = 0;
		for(uint64_t i=0; i<num; i++){
			WorkloadWrapper::Request *req = workload_wrapper_->GetNextRequest();
			ycsbc::Operation opt = req->Type();
			assert(req != nullptr);
			auto start = TIME_NOW;
			if(opt == READ){
				printf("READ\n");
				db_->get(&td, req->Key());
			}else if(opt == UPDATE){
				printf("UPDATE\n");
				db_->put(&td, req->Key(), w_value_table[req->Length()]);
			}else if(opt == INSERT){
				printf("INSERT\n");
				db_->put(&td, req->Key(), w_value_table[req->Length()]);
			}else if(opt == READMODIFYWRITE){
				printf("READMODIFYWRITE\n");
				db_->get(&td, req->Key());
				db_->put(&td, req->Key(), w_value_table[req->Length()]);
			} /*else if(opt == SCAN){
				leveldb::Iterator* iter = db_->NewIterator(read_options_);
				iter->Seek(req->Key());
				for (int j = 0; j < req->Length() && iter->Valid(); j++) {
			// Do something with it->key() and it->value().
			iter->Next();
			}
			ERR(iter->status());
			delete iter;
			}*/ else{
				throw utils::Exception("Operation request is not recognized!");
			}
		double time =  TIME_DURATION(start, TIME_NOW);
		request_time.Insert(time);
		if(opt == READ || opt == SCAN){
			read_time.Insert(time);
		}else if(opt == UPDATE || opt == INSERT || opt == READMODIFYWRITE){
			update_time.Insert(time);
		}else{
			assert(0);
		}

		qcnt++;
		if (qcnt % 1000 == 0) {
			uint64_t ret = total_query_processed_.fetch_add(qcnt);
			if (ret % 2000000 == 0) {
				auto tp_now = std::chrono::steady_clock::now();
				std::chrono::duration<double> ts = tp_now - tp_last_;

				uint64_t num_queries_this_round = ret + qcnt - total_query_processed_last_;
				total_query_processed_last_.store(ret + qcnt);
				std::chrono::duration<double> dur_from_begin = tp_now - tp_begin_;
				//LOG("TIMESTAMP: %.3lf Throughput: %lf ops/sec", dur_from_begin.count(), (double) num_queries_this_round / ts.count());

				tp_last_ = tp_now;
			}
			qcnt = 0;
		}
		}
	}

	if(is_warmup)
		return ;

	mutex_.lock();
	request_time_->Join(&request_time);
	read_time_->Join(&read_time);
	update_time_->Join(&update_time);
	//wal_time_ += leveldb::get_perf_context()->write_wal_time/1000.0;
	//wait_time_ += leveldb::get_perf_context()->write_thread_wait_nanos/1000.0;
	//complete_memtable_time_ += leveldb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	//write_delay_time_ += leveldb::get_perf_context()->write_delay_time/1000.0;
	//block_read_time_ += leveldb::get_perf_context()->block_read_time/1000.0;
	//write_memtable_time_ += leveldb::get_perf_context()->write_memtable_time/1000.0;
	//printf("%s\n\n",leveldb::get_perf_context()->ToString().c_str());
	//printf("%s\n\n",leveldb::get_iostats_context()->ToString().c_str());
	mutex_.unlock();
}

void BrDBClient::BrDBLoader(uint64_t num, int coreid){
	ThreadData td;
	td.cpu = coreid;
	td.numa = numa_node_of_cpu(td.cpu);
	const char* env = std::getenv("BR_CLIENT_NUMA_BIND");
	if (env && strcmp(env, "")) {
		int client_numa_bind = std::stoi(env);
		if (client_numa_bind >= 0) {
			numa_run_on_node(client_numa_bind);
		} else if (client_numa_bind == -1) {
			SetAffinity(coreid);
		}
		//numa_run_on_node(coreid % numa_num_configured_nodes());
	}
	//leveldb::SetPerfLevel(leveldb::PerfLevel::kEnableTimeExceptForMutex);
	//leveldb::get_perf_context()->Reset();
	//leveldb::get_iostats_context()->Reset();

	TimeRecord update_time(num + 1);

	int entries_per_batch = 1;
	env = std::getenv("BR_CLIENT_WRITE_ONLY_BATCH");
	if (env) {
		int batch_size = std::stoi(env);
		if (batch_size > 1) {
			entries_per_batch = batch_size;
		}
	}

	if (entries_per_batch > 1) {
#if 0
		leveldb::WriteBatch batch;
		uint64_t cnt = 0;
		for(uint64_t i=0; i<num; i+= entries_per_batch) {
			batch.Clear();
			for (int j = 0; j < entries_per_batch; j++) {
				if (cnt == num) {
					break;
				}
				std::string table;
				std::string key;
				std::vector<ycsbc::CoreWorkload::KVPair> values;
				workload_proxy_->LoadInsertArgs(table, key, values);
				assert(values.size() == 1);
				for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
					std::string value = field_pair.second;
					batch.Put(key, value);
				}
				cnt++;
			}
			auto start = TIME_NOW;
			ERR(db_->BatchWrite(write_options_, &batch));
			double time =  TIME_DURATION(start, TIME_NOW);
			for (int j = 0; j < entries_per_batch; j++) {
				update_time.Insert(time / entries_per_batch);
			}
		}
#else
		//std::vector<std::pair<std::string, std::string>> batch;
		std::vector<std::pair<std::string_view, std::string_view>> batch;
		uint64_t cnt = 0;
		for(uint64_t i=0; i<num; i+= entries_per_batch) {
			batch.clear();
			int qcnt_batch = cnt;
			for (int j = 0; j < entries_per_batch; j++) {
				if (cnt == num) {
					break;
				}
				std::string table;
				std::string key;
				std::vector<ycsbc::CoreWorkload::KVPair> values;
				workload_proxy_->LoadInsertArgs(table, key, values);
				assert(values.size() == 1);
				for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
					std::string value = field_pair.second;
					batch.emplace_back(std::move(key), std::move(value));
				}
				cnt++;
			}
			qcnt_batch = cnt - qcnt_batch;

			auto start = TIME_NOW;
			//ERR(db_->BatchSortAndWrite(batch));
			db_->put_batch(&td, batch);
			double time = TIME_DURATION(start, TIME_NOW);
			for (int j = 0; j < entries_per_batch; j++) {
				update_time.Insert(time / entries_per_batch);
			}

			uint64_t ret = total_query_processed_.fetch_add(qcnt_batch);
			if (ret % (400 * entries_per_batch) == 0) {
				auto tp_now = std::chrono::steady_clock::now();
				std::chrono::duration<double> ts = tp_now - tp_last_;

				uint64_t num_queries_this_round = ret + qcnt_batch - total_query_processed_last_;
				total_query_processed_last_.store(ret + qcnt_batch);
				std::chrono::duration<double> dur_from_begin = tp_now - tp_begin_;
				//LOG("TIMESTAMP: %.3lf Throughput: %lf ops/sec", dur_from_begin.count(), (double) num_queries_this_round / ts.count());

				tp_last_ = tp_now;
			}
		}
#endif
	} else {
		int qcnt = 0;
		for(uint64_t i=0; i<num; i++){
			std::string table;
			std::string key;
			std::vector<ycsbc::CoreWorkload::KVPair> values;
			workload_proxy_->LoadInsertArgs(table, key, values);
			assert(values.size() == 1);
			auto start = TIME_NOW;
			for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
				std::string value = field_pair.second;
				//ERR(db_->Put(write_options_, key, value, cd_[coreid]));
				db_->put(&td, key, value);
			}
			double time =  TIME_DURATION(start, TIME_NOW);
			update_time.Insert(time);

			qcnt++;
			if (qcnt % 1000 == 0) {
				uint64_t ret = total_query_processed_.fetch_add(qcnt);
				if (ret % 500000 == 0) {
					auto tp_now = std::chrono::steady_clock::now();
					std::chrono::duration<double> ts = tp_now - tp_last_;

					uint64_t num_queries_this_round = ret + qcnt - total_query_processed_last_;
					total_query_processed_last_.store(ret + qcnt);
					std::chrono::duration<double> dur_from_begin = tp_now - tp_begin_;
					//LOG("TIMESTAMP: %.3lf Throughput: %lf ops/sec", dur_from_begin.count(), (double) num_queries_this_round / ts.count());

					tp_last_ = tp_now;
				}
				qcnt = 0;
			}
		}
	}
	mutex_.lock();
	update_time_->Join(&update_time);
	//wal_time_ += leveldb::get_perf_context()->write_wal_time/1000.0;
	//wait_time_ += leveldb::get_perf_context()->write_thread_wait_nanos/1000.0;
	//complete_memtable_time_ += leveldb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	//write_delay_time_ += leveldb::get_perf_context()->write_delay_time/1000.0;
	//write_memtable_time_ += leveldb::get_perf_context()->write_memtable_time/1000.0;
	mutex_.unlock();
}

void BrDBClient::Reset(){
	//options_.statistics->Reset();
	//db_->ResetStats();
	wait_time_ = wal_time_ = 0;
	complete_memtable_time_ = 0;
	block_read_time_ = 0;
	write_delay_time_ = 0;
	write_memtable_time_ = 0;
	submit_time_ = 0;
	for (auto& cd : cd_) {
		//memset(cd, 0, sizeof(leveldb::client_data));
		memset(cd, 0, sizeof(client_data));
	}
}


void BrDBClient::PrintArgs(){
	printf("-----------configuration------------\n");
	printf("Loader threads: %d\n", loader_threads_);
	printf("Load num: %zu\n", load_num_);
	printf("Worker threads: %d\n" ,worker_threads_);
	printf("Request num: %zu\n", request_num_);
	printf("-------------------------------------\n");
	fflush(stdout);
}

//}

