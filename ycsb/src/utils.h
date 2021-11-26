#ifndef YCSB_SRC_UTILS_H
#define YCSB_SRC_UTILS_H
#include "cstdio"
#include "iostream"

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION(start, end) (std::chrono::duration<double>((end)-(start)).count() * 1000 * 1000)
typedef std::chrono::high_resolution_clock::time_point TimePoint;

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)	 (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)

void PrintWorkload(const char* filename);
void drop_cache();

void PrintWorkload(const char* filename){
	// TODO
}


void drop_cache() {
	// Remove cache
	int size = 256*1024*1024;
	char *garbage = new char[size];
	for(int i=0;i<size;++i)
		garbage[i] = i;
	for(int i=100;i<size;++i)
		garbage[i] += garbage[i-100];
	delete[] garbage;
}

struct TimeRecord{
	private:
		double *data_;
		const uint64_t capacity_;
		uint64_t size_;
		bool sorted;
	public:
		TimeRecord(uint64_t capacity):
			data_(nullptr),
			capacity_(capacity),
			size_(0),
			sorted(false){
				data_ = (double *)AllocBuffer(capacity_ * sizeof(double));
				assert(data_ != nullptr);
			}
		~TimeRecord(){
			assert(size_ <= capacity_);
			if(data_ != nullptr)
				DeallocBuffer(data_, capacity_ * sizeof(double));
		}
		void Insert(double time){
			data_[size_++] = time;
			assert(size_ <= capacity_);
			sorted = false;
		}
		double *Data(){
			return data_;
		}
		void Join(TimeRecord* time){
			assert(data_ != nullptr);
			assert(time->Data() != nullptr);
			uint64_t pos = __sync_fetch_and_add(&size_, time->Size());
			assert(size_ <= capacity_);
			memcpy(data_ + pos, time->Data(), sizeof(double) * time->Size());
			sorted = false;
		}
		double Sum(){
			assert(data_ != nullptr);
			return std::accumulate(data_, data_ + size_, 0.0);
		}
		uint64_t Size(){
			return size_;
		}
		double Tail(double f){
			assert(data_ != nullptr);
			if(!sorted){
				std::sort(data_, data_ + size_);
				sorted = true;
			}
			return data_[(uint64_t)(size_ * f)];
		}
};
#endif
