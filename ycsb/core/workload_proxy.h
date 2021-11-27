#include "core_workload.h"

namespace ycsbc{

class WorkloadProxy{
	private:
		CoreWorkload *workload_;

	public:
		WorkloadProxy(CoreWorkload *wl)
			:workload_(wl){

		}

		~WorkloadProxy(){
			
		}

    size_t record_count() { return workload_->record_count(); }
    size_t operation_count() { return workload_->operation_count(); }

    bool is_load() {
      return workload_->is_load();
    }

    std::string name_str() { return workload_->name_str(); }
    std::string distribution_str() { return workload_->distribution_str(); }

    std::string filename_prefix() { return workload_->filename_prefix(); }

		void LoadInsertArgs(std::string &key, size_t& value_len) {
			key = workload_->NextSequenceKey();
      value_len = workload_->field_count() * workload_->field_len();
		}

		Operation GetNextOperation(){
			return workload_->NextOperation();
		}

		void GetReadArgs(std::string &key) {
			key = workload_->NextTransactionKey();
		}

		void GetReadModifyWriteArgs(std::string &key, size_t& value_len) {
			key = workload_->NextTransactionKey();
      value_len = workload_->field_count() * workload_->field_len();
		}

		void GetScanArgs(std::string &key, int &len){
			key = workload_->NextTransactionKey();
			len = workload_->NextScanLength();
		}

		void GetUpdateArgs(std::string &key, size_t& value_len){
			key = workload_->NextTransactionKey();
      value_len = workload_->field_count() * workload_->field_len();
		}

		void GetInsertArgs(std::string &key, size_t& value_len){
			key = workload_->NextTransactionKey();
      value_len = workload_->field_count() * workload_->field_len();
		}
};
}
