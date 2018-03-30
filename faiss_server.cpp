#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"

double elapsed ()
{
	struct timeval tv;
	gettimeofday (&tv, NULL);
	return  tv.tv_sec + tv.tv_usec * 1e-6;
}

//db new
Status FaissServiceImpl::DbNew(ServerContext* context,
		const ::faiss_server::DbNewRequest* request, 
		::faiss_server::EmptyResponse* response) {
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:DbNew"
		<< " max_size:" << request->max_size()
		<< " model:" << request->model()
		<< " db_name:" << request->db_name();
	double t0 = elapsed();
	std::string dbName = request->db_name();
	std::string model = request->model();
	//校验参数
	if (dbName.length() < 1 || dbName.length() > 50 ||
			model.length() < 1 || model.length() > 100) {
		response->set_error_code(grpc::StatusCode::INVALID_ARGUMENT);
		response->set_error_msg("INVALID_ARGUMENT");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return grpc::Status::CANCELLED;
	}
	//检查dbs
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);
	if (it != dbs.end()) {
		//已经存在同名db
		response->set_error_code(grpc::StatusCode::ALREADY_EXISTS);
		response->set_error_msg("ALREADY_EXISTS");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return grpc::Status::OK;
	}
	
	uint64_t maxSize = DefaultDBSize;	
	if (request->max_size() > 1 && request->max_size() < MaxDBSize) {
		maxSize = request->max_size();
	}
	oss << " new_max_size:" << maxSize;
	//模型路径
	std::string modelPath = "./model/" + request->model();

	//加载index文件
	FaissDB *db = new FaissDB(dbName, modelPath, maxSize);
	int rc = db->loadIndex(m_resources, modelPath);
	if (0 != rc) {
		response->set_error_code(grpc::StatusCode::DATA_LOSS);
		response->set_error_msg("load index failed");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return Status::OK;
	}

	dbs[dbName] = db;

	//store kv format
	//dbName:modelPath##maxSize
	//dbName: 增加一个前缀后再入库
	//modelPath: 初始化的模型文件 
	//maxSize: 用于设置某个db的最大feature的大小
	size_t len = 128;
	char key[len] ={'\0'};
	char val[len] = {'\0'};
	snprintf(key, len, "%s%s", SPrefix.c_str(), dbName.c_str());
	snprintf(val, len, "%s%s%ld", modelPath.c_str(), SDivide.c_str(), maxSize);
	rc = lmdbSet(key, val);
	response->set_error_code(rc);
	response->set_request_id(request->request_id());
	oss << " persist_info:(" << key << ":" << val <<") store resp " << rc
		<< " error_code:" << response->error_code();
	LOG(INFO) << oss.str();
	return Status::OK; 
}
//db list
Status FaissServiceImpl::DbList(ServerContext* context, const ::faiss_server::DbListRequest* request, ::faiss_server::DbListResponse* response) {
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:DbList";

	//no lock	
	response->set_request_id(request->request_id());
	response->set_error_code(grpc::StatusCode::OK);
	int count = 0;
	for (std::map<std::string, FaissDB*>::iterator it = dbs.begin();
			it != dbs.end(); ++it) {
		count ++;
		auto status = response->add_db_status();
		auto db = it->second; 
		status->set_name(it->first);
		status->set_ntotal((db->index)->ntotal);
		status->set_max_size(db->maxSize);
		status->set_curr_max_id(db->maxID);
		status->set_curr_persist_max_id(db->maxPersistID);
		status->set_persist_path(db->persistPath);
		status->set_raw_data_path(db->lmdbPath);
		status->set_dimension((db->index)->d);
		std::string modelPath = db->modelPath;
		size_t pos = modelPath.find_last_of("/");
		if (pos != std::string::npos) {
			status->set_model(modelPath.substr(pos + 1));
		}
		status->set_black_list_len((db->blackList).size());
	}
	oss << " db_len:" << count
		<< " error_code:" << response->error_code();
	LOG(INFO) << oss.str();
	return Status::OK;
} 

//db delete 
Status FaissServiceImpl::DbDel(ServerContext* context, const ::faiss_server::DbDelRequest* request, ::faiss_server::EmptyResponse* response) { 
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:DbDel"
		<< " db_name:" << request->db_name();

	std::string dbName = request->db_name();
	//校验参数
	if (dbName.length() < 1 || dbName.length() > 50) {
		response->set_error_code(grpc::StatusCode::INVALID_ARGUMENT);
		response->set_error_msg("INVALID_ARGUMENT");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return grpc::Status::CANCELLED;
	}

	//检查dbs
	{
		unique_writeguard<WfirstRWLock> writelock(*m_lock);
		std::map<std::string, FaissDB*>::iterator it;
		it = dbs.find(dbName);
		if (it != dbs.end()) {
			auto db = it->second;
			unique_writeguard<WfirstRWLock> writelock(*(db->lock));
			//db存在
			//delete lmdb and index file
			delete db;
			db = NULL;
			dbs.erase(it);
			
			//delete lmdb storage 
			size_t len = 128;
			char key[len] ={'\0'};
			snprintf(key, len, "%s%s", SPrefix.c_str(), dbName.c_str());
			lmdbDel(key);

			response->set_error_code(grpc::StatusCode::OK);
			response->set_request_id(request->request_id());
			oss << " error_code:" << response->error_code();
			LOG(INFO) << oss.str();	
			return grpc::Status::OK;
		}
	}	
	//不存在
	response->set_request_id(request->request_id());
	response->set_error_code(grpc::StatusCode::NOT_FOUND);
	response->set_error_msg("NOT_FOUND");
	oss << " error_code:" << response->error_code()
		<< " error_msg:" << response->error_msg();
	LOG(WARNING) << oss.str();	
	return Status::OK;
} 
