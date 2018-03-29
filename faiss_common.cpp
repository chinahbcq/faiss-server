#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"

Status FaissServiceImpl::Ping(ServerContext* context, 
		const ::faiss_server::PingRequest* request, 
		::faiss_server::PingResponse* response) {
	response->set_payload("pong");
	return Status::OK;
}

void FaissServiceImpl::PersistIndexPeriod(FaissServiceImpl *handle,const unsigned int duration) {
	while (true) {
		std::this_thread::sleep_for (std::chrono::seconds(duration));
		
		if (NULL == handle) {
			continue;
		}
		{
			unique_readguard<WfirstRWLock> readlock(*(handle->m_lock));
			auto *dbs = &(handle->dbs);
			for (auto it = dbs->begin(); it != dbs->end(); it++) {
				auto db = it->second;
				db->persistIndex();
			}
		}
	}
}

int FaissServiceImpl::InitServer() {
	cudaSetDevice(0);
	m_resources = new StandardGpuResources;
	if (NULL == m_resources) {
		return -1;
	}
	
	m_lock = new WfirstRWLock;
	if (NULL == m_lock) {
		return -1;
	}

	//加载本地已有的db
	int rc = LoadLocalDBs();
	if (0 != rc) {
		return rc;
	}
	return 0;
}

int FaissServiceImpl::LoadLocalDBs() {
	LOG(INFO) << "load local dbs";
	MDB_txn *txn = NULL;
	MDB_cursor *cursor;
	int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);
	MDB_val key, data;
	
	size_t len = 128;
	char _key[len] = {'\0'};

	key.mv_size = len;
	key.mv_data = _key;
	
	rc = mdb_cursor_open(txn, *m_dbi, &cursor);
	int prefixLen = SPrefix.length();
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		std::ostringstream oss;
		std::string keyStr, valStr;
		keyStr.append((char*)key.mv_data, key.mv_size);
		oss << "record:" << keyStr;
		size_t pos = keyStr.find(SPrefix.c_str());
		if (keyStr.length() < SPrefix.length() + 1 ||
				pos == std::string::npos) {
			oss << " msg:invalid key";
			LOG(WARNING) << oss.str();
			continue;
		}
		std::string dbName = keyStr.substr(pos + SPrefix.length());
		oss << " db_name:" << dbName;
		
		valStr.append((char*)data.mv_data, data.mv_size);
		std::string modelPath, sizeStr;
		pos = valStr.find(SDivide.c_str());
		size_t maxSize = DefaultDBSize;
		if (pos == std::string::npos) {
			modelPath = valStr;
		} else if (pos > 0) {
			sizeStr = valStr.substr(pos + SDivide.length());
			maxSize = atoi(sizeStr.c_str());
			modelPath = valStr.substr(0, pos);
		}
		oss << " modelPath:" << modelPath 
			<< " maxSize:" << maxSize;
		//插入新的db
		FaissDB *db = new FaissDB(dbName, modelPath, maxSize);
		int rc = db->reload(m_resources);
		oss << " res:" << rc;
		if (rc == ErrorCode::OK) {
			dbs[dbName.c_str()] = db;
			LOG(INFO) << oss.str();
			continue;
		} else if (rc == ErrorCode::NOT_FOUND) {
			//TODO 都不存在,则删除该条记录
			oss << " error_msg:" << "db not exist";
			LOG(WARNING) << oss.str();
			continue;
		} else {
			oss << " error_msg:" << "internal error!";
			LOG(ERROR) << oss.str();
			return rc;
		}
	}
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);	

	return 0;
}

FaissServiceImpl::FaissServiceImpl():LmDB(SGlobalDBName),
	m_resources(NULL), m_lock(NULL) {
	int rc = InitServer();
	if (rc != 0) {
		LOG(FATAL) << "initialize FaissServiceImpl failed:" << rc;
		exit(-1);
	}
}

FaissServiceImpl::~FaissServiceImpl() {
	delete m_resources;
}
