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
		printf("----------persist index-----------\n");
		std::this_thread::sleep_for (std::chrono::seconds(duration));
		
		if (NULL == handle) {
			continue;
		}
		{
			unique_readguard<WfirstRWLock> readlock(*(handle->m_lock));
			auto *dbs = &(handle->dbs);
			for (auto it = dbs->begin(); it != dbs->end(); it++) {
				printf("%s\n", (it->first).c_str());
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
	printf("---------------loadLocalDBs-----------\n");
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
		std::string keyStr, valStr;
		keyStr.append((char*)key.mv_data, key.mv_size);
		printf("key:%s, keyLen:%d, SprefixLen:%d\n", keyStr.c_str(), key.mv_size, prefixLen);
		size_t pos = keyStr.find(SPrefix.c_str());
		if (keyStr.length() < SPrefix.length() + 1 ||
				pos == std::string::npos) {
			printf("invalid key\n");
			continue;
		}
		std::string dbName = keyStr.substr(pos + SPrefix.length());
		printf("db_name:%s\n", dbName.c_str());
		
		valStr.append((char*)data.mv_data, data.mv_size);
		printf("val:%s, valLen:%d\n", valStr.c_str(), valStr.length());

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
		printf("modelPath:%s, maxSize:%ld, pos:%ld\n", modelPath.c_str(), maxSize, pos);
		//插入新的db
		FaissDB *db = new FaissDB(dbName, modelPath, maxSize);
		int rc = db->reload(m_resources);
		printf("reload database '%s' res:%d\n", dbName.c_str(), rc);
		if (rc == ErrorCode::OK) {
			dbs[dbName.c_str()] = db;
			continue;
		} else if (rc == ErrorCode::NOT_FOUND) {
			printf("database '%s' not exist\n", dbName.c_str());
			//TODO 都不存在,则删除该条记录
			continue;
		} else {
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
		printf("initialize FaissServiceImpl failed:%d\n", rc);
		exit(-1);
	}
}

FaissServiceImpl::~FaissServiceImpl() {
	delete m_resources;
}
