#ifndef FAISS_LOGIC_H
#define FAISS_LOGIC_H

#include <stdlib.h>
#include <stdio.h>
#include <thread>
#include <grpc++/grpc++.h>
#include <pthread.h>
#include "core_db.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using faiss_server::FaissService;

class FaissServiceImpl final : public FaissService::Service, public LmDB {
	private:
		std::map<std::string, FaissDB*> dbs;

		StandardGpuResources *m_resources;
		
		//share lock for dbs
		WfirstRWLock *m_lock;
		
		int InitServer();

		//load dbs from persist storage
		int LoadLocalDBs();
	public:
		FaissServiceImpl();
		
		virtual ~FaissServiceImpl();
	
		//周期持久化faiss index	
		static void PersistIndexPeriod(FaissServiceImpl *handle, const unsigned int duration);	
		
		//注意 修改此处，需要make clean ，再make
		Status Ping(ServerContext* context, const ::faiss_server::PingRequest* request, ::faiss_server::PingResponse* response) override;

		Status DbNew(ServerContext* context, const ::faiss_server::DbNewRequest* request, ::faiss_server::EmptyResponse* response) override;
		
		Status HSet(ServerContext* context, const ::faiss_server::HSetRequest* request, ::faiss_server::HSetResponse* response) override;
		
		Status HDel(ServerContext* context, const ::faiss_server::HGetDelRequest* request, ::faiss_server::EmptyResponse* response) override;
		
		Status HSearch(ServerContext* context, const ::faiss_server::HSearchRequest* request, ::faiss_server::HSearchResponse* response) override;
		
		Status DbList(ServerContext* context, const ::faiss_server::DbListRequest* request, ::faiss_server::DbListResponse* response) override;
		
		Status DbDel(ServerContext* context, const ::faiss_server::DbDelRequest* request, ::faiss_server::EmptyResponse* response) override;
		
		Status HGet(ServerContext* context, const ::faiss_server::HGetDelRequest* request, ::faiss_server::HGetResponse* response) override;
};

#endif
