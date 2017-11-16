#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"

Status FaissServiceImpl::HSet(ServerContext* context, 
		const ::faiss_server::HSetRequest* request, 
		::faiss_server::HSetResponse* response) {
	printf("---------------HSet--------------\n");
	response->set_request_id(request->request_id());
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);
	if (it == dbs.end()) {
		response->set_error_code(NOT_FOUND);	
		response->set_error_msg("DB NOT FOUND");	
		return Status::OK;
	}

	std::string feaStr = request->feature();
	if (feaStr.length() < 1) {
		response->set_error_code(INVALID_ARGUMENT);	
		response->set_error_msg("INVALID_ARGUMENT: feature");	
		return Status::OK;
	}
	FaissDB *db = it->second;
	auto index = db->index;

	int feaLen = feaStr.length() / sizeof(float);
	int d = index->d;
	if (feaLen != d) {
		response->set_error_code(DIMENSION_NOT_EQUAL);	
		response->set_error_msg("request feature dimension is not equal to database");	
		return Status::OK;
	}

	long id = 0;
	float *p = (float*)feaStr.data();
	//check data content
	bool isValid = true;
	for (int i = 0; i < d; i ++) {
		if (p[i] > 1.0 || p[i] < -1.0) {
			isValid = false;
			break;
		}
	}
	if (!isValid) {
		response->set_error_code(INVALID_ARGUMENT);	
		response->set_error_msg("request feature is invalid");	
		return Status::OK;
	}
	int rc = db->addFeature((float*)feaStr.data(), d, &id);
	if (rc != 0) {
		response->set_error_code(rc);	
		response->set_error_msg("add feature failed");	
		return Status::OK;
	}
	response->set_error_code(OK);	
	response->set_id(id);
	printf("new id:%ld, index total:%ld, add key ok\n",id, index->ntotal);
	return Status::OK; 
}

//support idempotent delete
Status FaissServiceImpl::HDel(ServerContext* context,
		const ::faiss_server::HGetDelRequest* request,
		::faiss_server::EmptyResponse* response) {
	printf("---------------HDel--------------\n");
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);

	if (it == dbs.end()) {
		std::cout << "not found" <<std::endl;
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("not found");
		response->set_request_id(request->request_id());
		return Status::OK;
	}

	size_t id = request->id();

	FaissDB *db = it->second;
	auto index = db->index;


	//not support remove, remove 会涉及memmov这个数组,时间复杂度很高
	//faiss::IDSelectorRange range(1001,1002);
	//index->remove_ids(range);	

	int rc = db->delFeature(id);
	printf("delete feature from mongo %d\n", rc);
	if (rc == grpc::StatusCode::ALREADY_EXISTS) {
		//found
		printf("already delete\n");
		response->set_error_code(ALREADY_EXISTS);
		response->set_error_msg("already deleted");
		response->set_request_id(request->request_id());
		return Status::OK;
	}

	printf("index ntotal:%ld\n", index->ntotal);
	response->set_error_code(rc);
	response->set_request_id(request->request_id());
	return Status::OK; 
}
Status FaissServiceImpl::HGet(ServerContext* context,
		const ::faiss_server::HGetDelRequest* request,
		::faiss_server::HGetResponse* response) {
	printf("---------------HGet--------------\n");
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);

	if (it == dbs.end()) {
		std::cout << "db not found" <<std::endl;
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("NOT_FOUND");
		response->set_request_id(request->request_id());
		return Status::OK;
	}

	auto db = it->second;
	size_t feaLen = db->index->d;
	float *feature = NULL;
	int rc = db->getFeature(request->id(), &feature, &feaLen);

	if (MDB_NOTFOUND == rc) {//not found
		std::cout << "feature not found" <<std::endl;
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("NOT_FOUND");
		response->set_request_id(request->request_id());
		return Status::OK;
	} else if (rc != 0) {
		std::cout << "internal error" <<std::endl;
		response->set_error_code(rc);
		response->set_error_msg("internal error");
		response->set_request_id(request->request_id());
		return Status::OK;
	}

	response->set_error_code(0);
	response->set_feature(feature, feaLen*sizeof(float));
	response->set_dimension(feaLen);

	response->set_request_id(request->request_id());
	return Status::OK;
} 
