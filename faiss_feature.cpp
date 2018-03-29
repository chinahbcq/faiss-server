#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"

Status FaissServiceImpl::HSet(ServerContext* context, 
		const ::faiss_server::HSetRequest* request, 
		::faiss_server::HSetResponse* response) {
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:HSet"
		<< " db_name:" << request->db_name();

	response->set_request_id(request->request_id());
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);
	if (it == dbs.end()) {
		response->set_error_code(NOT_FOUND);	
		response->set_error_msg("DB NOT FOUND");
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return Status::OK;
	}

	std::string feaStr = request->feature();
	if (feaStr.length() < 1) {
		response->set_error_code(INVALID_ARGUMENT);	
		response->set_error_msg("INVALID_ARGUMENT: feature");
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return Status::OK;
	}
	FaissDB *db = it->second;
	auto index = db->index;

	int feaLen = feaStr.length() / sizeof(float);
	int d = index->d;
	if (feaLen != d) {
		response->set_error_code(DIMENSION_NOT_EQUAL);	
		response->set_error_msg("request feature dimension is not equal to database");	
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
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
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}
	int rc = db->addFeature((float*)feaStr.data(), d, &id);
	if (rc != 0) {
		response->set_error_code(rc);	
		response->set_error_msg("add feature failed");	
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}
	response->set_error_code(OK);	
	response->set_id(id);
	oss << " new_fea_id:" << id
		<< " ntotal:" << index->ntotal  
		<< " error_code:" << response->error_code();
	LOG(INFO) << oss.str();
	return Status::OK; 
}

//support idempotent delete
Status FaissServiceImpl::HDel(ServerContext* context,
		const ::faiss_server::HGetDelRequest* request,
		::faiss_server::EmptyResponse* response) {
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:HDel"
		<< " id:" << request->id()
		<< " db_name:" << request->db_name();

	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);

	if (it == dbs.end()) {
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("not found");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return Status::OK;
	}

	size_t id = request->id();

	FaissDB *db = it->second;
	auto index = db->index;


	//not support remove, remove 会涉及memmov这个数组,时间复杂度很高
	//faiss::IDSelectorRange range(1001,1002);
	//index->remove_ids(range);	

	int rc = db->delFeature(id);
	oss << " delete_feature:" << rc;
	if (rc == grpc::StatusCode::ALREADY_EXISTS) {
		//found
		response->set_error_code(ALREADY_EXISTS);
		response->set_error_msg("already deleted");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();	
		return Status::OK;
	}
	
	response->set_error_code(rc);
	response->set_request_id(request->request_id());
	oss << " ntotal:" << index->ntotal
		<< " error_code:" << response->error_code();
	LOG(INFO) << oss.str();
	return Status::OK; 
}
Status FaissServiceImpl::HGet(ServerContext* context,
		const ::faiss_server::HGetDelRequest* request,
		::faiss_server::HGetResponse* response) {
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:HGet"
		<< " id:" << request->id()
		<< " db_name:" << request->db_name();
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);

	if (it == dbs.end()) {
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("NOT_FOUND");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << "db not found";
		LOG(WARNING) << oss.str();	
		return Status::OK;
	}

	auto db = it->second;
	size_t feaLen = db->index->d;
	float *feature = NULL;
	int rc = db->getFeature(request->id(), &feature, &feaLen);

	if (MDB_NOTFOUND == rc) {//not found
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("NOT_FOUND");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << "feature not found";
		LOG(WARNING) << oss.str();	
		return Status::OK;
	} else if (rc != 0) {
		response->set_error_code(rc);
		response->set_error_msg("internal error");
		response->set_request_id(request->request_id());
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}

	response->set_error_code(0);
	response->set_feature(feature, feaLen*sizeof(float));
	response->set_dimension(feaLen);

	response->set_request_id(request->request_id());
	oss << " dim:" << feaLen
		<< " error_code:" << response->error_code();
	LOG(INFO) << oss.str();
	return Status::OK;
} 
