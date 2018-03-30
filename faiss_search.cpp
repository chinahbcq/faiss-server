#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"
#include "core_db.h"

Status FaissServiceImpl::HSearch(ServerContext* context,
		const ::faiss_server::HSearchRequest* request, 
		::faiss_server::HSearchResponse* response) {
	std::ostringstream oss;
	oss << "request_id:" << request->request_id()
		<< " cmd:HSearch"
		<< " db_name:" << request->db_name()
		<< " top_k:" << request->top_k()
		<< " dist_type:" << request->distance_type();
	
	response->set_request_id(request->request_id());
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);
	if (it == dbs.end()) {
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("dbname not found");
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}
	
	size_t topk = request->top_k();
	if (topk < 1) {
		topk = 3;
	} else if (topk > 5) {
		topk = 5;
	}
	std::string feaStr = request->feature();
	if (feaStr.length() < 1 || topk > 10) {
		response->set_error_code(INVALID_ARGUMENT);	
		response->set_error_msg("invalid argument");
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}
	int disType = faiss_server::HSearchRequest::Euclid;
	if (request->distance_type() == faiss_server::HSearchRequest::Cosine) {
		disType = faiss_server::HSearchRequest::Cosine;
	}

	FaissDB *db = it->second;
	auto index = db->index;

	int feaLen = feaStr.length() / sizeof(float);
	int d = index->d;
	oss << " db_dim:" << d
		<< " req_dim:" << feaLen;
	if (feaLen != d) {
		response->set_error_code(DIMENSION_NOT_EQUAL);	
		response->set_error_msg("request feature dimension is not equal to database");	
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}

	int respCount = 0;
	int searchTopK = topk * 2;
	VLOG(50) << "Searching the "<< searchTopK << " nearest neighbors in the index";

	std::vector<faiss::Index::idx_t> nns(searchTopK);
	std::vector<float>               dis(searchTopK);

	//read lock
	{
		unique_readguard<WfirstRWLock> readlock(*(db->lock));
		index->search (1, (float*)feaStr.data(), searchTopK, dis.data(), nns.data());
	}
	
	
	for (int j = 0; j < searchTopK && respCount < topk; j++) {
		if (db->inBlackList(nns[j])) {
			continue;
		}	
		if (dis[j] > globalConfig.EuclidThresh) {
			break;
		}
		float dist = dis[j];
		
		if (disType == faiss_server::HSearchRequest::Cosine) {
			//compute cosine distance
			int rc = db->calcCosine((float*)feaStr.data(), nns[j], &dist);
			if (rc == MDB_NOTFOUND) {
				continue;
			} else if (rc != 0) {
				response->set_error_code(rc);
				response->set_error_msg("calculate cosine distance failed");
				oss << " error_code:" << response->error_code()
					<< " error_msg:" << response->error_msg();
				LOG(WARNING) << oss.str();
				return Status::OK;
			}
		}
		oss << " " << nns[j] << ":" << dist;
		auto rs = response->add_results();
		rs->set_score(dist);
		rs->set_id(nns[j]);
		respCount ++;
	}
	response->set_error_code(OK);
	oss << " error_code:0";
	LOG(INFO) << oss.str();

	return Status::OK;
}
