#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"
#include "core_db.h"
#define debug 1
Status FaissServiceImpl::HSearch(ServerContext* context,
		const ::faiss_server::HSearchRequest* request, 
		::faiss_server::HSearchResponse* response) {
	printf("---------------HSearch--------------\n");
	response->set_request_id(request->request_id());
	std::string dbName = request->db_name();
	std::map<std::string, FaissDB*>::iterator it;
	it = dbs.find(dbName);
	if (it == dbs.end()) {
		response->set_error_code(grpc::StatusCode::NOT_FOUND);
		response->set_error_msg("dbname not found");
		return Status::OK;
	}
	
	size_t topk = request->top_k();
	if (topk == 0) {
		topk = 5;
	}
	std::string feaStr = request->feature();
	if (feaStr.length() < 1 || topk > 10) {
		response->set_error_code(INVALID_ARGUMENT);	
		response->set_error_msg("invalid argument");	
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
	if (feaLen != d) {
		response->set_error_code(DIMENSION_NOT_EQUAL);	
		response->set_error_msg("request feature dimension is not equal to database");	
		return Status::OK;
	}
	printf ("Searching the %ld nearest neighbors in the index\n", topk);

	std::vector<faiss::Index::idx_t> nns(topk);
	std::vector<float>               dis(topk);

	//read lock
	{
		unique_readguard<WfirstRWLock> readlock(*(db->lock));
		index->search (1, (float*)feaStr.data(), topk, dis.data(), nns.data());
	}
	
	for (int j = 0; j < topk; j++) {
		if (db->inBlackList(nns[j])) {
			continue;
		}	
		if (dis[j] > EuclidThresh) {
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
				return Status::OK;
			}
		}
		printf("id:%ld, cosine dist: %f\n", nns[j], dist);
		auto rs = response->add_results();
		rs->set_score(dist);
		rs->set_id(nns[j]);
	}
	response->set_error_code(OK);
#if debug 
	for (int j = 0; j < topk; j++) {
		printf ("%7ld ", nns[j]);
	}
	printf ("\n     dis: ");
	for (int j = 0; j < topk; j++) {
		printf ("%7g ", dis[j]);
	}
	printf ("\n");
#endif
	return Status::OK;
}
