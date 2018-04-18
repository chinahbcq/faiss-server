#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"
#include "core_db.h"
struct Node {
	::google::protobuf::uint64 id;
  	float score;
};
struct {
	bool operator() (Node node1,Node node2) {
		return node1.score > node2.score;
	}
} SortFunc;
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
	
	size_t topk = request->top_k();
	if (topk < 1) {
		topk = 3;
	} else if (topk > 5) {
		topk = 5;
	}

	int respCount = 0;
	std::vector<Node> cosineNodes;
	int searchTopK = topk * 2;
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
	{
		unique_readguard<WfirstRWLock> readlock(*m_lock);
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
		if (index->ntotal < 1) {
			response->set_error_code(NOT_FOUND);	
			response->set_error_msg("database is empty");	
			oss << " error_code:" << response->error_code()
				<< " error_msg:" << response->error_msg();
			LOG(WARNING) << oss.str();
			return Status::OK;
		}
		VLOG(50) << "Searching the "<< searchTopK << " nearest neighbors in the index";

		std::vector<faiss::Index::idx_t> nns(searchTopK);
		std::vector<float>               dis(searchTopK);

		//write lock
		{
			unique_writeguard<WfirstRWLock> writelock(*(db->lock));
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
				Node node;
				node.score = dist;
				node.id = nns[j];
				cosineNodes.push_back(node);
				respCount ++;
			} else {
				oss << " " << nns[j] << ":" << dist;
				auto rs = response->add_results();
				rs->set_score(dist);
				rs->set_id(nns[j]);
				respCount ++;
			}
		}
	}
	if (respCount < 1) {
		response->set_error_code(NOT_FOUND);
		response->set_error_msg("search no result");
		oss << " error_code:" << response->error_code()
			<< " error_msg:" << response->error_msg();
		LOG(WARNING) << oss.str();
		return Status::OK;
	}
	if (disType == faiss_server::HSearchRequest::Cosine) {
		//sort cosine distance
		std::sort(cosineNodes.begin(),cosineNodes.end(),SortFunc);
		for (auto it = cosineNodes.begin(); it != cosineNodes.end(); ++it) {
			oss << " " << it->id << ":" << it->score;
			auto rs = response->add_results();
			rs->set_score(it->score);
			rs->set_id(it->id);
		}
	}
	response->set_error_code(OK);
	oss << " error_code:0";
	LOG(INFO) << oss.str();

	return Status::OK;
}
