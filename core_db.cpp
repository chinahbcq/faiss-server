#include "core_db.h"

FaissDB::FaissDB(std::string &db_name, 
		std::string &model_path,
		size_t max_size):LmDB(db_name),modelPath(model_path), maxSize(max_size) {
	lock = new WfirstRWLock;
	index = NULL;
	persistPath = "./data/" + db_name + ".index";
	maxPersistID = 0;
	maxID = 0;
	writeFlag = true;
}
int FaissDB::reload(StandardGpuResources *rs) {
	std::ostringstream oss;
	bool rt = checkPathExists(this->persistPath);	
	oss << "persist_path:" << this->persistPath
		<< " is_exist:" << rt;
	int rc = 0, rc2 = 0;
	if (rt) {//持久化文件存在 
		//加载黑名单
		rc2 = this->loadBlackList(SBlackListKey);
		if (rc2 != MDB_NOTFOUND && rc2 !=  0) { //没有黑名单可加载
			oss << " error_msg: load black_list failed:" << rc;
			LOG(WARNING) << oss.str();
			return ErrorCode::INTERNAL;
		}
		oss << " load_blacklist:OK black_list_len:" << blackList.size();

		rc = this->loadIndex(rs, this->persistPath);
		if (rc != 0) {
			oss << " load_index:failed,resp:" << rc;
			LOG(WARNING) << oss.str();
			return rc;
		} 
		oss << " load_index:OK";
		// 需要将db的其他信息maxID,maxPersistID读取出来
		rc = this->loadLostIndex();
		oss << " load_lost_index:" << (rc == 0 ? "OK":"FAILED");
		LOG(INFO) << oss.str();
		return rc;
	}
	rt = checkPathExists(modelPath);	
	oss << " model_path:" << modelPath
		<< " is_exist:" << rt;
	if (rt) {//训练模型文件存在 
		rc = this->loadIndex(rs, modelPath);
		oss << " load_index:" << (rc == 0 ? "OK":"FAILED");
		if (rc != 0) {
			LOG(WARNING) << oss.str();
			return rc;
		}
		// 考虑这种情况下，也可能存在删除黑名单,这将是非法数据
		rc = lmdbDel(SBlackListKey);
		if (rc == MDB_NOTFOUND) {
			LOG(INFO) << oss.str();	
			return 0;
		} else if (rc != 0) {
			oss << " delete_black_list:" << rc;
			LOG(WARNING) << oss.str();	
			return rc;
		}
		LOG(INFO) << oss.str();
		return 0;
	}
	LOG(WARNING) << oss.str();
	return ErrorCode::NOT_FOUND;
}
int FaissDB::loadLostIndex() {
	if (NULL == this->index) {
		return -1;
	}
	size_t maxID, maxPersistID;
	int rc1, rc2;
	rc1 = this->getID(SPersistIDKey, &maxPersistID);
	rc2 = this->getID(SMaxIDKey, &maxID);
	if (rc1 != 0 || rc2 != 0) {
		return -1;
	}

	//读取成功
	this->maxPersistID = maxPersistID;
	this->maxID = maxID;
	std::ostringstream oss;
	//db从lmdb中加载未持久化的数据
	oss << "maxID:"<< maxID
		<<" maxPersistID:" << maxPersistID;
	
	//index中最新的id <= lmdb中持久化的较为旧的id，则不做操作，
	//认为同步，小的部分会被覆盖
	if (maxID <= maxPersistID) {
		writeFlag = false;
		oss << " error_msg:need no index load";
		LOG(INFO) << oss.str();
		return 0;
	}

	//添加index中未被持久化的数据，这些数据在lmdb中
	float *feature = NULL;
	for (size_t id = maxPersistID + 1; id <= maxID; id ++) {
		size_t feaLen = index->d;
		rc1 = getFeature(id, &feature, &feaLen); 
		if (rc1 == MDB_NOTFOUND) {
			LOG(WARNING) << "feature_id("<< id <<"):not found";
			continue;
		} else if (rc1 != 0) {
			oss << "feature_id("<< id <<"):get failed:"<< rc1;
			LOG(WARNING) << oss.str();
			return rc1;
		}
		if (feaLen != index->d) {
			oss << " need_fea_len:" << index->d
				<< " get_fea_len:" << feaLen;
			delete feature;
			feature = NULL;
			LOG(WARNING) << oss.str();
			return -1;
		}
		//将特征添加进index	
		long _id = (long)id;
		index->add_with_ids(1, feature, &_id);
	}
	
	writeFlag = true;
	oss << " load_lost_index:OK";
	LOG(INFO) << oss.str();
	return 0;
}
int FaissDB::loadIndex(StandardGpuResources *resources,std::string &idxPath) {
	std::ostringstream oss;
	try {
		GpuIndexIVFPQConfig config;
		config.device = 0;
		faiss::Index *file_index = faiss::read_index(idxPath.c_str());
		faiss::IndexIVFPQ *cpu_index = dynamic_cast<faiss::IndexIVFPQ *>(file_index);
		oss << "idx_path:" << idxPath
			<< " black_size:" << blackList.size()
			<< " cpu_ntotal:" << cpu_index->ntotal;
		if (blackList.size() > 0) {//若黑名单不为空
			auto start = blackList.begin();
			auto end = blackList.end();
			end = --end;
			
			faiss::IDSelectorRange range(*start, 1 + *end);
			cpu_index->remove_ids(range);	
	
			oss << " black_start:" << *start
				<< " black_end:" << 1+*end
				<< " new_cpu_ntotal:" << cpu_index->ntotal;

			//将cpu_index 再持久化一次
			write_index(cpu_index, (this->persistPath).c_str());

			//需要跟index一起，将blackList持久化,否则出现数据不一致
			blackList.clear();
			int rc = lmdbDel(SBlackListKey);
			if (rc == MDB_NOTFOUND) {
				//nothing
			} else if (rc != 0) {
				oss << " error_msg:delete blackList from lmdb failed:" << rc;
				LOG(WARNING) << oss.str();
				return rc;
			}
			oss << " delete_black_list:ok";
		}

		this->index = new GpuIndexIVFPQ(resources, cpu_index, config);
		this->index->setNumProbes(NProbes);

		oss << " dim:" << index->d
			<< " gpu_ntotal:" << index->ntotal;
		delete file_index;
		file_index = NULL;
		LOG(INFO) << oss.str();
		return 0;
	} catch(...) {
		
		oss << " error_msg:load index '"<< idxPath <<"' failed";
		
		if (this->index != NULL) {
			delete this->index;
		}
		this->index = NULL;
		LOG(WARNING) << oss.str();
	}
	return -1;
}

FaissDB::~FaissDB() {
	delete this->index;
	this->index = NULL;
	//lock can't be delete;
	
	//remove index file
	if (!checkPathExists(persistPath)) {
		return;
	}
	if (remove(persistPath.c_str())) {
		LOG(WARNING) << "delete faiss index file failed:" << persistPath;
	} else {
		LOG(INFO) << "delete faiss index file OK:" << persistPath;
	}
}

bool FaissDB::inBlackList(long feaID) {
	if (blackList.find(feaID) != blackList.end()) {
		return true;
	}
	return false;
}

int FaissDB::calcCosine(const float *p1, long id, float *dis) {
	float *feature = NULL;
	size_t feaLen = 0;
	int rc = getFeature(id, &feature, &feaLen); 
	if (rc != 0) {
		return rc;
	}
	if (feaLen != index->d) {
		return DIMENSION_NOT_EQUAL;
	}

	*dis = cosine(p1, feature, index->d);
	return 0;
}

LmDB::LmDB(std::string &db_name):dbName(db_name) {
	m_env = NULL;
	m_dbi = new MDB_dbi;
	
	lmdbPath = "./data/" + dbName;

	int rc = initLmdb();
	if (rc != 0) {
		LOG(FATAL) << "init lmdb failed";
		exit(-1);
	}
}
LmDB::~LmDB() {
	mdb_dbi_close(m_env, *m_dbi);
	delete m_dbi;
	mdb_env_close(m_env);
	
	//删除raw数据文件
	std::string dataFile, lockFile;
	dataFile = lmdbPath + "/data.mdb";
	lockFile = lmdbPath + "/lock.mdb";
	if (remove(dataFile.c_str())) {
		LOG(WARNING) << "delete lmdb data file failed:" << dataFile;
	} else {
		LOG(INFO) << "delete lmdb data file OK:" << dataFile;
	}
	
	if (remove(lockFile.c_str())) {
		LOG(WARNING) << "delete  lmdb lock file failed:" << lockFile;
	} else {
		LOG(INFO) << "delete lmdb lock file OK:" << lockFile;
	}

	//删除raw数据文件
	if (rmdir(lmdbPath.c_str()) != 0) {
		LOG(WARNING) << "rm lmdb dir failed:" << lmdbPath;
	} else {
		LOG(INFO) << "rm lmdb dir OK:" << lmdbPath;
	}
}
int LmDB::initLmdb() {
	if (dbName.length() < 1) {
		LOG(WARNING) << "dbName is empty";
		return -1;
	}
	int rc = 0;
	rc = mdb_env_create(&m_env);
	rc = mdb_env_set_maxreaders(m_env, 100);
	rc = mdb_env_set_mapsize(m_env, 10485760);
	
	std::ostringstream oss;
	oss << "db_name:" << dbName
		<< " lmdb_path:" << lmdbPath;
	if (!mkFolder(lmdbPath)) {
		oss << " error_msg: create dbPath failed";
		LOG(WARNING) << oss.str();
		return -1;
	}
	rc = mdb_env_open(m_env, lmdbPath.c_str(), MDB_FIXEDMAP, 0664);

	oss << " mdb_env_open:" << rc;
	
	//open m_dbi	
	MDB_txn *txn = NULL;
	rc = mdb_txn_begin(m_env, NULL, 0, &txn);
	rc = mdb_dbi_open(txn, NULL, 0, m_dbi);
	oss << " mdb_dbi_open:" << rc;
	mdb_txn_abort(txn);
	LOG(INFO) << oss.str();
	return rc;
}
	
int FaissDB::addFeature(float *feature, const size_t len, long *id) {
	//add feature to index
	{
		unique_writeguard<WfirstRWLock> writelock(*(this->lock));
		//check maxSize exceeds
		if (index->ntotal > this->maxSize) {
			return EXCEEDS_MAX_SIZE;	
		}
		(this->maxID).fetch_add(1, std::memory_order_relaxed);
		*id = (this->maxID).load(std::memory_order_relaxed);
		this->index->add_with_ids(1, feature, id);
		this->writeFlag = true;
	}
	//add feature and maxID to lmdb
	char feaID[20] = {'\0'};
	char maxIDVal[20] = {'\0'};
	encodeID(feaID, *id);

	sprintf(maxIDVal, "%ld", *id);
	char *maxIDKey = SMaxIDKey;
	return lmdbSet(feaID, feature, sizeof(float)*len, 
			maxIDKey, maxIDVal, strlen(maxIDVal));
}
int FaissDB::getFeature(const size_t feaID, float **feature, size_t *len) {
	char keyData[20] = {'\0'};
	encodeID(keyData, feaID);

	void *fea = NULL;
	int fea_len;
	int rc = this->lmdbGet(keyData, &fea, &fea_len);
	if (rc != 0) {
		return rc;
	}
	*len = fea_len / sizeof(float);
	*feature = (float*) fea;
	return 0;
}
int FaissDB::delFeature(const size_t feaID){
	//检查黑名单是否存在该id
	unique_writeguard<WfirstRWLock> writelock(*(this->lock));
	std::set<long>::iterator it;
	auto blackList = &this->blackList;
	it = blackList->find(feaID);
	if (it != blackList->end()) {
		return grpc::StatusCode::ALREADY_EXISTS;
	}

	char key[20] = {'\0'};
	encodeID(key, feaID);
	int rc = this->lmdbDel(key);
	if (MDB_NOTFOUND == rc) {
		return grpc::StatusCode::ALREADY_EXISTS;
	} else if (rc != 0) {
		return rc;
	}
	//删除成功, 添加黑名单
	blackList->insert(feaID);

	//持久化黑名单
	rc = this->storeBlackList(SBlackListKey);

	return rc;
}

int FaissDB::storeBlackList(char *key) {
	//持久化黑名单
	if (blackList.size() < 1) {
		//将黑名单删除
		int rc = lmdbDel(key);
		return rc;
	}
	std::vector<long> vec(blackList.begin(), blackList.end());
	int rc = lmdbSet(key, vec.data(), sizeof(long)*vec.size());
	return rc;
}

int FaissDB::loadBlackList(char *key) {
	void *ids = NULL;
	int len = 0;
	int rc = lmdbGet(key, &ids, &len);
	if (rc != 0) {
		LOG(WARNING) << "get blackList return:" << rc;
		return rc;
	}

	len = len / sizeof(long);
	
	if (ids == NULL) {
		VLOG(50) << "get blackList return NULL";
		return -1;
	}
	
	blackList = std::set<long> ((long*)ids, (long*)ids + len);

	return 0;
}
int FaissDB::getID(char *key, size_t *id) {
	if (NULL == key) {
		return -1;
	}
	std::string str;
	int len = 20;
	int rc = lmdbGet(key, &str, &len);
	if (MDB_NOTFOUND == rc) {
		VLOG(50) << key << ":not found";
		*id = 0;
		return 0;
	} else if (rc != 0) {
		LOG(WARNING) << "get '" << key << "' failed:" << rc;
		return rc;
	} else if (len < 1 || len > 10 /*代表10^10的数量大小*/) {
		LOG(WARNING)<< "get '" << key << "' failed: value to long(" << len << ")";
		return -1;
	}
	char data[20] = {'\0'};
	snprintf(data, len+1, "%s", str.c_str());
	
	for (int i = 0; i < len; i++) {
		if (data[i] > '9' || data[i] < '0') {
			LOG(WARNING) << "get '" << key <<"' failed: bad data("<< data << ")";
			return -1;
		}
	}

	*id = atoi(data);
	return 0;
}

int FaissDB::persistIndex() {
	size_t persistID = 0;
	std::ostringstream oss;
	{
		unique_writeguard<WfirstRWLock> writelock(*(this->lock));
		if (!this->writeFlag) { //writeFlag == true
			VLOG(50) << "db_name:" << this->dbName << "need no persist";
			return 0;
		}
		
		auto cpu_index = faiss::gpu::index_gpu_to_cpu (this->index);
		oss << "cmd:auto_persist_index"
			<< " db_name:" << this->dbName
			<< " max_persist_id:" << this->maxPersistID
			<< " max_id:" << (this->maxID).load(std::memory_order_relaxed);

		write_index(cpu_index, (this->persistPath).c_str());
		//TODO 将黑名单中的ids顺便删除再持久化
		//同时将index重新reload一次
		this->writeFlag = false;
		this->maxPersistID = (this->maxID).load(std::memory_order_relaxed);
		persistID = this->maxPersistID;
		delete cpu_index;
		oss << " persist_path:" << this->persistPath;
	}

	//持久化index时将persistID写入lmdb中
	//add时，将maxID写入lmdb中， del不需要写
	char val[20] = {'\0'};

	sprintf(val, "%ld", persistID);
	int rc = lmdbSet(SPersistIDKey, val);
	oss << " set_lmdb:" << rc;
	LOG(INFO) << oss.str();
	return 0;
}
int LmDB::lmdbDel(char *_key) {
	MDB_txn *txn;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);
	MDB_val key;

	key.mv_size = std::strlen(_key);
	key.mv_data = _key;

	rc = mdb_del(txn, *m_dbi, &key, NULL);
	LOG(INFO) << "delete" << _key <<" resp:" << rc;
	if (MDB_NOTFOUND == rc) {
		mdb_txn_abort(txn);	
		return rc;
	} 
	rc = mdb_txn_commit(txn);
	return rc;
}

int LmDB::lmdbSet(char *key1, void *val1, int len1, char *key2, void *val2, int len2) {
	
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);
	VLOG(50) << "mdb_txn_begin res:" << rc;
	MDB_val key, data;
	key.mv_size = strlen(key1);
	key.mv_data = key1;
	data.mv_size = len1;
	data.mv_data = val1;
	rc = mdb_put(txn, *m_dbi, &key, &data, 0);
	
	key.mv_size = strlen(key2);
	key.mv_data = key2;
	data.mv_size = len2;
	data.mv_data = val2;
	rc = mdb_put(txn, *m_dbi, &key, &data, 0);
	
	rc = mdb_txn_commit(txn);
	
	if (rc != 0) {
		LOG(WARNING) << "add multi-data to lmdb failed,id1:" << key1 << " id2:" << key2;
		return rc;
	}
	VLOG(50) << "add multi-data to lmdb OK,id1:" << key1 << " id2:" << key2;
	return 0;
}

int LmDB::lmdbSet(char *_key, void *_val, int len) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);

	VLOG(50) << "mdb_txn_begin res:" << rc;

	MDB_val key, data;
	key.mv_size = std::strlen(_key);
	key.mv_data = _key;
	
	data.mv_size = len;
	data.mv_data = _val;

	rc = mdb_put(txn, *m_dbi, &key, &data, 0);
	rc = mdb_txn_commit(txn);
	
	if (rc != 0) {
		LOG(WARNING) << "store " << _key << " to lmdb failed";
		return rc;
	}
	VLOG(50) << "store " << _key << " to lmdb OK";
	return 0;
}

int LmDB::lmdbSet(char *_key, char *_val) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);

	MDB_val key, data;
	key.mv_size = std::strlen(_key);
	key.mv_data = _key;
	
	data.mv_size = std::strlen(_val);
	data.mv_data = _val;

	rc = mdb_put(txn, *m_dbi, &key, &data, 0);
	rc = mdb_txn_commit(txn);
	
	if (rc != 0) {
		LOG(WARNING) << "store " << _key << " to lmdb failed";
		return rc;
	}
	VLOG(50) << "store " << _key << " to lmdb OK";
	return 0;
}
int LmDB::lmdbGet(char *_key, void **val, int *val_len) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);

	MDB_val key, data;
	key.mv_size = std::strlen(_key);
	key.mv_data = _key;
	
	rc = mdb_get(txn, *m_dbi, &key, &data);
	mdb_txn_abort(txn);	
	
	if (rc == 0) {
		*val = data.mv_data;
		*val_len = data.mv_size;
	}
	return rc;
}
int LmDB::lmdbGet(char *_key, std::string *_val, int *val_len) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);

	MDB_val key, data;
	key.mv_size = std::strlen(_key);
	key.mv_data = _key;
	
	data.mv_size = *val_len; 
	char buf[*val_len] = {'\0'};
	data.mv_data = buf;

	rc = mdb_get(txn, *m_dbi, &key, &data);
	mdb_txn_abort(txn);	
	
	if (rc == 0) {
		*_val = (char*)data.mv_data;	
		*val_len = data.mv_size;
	}
	return rc;
}
