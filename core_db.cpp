#include "core_db.h"
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

int LmDB::lmdbDel(const char *_key) {
	MDB_txn *txn;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);
	MDB_val key;

	key.mv_size = strlen(_key);
	key.mv_data = const_cast<char*>(_key);

	rc = mdb_del(txn, *m_dbi, &key, NULL);
	if (MDB_NOTFOUND == rc) {
		mdb_txn_abort(txn);	
		return rc;
	} 
	rc = mdb_txn_commit(txn);
	return rc;
}

int LmDB::lmdbSet(const char *key1, void *val1, int len1, const char *key2, void *val2, int len2) {
	
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);
	VLOG(50) << "mdb_txn_begin res:" << rc;
	MDB_val key, data;
	key.mv_size = strlen(key1);
	key.mv_data = const_cast<char*>(key1);
	data.mv_size = len1;
	data.mv_data = val1;
	rc = mdb_put(txn, *m_dbi, &key, &data, 0);
	
	key.mv_size = strlen(key2);
	key.mv_data = const_cast<char*>(key2);
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

int LmDB::lmdbSet(const char *_key, void *_val, int len) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);

	VLOG(50) << "mdb_txn_begin res:" << rc;

	MDB_val key, data;
	key.mv_size = strlen(_key);
	key.mv_data = const_cast<char*>(_key);
	
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

int LmDB::lmdbSet(const char *_key, char *_val) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, 0, &txn);

	MDB_val key, data;
	key.mv_size = strlen(_key);
	key.mv_data = const_cast<char*>(_key);
	
	data.mv_size = strlen(_val);
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
int LmDB::lmdbGet(const char *_key, void **val, int *val_len) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);

	MDB_val key, data;
	key.mv_size = strlen(_key);
	key.mv_data = const_cast<char*>(_key);
	
	rc = mdb_get(txn, *m_dbi, &key, &data);
	mdb_txn_abort(txn);	
	
	if (rc == 0) {
		*val = data.mv_data;
		*val_len = data.mv_size;
	}
	return rc;
}
int LmDB::lmdbGet(const char *_key, std::string *_val, int *val_len) {
	MDB_txn *txn = NULL;
	int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);

	MDB_val key, data;
	key.mv_size = strlen(_key);
	key.mv_data = const_cast<char*>(_key);
	
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
