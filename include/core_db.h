#ifndef CORE_DB_H
#define CORE_DB_H

#include "share_lock.h"
#include "utils.h"
#include <atomic>
#include <sstream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "faiss_def.grpc.pb.h"
#include "faiss/gpu/StandardGpuResources.h"
#include "faiss/gpu/GpuIndexIVFPQ.h"
#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/IndexFlat.h"
#include "faiss/index_io.h"
#include "faiss/AuxIndexStructures.h"
#include "lmdb/lmdb.h"
using namespace faiss;
using namespace faiss::gpu;

class LmDB {
	public:
		//database name
		std::string dbName;
		std::string lmdbPath;
		LmDB(std::string &dbName);
		~LmDB();
	private:
		int initLmdb();
	
	protected:
		MDB_env *m_env;
		MDB_dbi *m_dbi;
			
		int lmdbSet(char *key, char *val);
		int lmdbSet(char *key, void *val, int len);
		int lmdbSet(char *key1, void *val1, int len1, char *key2, void *val2, int len2); 

		int lmdbDel(char *key);
		
		int lmdbGet(char *key, std::string *val, int *val_len);
		int lmdbGet(char *key, void **val, int *val_len);

};
class FaissDB:public LmDB {
	public:

		/**
		 * dbName: 业务层数据库名称
		 *		faiss index 持久化默认地址： ./data/${dbName}.index
		 *		faiss raw feature 持久化默认地址: ./data/${dbName}/data.mdb
		 * modelPath: faiss index使用的模型路径
		 *
		 */
		FaissDB(std::string &dbName, std::string &modelPath, size_t maxSize);
		~FaissDB();

		//initialize
		int init();

		//add feature
		int addFeature(float *feature, const size_t len, long *feaID);

		//get feature
		int getFeature(const size_t feaID, float **feature, size_t *len);

		//delete feature
		int delFeature(const size_t feaID);

		//persist faiss index 
		int persistIndex();
		
		//reload db will do the following tasks:
		//1) load blackList from lmdb
		//2) load cpu index from $(dbName).index file
		//3) remove blackList ids from cpu index if the list is not null
		//4) load gpu index from cpu index
		//5) update some status, maxID,maxPersistID .et
		//5) load lost feature to gpu index
		//6) persist the index
		int reload(StandardGpuResources *rs);
		
		//check weather the given feaID is in the blackList
		bool inBlackList(long feaId);

		//load faiss index
		int loadIndex(StandardGpuResources *rs, std::string &idxPath);

		//计算输入p1与lmdb中的某个ID的cosine距离		
		int calcCosine(const float *p1, long id, float *dis);
	
	private:
		//get stored maxPersistID or maxID
		int getID(char *key, size_t *id);

		//store blackList to lmdb
		//should call with a writelock
		int storeBlackList(char *key);
		
		//load blackList from lmdb	
		//should call with a writelock
		int loadBlackList(char *key);
		
		//从lmdb中加载未持久化的特征到index中
		int loadLostIndex();
		
	public:
		GpuIndexIVFPQ *index;
		
		//max size of features
		//ntotal of index should less than maxSize
		size_t maxSize;

		//index persist path
		std::string persistPath;
		
		//index model path
		std::string modelPath;

		//currently the max persist feature id of the faiss database 
		size_t maxPersistID;

		//currently the max feature id in the faiss database
		//maxID start from 1
		std::atomic<size_t> maxID; 
		
		//feature read,write flag, writeFlag = true if write occurs;
		//persist will timely check this flag,
		//and persist index when the flag is true;
		//writeFlag will set false after persist.
		std::atomic<bool> writeFlag; 

		//the deleted ids are stored in blackList. Compack the data when the length
		//of blackList is sufficiently large.
		std::set<long> blackList;

		//share lock for index
		WfirstRWLock *lock;
};

#endif
