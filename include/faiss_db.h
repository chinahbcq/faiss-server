#ifndef FAISS_DB_H
#define FAISS_DB_H

#include "core_db.h"
#include "faiss_def.grpc.pb.h"
#include "faiss/gpu/StandardGpuResources.h"
#include "faiss/gpu/GpuIndexIVFPQ.h"
#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/IndexFlat.h"
#include "faiss/index_io.h"
#include "faiss/AuxIndexStructures.h"

using namespace faiss;
using namespace faiss::gpu;
class FaissDB:public LmDB {
	public:

		/**
		 * dbName: 业务层数据库名称
		 *		faiss index 持久化默认地址： ./data/${dbName}.index
		 *		faiss raw feature 持久化默认地址: ./data/${dbName}/data.mdb
		 * modelPath: faiss index使用的模型路径
		 * maxSize: max number of features
		 * gpuLock: global lock for all dbs when communicate with GPU
		 */
		FaissDB(std::string &dbName, 
			std::string &modelPath, 
			size_t maxSize,
			WfirstRWLock *gpuLock);
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

		//内部基础状态信息
		void status();
	
	private:
		//get stored maxPersistID or maxID
		int getID(const char *key, size_t *id);

		//store blackList to lmdb
		//should call with a writelock
		int storeBlackList(const char *key);
		
		//load blackList from lmdb	
		//should call with a writelock
		int loadBlackList(const char *key);
		
		//从lmdb中加载未持久化的特征到index中
		int loadLostIndex();
		
	public:
		GpuIndexIVFPQ *index;

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