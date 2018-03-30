#ifndef CORE_DB_H
#define CORE_DB_H

#include "share_lock.h"
#include "utils.h"
#include <atomic>
#include <sstream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "lmdb/lmdb.h"

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
			
		int lmdbSet(const char *key, char *val);
		int lmdbSet(const char *key, void *val, int len);
		int lmdbSet(const char *key1, void *val1, int len1, const char *key2, void *val2, int len2); 

		int lmdbDel(const char *key);
		
		int lmdbGet(const char *key, std::string *val, int *val_len);
		int lmdbGet(const char *key, void **val, int *val_len);

};

#endif
