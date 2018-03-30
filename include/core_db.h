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
			
		int lmdbSet(char *key, char *val);
		int lmdbSet(char *key, void *val, int len);
		int lmdbSet(char *key1, void *val1, int len1, char *key2, void *val2, int len2); 

		int lmdbDel(char *key);
		
		int lmdbGet(char *key, std::string *val, int *val_len);
		int lmdbGet(char *key, void **val, int *val_len);

};

#endif
