#include <stdio.h>

#include "zmalloc.h"
#include "redis.h"
#include "leveldb.h"

/** constants */
robj *iterateKeys = NULL;
robj *iterateKeysAndValues = NULL;

robj *ldbCacheOn = NULL;
robj *ldbCacheOff = NULL;

#define ITERMODE_KEYSONLY 0
#define ITERMODE_KEYSANDVALUES 1


/*****************************************************************************/

/** error checking */
#define checkNoError(err)                                                   \
	if ((err) != NULL) {                                                    \
		fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, (err));          \
		abort();                                                            \
	}

#define checkCondition(cond)                                            \
  if (!(cond)) {                                                        \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #cond);          \
    abort();                                                            \
  }

/** leveldb memory defines (no clash with redis) **/

#undef malloc
#undef free

#define ldb_malloc(s) malloc(s)
#define ldb_free(s) free(s)

/*****************************************************************************/

/** helpers **/

void createLevelDBFilename(redisDb *db) {
	asprintf(&db->leveldb_filename, "%s/%d", server.leveldbdir, db->id);
}

/** comparator functions - database must be created and used with the same comparator */
static void comparatorDestroy(void *arg) {	
}

/** memory comparator */
/*
static int memcmpCompare(void *arg, const char *a, size_t alen,
                      const char *b, size_t blen) {
	int n = (alen < blen) ? alen : blen;
	int r = memcmp(a, b, n);
	if (r == 0) {
		if (alen < blen) r = -1;
		else if (alen > blen) r = +1;
	}
	
	return r;
}
static const char* memcmpName(void *arg) {
	return "memcmp";
}
*/

/** case insensitive case comparison */
static int strncasecmpCompare(void *arg, const char *a, size_t alen,
                      const char *b, size_t blen) {
	int n = (alen < blen) ? alen : blen;
	int r = strncasecmp(a, b, n);
	if (r == 0) {
		if (alen < blen) r = -1;
		else if (alen > blen) r = +1;
	}
	
	return r;
}
static const char* strncasecmpName(void *arg) {
	return "strncasecmp";
}

/*****************************************************************************/

/** main level db private functions **/

/** jdbanni: open a Level Db database */
int openLevelDB(redisDb *db) {
	char *err = NULL;

	createLevelDBFilename(db);
	
	/** create some string constants **/
	iterateKeys = createObject(REDIS_STRING,sdsnew("keys"));
	iterateKeysAndValues = createObject(REDIS_STRING,sdsnew("keysandvalues"));
	ldbCacheOn = createObject(REDIS_STRING,sdsnew("on"));
	ldbCacheOff = createObject(REDIS_STRING,sdsnew("off"));
	
	/** create the relevant data structures for this database */
	db->env = leveldb_create_default_env();
	db->cache = leveldb_cache_create_lru(1024 * 1024 * 128);
	db->options = leveldb_options_create();
	db->roptions = leveldb_readoptions_create();
	db->woptions = leveldb_writeoptions_create();
	db->cmp = leveldb_comparator_create(NULL, comparatorDestroy, strncasecmpCompare, strncasecmpName);
  
	/* database options */
	leveldb_options_set_comparator(db->options, db->cmp);
	leveldb_options_set_error_if_exists(db->options, 0);
	leveldb_options_set_cache(db->options, db->cache);
	leveldb_options_set_env(db->options, db->env);
	leveldb_options_set_info_log(db->options, NULL);
	leveldb_options_set_write_buffer_size(db->options, 1024 *1024 * 128);
	leveldb_options_set_paranoid_checks(db->options, 0);
	leveldb_options_set_max_open_files(db->options, 1000); 
	leveldb_options_set_block_size(db->options, 1024 * 32);
	leveldb_options_set_compression(db->options, leveldb_snappy_compression);
	leveldb_options_set_create_if_missing(db->options, 1);

	/* read options */
	leveldb_readoptions_set_verify_checksums(db->roptions, 0);
	leveldb_readoptions_set_fill_cache(db->roptions, 1);

	/* write options */
	leveldb_writeoptions_set_sync(db->woptions, 0);

	/** now create the database */
	db->leveldb = leveldb_open(db->options, db->leveldb_filename, &err);
	checkNoError(err);

    redisLog(REDIS_NOTICE,"LevelDB database opened: %s",db->leveldb_filename);
	return REDIS_OK;
}

/** jdbanni: close a Level DB database */
int closeLevelDB(redisDb *db) {
	decrRefCount(iterateKeys);		
	decrRefCount(iterateKeysAndValues);		
	
	leveldb_close(db->leveldb);
	leveldb_comparator_destroy(db->cmp);
	leveldb_options_destroy(db->options);
	leveldb_readoptions_destroy(db->roptions);
	leveldb_writeoptions_destroy(db->woptions);
	leveldb_cache_destroy(db->cache);
	leveldb_env_destroy(db->env);
	
    redisLog(REDIS_NOTICE,"LevelDB database closed: %s",db->leveldb_filename);
	zfree(db->leveldb_filename); /** allocated by asprintf */
	
	return REDIS_OK;
}

/** set a value to an already opened database */
int setLevelDB(redisDb *db, robj *key, robj *value) {
	char *err = NULL;
	
	leveldb_put(db->leveldb, db->woptions, key->ptr, stringObjectLen(key), value->ptr, stringObjectLen(value), &err);
	checkNoError(err);	
	
	return REDIS_OK;	
}

/** return a value from an already opened database, client responsible for freeing this item */
int getLevelDB(redisDb *db, robj *key, robj **value) {
	char *err = NULL;
	size_t value_len=0;
	char *tempvalue;
	
	tempvalue = leveldb_get(db->leveldb, db->roptions, key->ptr, stringObjectLen(key), &value_len, &err);	
	checkNoError(err);
	
	if (tempvalue == NULL) {
		*value=NULL;
	} else {		
		/** create a redis object */
		*value=createStringObject(tempvalue, value_len);
	
		/** free the value from level db */
		ldb_free(tempvalue);
	}
	
	return REDIS_OK;	
}

/** append a value to an already opened database */
int appendLevelDB(redisDb *db, robj *key, robj *value) {
	char *err = NULL;
	size_t existingvalue_len=0, newvalue_len=0;
	char *existingvalue, *newvalue;
	
	/** first see if it exists */
	existingvalue = leveldb_get(db->leveldb, db->roptions, key->ptr, stringObjectLen(key), &existingvalue_len, &err);	
	checkNoError(err);
	
	if (existingvalue == NULL || existingvalue_len == 0) {
		/** a new value (or an existing 0 length one) **/
		leveldb_put(db->leveldb, db->woptions, key->ptr, stringObjectLen(key), value->ptr, stringObjectLen(value), &err);
		checkNoError(err);	
	} else {		
		/** create the array of pointers to keys/values */
		newvalue_len=existingvalue_len + stringObjectLen(value);
		newvalue=zmalloc(newvalue_len);
		
		/** copy the existing value **/
		memcpy(newvalue, existingvalue, existingvalue_len);
		/** copy the new value **/
		memcpy(newvalue + existingvalue_len, value->ptr, stringObjectLen(value));
		
		/** now write the new value **/
		leveldb_put(db->leveldb, db->woptions, key->ptr, stringObjectLen(key), newvalue, newvalue_len, &err);
		checkNoError(err);		
		
		/** free the new value **/
		zfree(newvalue);
	}
	
	/** free the value returned from level db */
	ldb_free(existingvalue);
	
	return REDIS_OK;	
}

/** remove a key from Level DB */
int deleteLevelDB(redisDb *db, robj *key) {
	char *err = NULL;
	
	leveldb_delete(db->leveldb, db->woptions, key->ptr, stringObjectLen(key), &err);
	checkNoError(err);	
	
	return REDIS_OK;		
}

/** iterate forwards from a key, returning a number of keys */
int iterateKeysForwardsLevelDB(redisDb *db, robj *key, robj *response[], long *count) {
	char *err = NULL;
	char *tempkey, *tempvalue;
	leveldb_iterator_t *iter;
	robj *iterkey, *itervalue;
	size_t key_len, value_len;
	long i,index;

	/** create the iterator */
	iter=leveldb_create_iterator(db->leveldb, db->roptions);
    checkCondition(!leveldb_iter_valid(iter));
    
	/** and position it */
    leveldb_iter_seek(iter, key->ptr, stringObjectLen(key));

	/** now go through all the keys */
	for (i=0, index=0;i < *count;i++) {
        if (!leveldb_iter_valid(iter)) {
            /** iterator no longer valid, so return what we currently have */
            break;
        }

		/** fetch the key **/
		tempkey=(char *)leveldb_iter_key(iter, &key_len);
		iterkey=createStringObject(tempkey, key_len);
		response[index++]=iterkey;

		/** fetch the value **/
		tempvalue=(char *)leveldb_iter_value(iter, &value_len);
		itervalue=createStringObject(tempvalue, value_len);
		response[index++]=itervalue;
		
		/** move the iterator to the next key */
	    leveldb_iter_next(iter);
	}
	
	/** the final count */
	*count=index;
	
	/** remove the iterator */
	leveldb_iter_destroy(iter);
    
	return REDIS_OK;			
}

/** iterate forwards from the end, returning a number of keys */
int iterateKeysForwardsFirstLevelDB(redisDb *db, robj **value, long *count) {
	
	return REDIS_OK;			
}

/** iterate backwards from a key, returning a number of keys */
int iterateKeysBackwardsLevelDB(redisDb *db, robj *key, robj **value, long *count) {
	
	return REDIS_OK;			
}

/** iterate backwards from the end, returning a number of keys */
int iterateKeysBackwardsLastLevelDB(redisDb *db, robj **value, long *count) {
	
	return REDIS_OK;			
}

/** repair the database */
int repairLevelDB(redisDb *db) {
	redisLog(REDIS_WARNING,"Repair not currently supported");
    
	return REDIS_OK;			
}

/** compact the database - warning, could take a long time to execute with large fragmentation */
int compactLevelDB(redisDb *db) {
	redisLog(REDIS_WARNING,"Compact not currently supported");
    
	return REDIS_OK;			
}

/*****************************************************************************/

/** public externed commands **/

/** jdbanni: for writing to LevelDB */
void setCommandLdb(redisClient *c) {
	if (setLevelDB(c->db, c->argv[1], c->argv[2]) != REDIS_OK) {
        addReplyError(c,"unable to set a LevelDB value using LDBSET");		
	}
	
	/** set the key to Redis as well if required */
	if (c->ldbUseCache == 1) {
		setKey(c->db, c->argv[1], c->argv[2]);	
		server.dirty++;
		if (c->ldbExpiryTime) setExpire(c->db,c->argv[1],time(NULL)+c->ldbExpiryTime);	    
	}
	
	addReply(c,shared.ok);
}

/** jdbanni: for appending to an existing value, creating if not exists to LevelDB */
void appendCommandLdb(redisClient *c) {
	robj *value;

	if (appendLevelDB(c->db, c->argv[1], c->argv[2]) != REDIS_OK) {
        addReplyError(c,"unable to append a LevelDB value using LDBSET");		
	}
	
	/**
		Currently not working with the cache
	**/
	
	addReply(c,shared.ok);
}

/** jdbanni: for reading from LevelDB */
void getCommandLdb(redisClient *c) {
	robj *value=NULL;
	
	if (c->ldbUseCache == 1) {
		/** try the redis cache first */
		value=lookupKeyRead(c->db, c->argv[1]);
		
		if (value != NULL) {
			addReplyBulk(c, value);			
		} else {
			if (getLevelDB(c->db, c->argv[1], &value) != REDIS_OK) {
		        addReplyError(c,"unable to get a LevelDB value using LDBGET");		
			}			
			
			if (value != NULL) {
				/** set the key to Redis as well if required */
				setKey(c->db, c->argv[1], value);	
				server.dirty++;
				if (c->ldbExpiryTime) setExpire(c->db,c->argv[1],time(NULL)+c->ldbExpiryTime);
			
				addReplyBulk(c, value);
				decrRefCount(value);
			} else {
				addReply(c, shared.nullbulk);
			}
		}
	} else {
		/** not using the cache */
		if (getLevelDB(c->db, c->argv[1], &value) != REDIS_OK) {
	        addReplyError(c,"unable to get a LevelDB value using LDBGET");		
		}
	
		if (value != NULL) {
			addReplyBulk(c, value);			
			decrRefCount(value);
		} else {
			addReply(c, shared.nullbulk);
		}
	}
}

/** jdbanni: delete a key from Level DB */
void deleteCommandLdb(redisClient *c) {	
	if (deleteLevelDB(c->db, c->argv[1]) != REDIS_OK) {
        addReplyError(c,"unable to delete a LevelDB value using LDBDEL");		
	}
	
	if (c->ldbUseCache == 1) {
		dbDelete(c->db, c->argv[1]);
		server.dirty++;
	}
	
	addReply(c,shared.ok);
}

/** jdbanni: iterate forwards from a supplied key to a specified count of keys */
void iterForwardsCommandLdb(redisClient *c) {
	robj **response;
	long count, itemcount;
	long i;
	int mode=ITERMODE_KEYSONLY;

	/** get the count of keys to return */
	getLongFromObjectOrReply(c, c->argv[2], &count, NULL);	

	/** bounds check (arbitrary limit) */
	if (count < 1 || count > 0xFFFF) {
        addReplyError(c,"unable to iterate over > 65535 keys in LDBITERFORWARDS");	
		return;
	}

	/** check the mode required **/
	if (compareStringObjects(c->argv[3], iterateKeys) == 0) {
		mode=ITERMODE_KEYSONLY;
		itemcount=count;
	} else if (compareStringObjects(c->argv[3], iterateKeysAndValues) == 0) {
		mode=ITERMODE_KEYSANDVALUES;
		itemcount=count*2; /** double the count required **/
	} else {
        addReplyError(c,"mode must be one of: keys, keysandvalues");	
		return;
	}
	
    /** create the array of pointers to keys/values */
	response=zmalloc(sizeof(robj *) * (itemcount));

	/** now iterate, count will hold the number of response objects */
	if (iterateKeysForwardsLevelDB(c->db, c->argv[1], response, &count) != REDIS_OK) {
        addReplyError(c,"unable to iterate LevelDB value using LDBITERFORWARDS");				
		return;
	}
	
	/** must have at least one item to return */
	if (count == 0) {
		addReply(c, shared.nullbulk);		
	} else {	
		/** the total count found */
		if (mode == ITERMODE_KEYSONLY)
			addReplyMultiBulkLen(c,count/2);
		else
			addReplyMultiBulkLen(c,count);
		
		/** add the replies (which could be keys, values or keys/values) */
		for (i=0;i < count;i+=2) {
			/** set the key to Redis as well if required (keys are even, values are odd)*/
			if (c->ldbUseCache == 1) {
				setKey(c->db, response[i], response[i+1]);	
				server.dirty++;
				if (c->ldbExpiryTime) setExpire(c->db,response[i],time(NULL)+c->ldbExpiryTime);	    
			}
			
			if (mode == ITERMODE_KEYSONLY) {
				addReplyBulk(c, response[i]); /** keys */
			}
			else if (mode == ITERMODE_KEYSANDVALUES) {
				addReplyBulk(c, response[i]); /** keys */
				addReplyBulk(c, response[i+1]); /** values */
			}
			decrRefCount(response[i]); /** frees the key, it is duplicated in setKey() if in the cache **/
			decrRefCount(response[i+1]); /** frees the value, if has been incremented if we are caching **/
		}		
	}
	
	/** free the array as well */
	zfree(response);
}

void iterBackwardsCommandLdb(redisClient *c) {
	
}

void iterForwardsFirstCommandLdb(redisClient *c) {
	
}

void iterBackwardsFirstCommandLdb(redisClient *c) {
	
}

/** compact the database */
void compactCommandLdb(redisClient *c) {
	if (compactLevelDB(c->db) != REDIS_OK) {
		addReplyError(c,"unable to compact the database");
	}
	
	addReply(c,shared.ok);
}

/** repair the database */
void repairCommandLdb(redisClient *c) {
	if (repairLevelDB(c->db) != REDIS_OK) {		
		addReplyError(c,"unable to repair the database");
	}
	
	addReply(c,shared.ok);
}

/** enabled or disable caching for this client */
void cacheLdb(redisClient *c) {
	long expiryTime=-1;
	
	/** get the expiry time for the cache, 0 for do not use expiry */
	if (getLongFromObjectOrReply(c, c->argv[1], &expiryTime, NULL) == REDIS_ERR)
		return;

    if (expiryTime < 0) {
        addReplyError(c,"invalid expire time in LDBCACHE");
        return;
    }

	/** check the mode required and set it for this client **/
	if (compareStringObjects(c->argv[2], ldbCacheOn) == 0) {
		c->ldbExpiryTime=expiryTime;
		c->ldbUseCache=1;
	} else if (compareStringObjects(c->argv[2], ldbCacheOff) == 0) {
		c->ldbExpiryTime=expiryTime;
		c->ldbUseCache=0;
	} else {
        addReplyError(c,"cache mode must be ON or OFF");	
		return;
	}
	
	addReply(c,shared.ok);	
}
