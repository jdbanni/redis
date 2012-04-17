#include <stdio.h>

#include "zmalloc.h"
#include "redis.h"
#include "leveldb.h"

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

/** jdbanni: open a Level Db database */
int openLevelDB(redisDb *db) {
	char *err = NULL;

	createLevelDBFilename(db);
	
	/** create the relevant data structures for this database */
	db->env = leveldb_create_default_env();
	db->cache = leveldb_cache_create_lru(1024 * 1024 * 128);
	db->options = leveldb_options_create();
	db->roptions = leveldb_readoptions_create();
	db->woptions = leveldb_writeoptions_create();
//	db->cmp = leveldb_comparator_create(NULL, comparatorDestroy, strncasecmpCompare, strncasecmpName);
  
	/* database options */
//	leveldb_options_set_comparator(db->options, db->cmp);
	leveldb_options_set_error_if_exists(db->options, 0);
	leveldb_options_set_cache(db->options, db->cache);
	leveldb_options_set_env(db->options, db->env);
	leveldb_options_set_info_log(db->options, NULL);
	leveldb_options_set_write_buffer_size(db->options, 1024 *1024 * 128);
	leveldb_options_set_paranoid_checks(db->options, 0);
	leveldb_options_set_max_open_files(db->options, 1000); 
	leveldb_options_set_block_size(db->options, 1024 * 64);
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
	leveldb_close(db->leveldb);
//	leveldb_comparator_destroy(db->cmp);
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
		zfree(tempvalue);
	}
	
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
int iterateKeysForwardsLevelDB(redisDb *db, robj *key, robj *keys[], long *count) {
	char *err = NULL;
	char *tempkey=NULL;
	leveldb_iterator_t *iter;
	robj *iterkey;
	size_t key_len;
	long i;

	/** create the iterator */
	iter=leveldb_create_iterator(db->leveldb, db->roptions);
    checkCondition(!leveldb_iter_valid(iter));
    
	/** and position it */
    leveldb_iter_seek(iter, key->ptr, stringObjectLen(key));
		
	/** now get all the keys */
	for (i=0;i < *count;i++) {
        if (!leveldb_iter_valid(iter)) {
            /** iterator no longer valid, so return what we currently have */
            break;
        }
		tempkey=(char *)leveldb_iter_key(iter, &key_len);
		
		/** create the key object */
		iterkey=createStringObject(tempkey, key_len);
		keys[i]=iterkey;
		
		/** move the iterator to the next key */
	    leveldb_iter_next(iter);
	}
	
	/** the final count */
	*count=i;
	
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

/** jdbanni: for writing to LevelDB. We don't use the object encoding, instead relying on snappy if set */
void setCommandLdb(redisClient *c) {
	if (setLevelDB(c->db, c->argv[1], c->argv[2]) != REDIS_OK) {
        addReplyError(c,"unable to set a LevelDB value using LDBSET");		
	}
	server.dirty++;
	addReply(c,shared.ok);
}

/** jdbanni: for reading from LevelDB */
void getCommandLdb(redisClient *c) {
	robj *value;
	
	if (getLevelDB(c->db, c->argv[1], &value) != REDIS_OK) {
        addReplyError(c,"unable to get a LevelDB value using LDBGET");		
	}
	
	if (value != NULL) {
		/** for return to the networking layer */
		addReplyBulk(c, value);

		/** free the object */
		decrRefCount(value);
	} else {
		addReply(c, shared.nullbulk);
	}
}

/** jdbanni: delete a key from Level DB */
void deleteCommandLdb(redisClient *c) {
	if (deleteLevelDB(c->db, c->argv[1]) != REDIS_OK) {
        addReplyError(c,"unable to delete a LevelDB value using LDBDEL");		
	}
	server.dirty++;
	addReply(c,shared.ok);
}

/** jdbanni: iterate forwards from a supplied key to a specified count of keys */
void iterForwardsCommandLdb(redisClient *c) {
	robj **keys;
	long count;
	long i;

	/** get the count of keys to return */
	getLongFromObjectOrReply(c, c->argv[2], &count, NULL);	

	/** bounds check */
	if (count < 1 || count > 0xFFFF) {
        addReplyError(c,"unable to iterate over > 65535 keys in LDBITERFORWARDS");	
		return;
	}
	
    /** create the array of pointers to keys */
	keys=zmalloc(sizeof(robj *) * (count));

	/** now iterate */
	if (iterateKeysForwardsLevelDB(c->db, c->argv[1], keys, &count) != REDIS_OK) {
        addReplyError(c,"unable to iterate LevelDB value using LDBITERFORWARDS");				
		return;
	}
	
	/** must have at least one item to return */
	if (count == 0) {
		addReply(c, shared.nullbulk);		
	} else {	
		/** the total count found */
		addReplyMultiBulkLen(c,count);
	    	
		/** add the replies */
		for (i=0;i < count;i++) {
			if (keys != NULL) {
				addReplyBulk(c, keys[i]);
				decrRefCount(keys[i]);		
			}
		}		
	}
	
	/** free the array as well */
	zfree(keys);
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
