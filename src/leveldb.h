#ifndef __LEVELDB_H
#define __LEVELDB_H

#include "redis.h"

/** jdbanni: open a Level Db database */
extern int openLevelDB(redisDb *db);

/** jdbanni: close a Level DB database */
extern int closeLevelDB(redisDb *db);

/** set a value to an already opened database */
extern int setLevelDB(redisDb *db, robj *key, robj *value);

/** return a value from an already opened database, client responsible for freeing this item */
extern int getLevelDB(redisDb *db, robj *key, robj **value);

/** remove a key from Level DB */
extern int deleteLevelDB(redisDb *db, robj *key);

/** iterate forwards from a key, returning a number of keys */
extern int iterateKeysForwardsLevelDB(redisDb *db, robj *key, robj *value[], long *count, int mode);

/** iterate forwards from the end, returning a number of keys */
extern int iterateKeysForwardsFirstLevelDB(redisDb *db, robj **value, long *count);

/** iterate backwards from a key, returning a number of keys */
extern int iterateKeysBackwardsLevelDB(redisDb *db, robj *key, robj **value, long *count);

/** iterate backwards from the end, returning a number of keys */
extern int iterateKeysBackwardsLastLevelDB(redisDb *db, robj **value, long *count);

/** repair the database */
extern int repairLevelDB(redisDb *db);

/** compact the database - warning, could take a long time to execute with large fragmentation */
extern int compactLevelDB(redisDb *db);

#endif
