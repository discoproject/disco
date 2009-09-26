
#ifndef __DISCODB_H__
#define __DISCODB_H__

#include <stdint.h>

#define DDB_MIN_BLOB_SIZE (1024 * 1024)
#define DDB_MAX_NUM_VALUES UINT_MAX
#define DDB_MAX_NUM_KEYS UINT_MAX
#define DDB_HASH_MIN_KEYS 25

struct ddb_cons;
struct ddb;

struct ddb_entry{
        const char *data;
        uint32_t length;
};

struct ddb_cons *ddb_new();
int ddb_add(struct ddb_cons *db, const struct ddb_entry *key,
        const struct ddb_entry *values, uint32_t num_values);
const char *ddb_finalize(struct ddb_cons *c, uint64_t *length);

struct ddb *ddb_loads(const char *data, uint64_t length);
const char *ddb_dumps(const struct ddb *db, uint64_t *length);

struct ddb_cursor *ddb_keys(const struct ddb *db);
struct ddb_cursor *ddb_values(const struct ddb *db);
struct ddb_cursor *ddb_getitem(const struct ddb *db, const struct ddb_entry *key);

const struct ddb_entry *ddb_next(struct ddb_cursor *cur);
uint32_t ddb_resultset_size(const struct ddb_cursor *cur);



#endif /* __DISCODB_H__ */
