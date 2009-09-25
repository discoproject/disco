
#ifndef __DISCODB_H__
#define __DISCODB_H__

#include <stdint.h>

#define DDB_MAX_NUM_VALUES UINT_MAX
#define DDB_MAX_NUM_KEYS UINT_MAX
#define DDB_HASH_MIN_KEYS 25

struct ddb_cons;

struct ddb_entry{
        const char *data;
        uint32_t length;
};

struct ddb_cons *ddb_new();
int ddb_add(struct ddb_cons *db, const struct ddb_entry *key,
        const struct ddb_entry *values, uint32_t num_values);
const char *ddb_finalize(struct ddb_cons *c, uint64_t *length);


#endif /* __DISCODB_H__ */
