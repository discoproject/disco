
#ifndef __DISCODB_H__
#define __DISCODB_H__

#include <stdint.h>

#define DDB_MAX_NUM_VALUES 4294967295
#define DDB_MAX_NUM_KEYS 4294967295
#define DDB_HASH_MIN_KEYS 25

#define DDB_ERR_OUT_OF_MEMORY 1
#define DDB_ERR_QUERY_NOT_SUPPORTED 2

struct ddb_cons;
struct ddb;

struct ddb_entry{
        const char *data;
        uint32_t length;
};

struct ddb_query_term{
        struct ddb_entry key;
        int not;
};

struct ddb_query_clause{
        struct ddb_query_term *terms;
        uint32_t num_terms;
};

struct ddb_cons *ddb_new();
int ddb_add(struct ddb_cons *db, const struct ddb_entry *key,
        const struct ddb_entry *values, uint32_t num_values);
const char *ddb_finalize(struct ddb_cons *c, uint64_t *length);

struct ddb *ddb_loads(const char *data, uint64_t length);
const char *ddb_dumps(struct ddb *db, uint64_t *length);

struct ddb_cursor *ddb_keys(struct ddb *db);
struct ddb_cursor *ddb_values(struct ddb *db);
struct ddb_cursor *ddb_getitem(struct ddb *db,
        const struct ddb_entry *key);
struct ddb_cursor *ddb_query(struct ddb *db,
        const struct ddb_query_clause *clauses, uint32_t num_clauses);

int ddb_error(struct ddb *db, const char **errstr);
void ddb_free_cursor(struct ddb_cursor *cur);
const struct ddb_entry *ddb_next(struct ddb_cursor *cur);
uint32_t ddb_resultset_size(const struct ddb_cursor *cur);



#endif /* __DISCODB_H__ */
