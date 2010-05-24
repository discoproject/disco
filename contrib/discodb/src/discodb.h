
#ifndef __DISCODB_H__
#define __DISCODB_H__

#include <stdint.h>
#include <unistd.h>

#define DDB_MAX_NUM_VALUES 4294967295
#define DDB_MAX_NUM_KEYS 4294967295
#define DDB_HASH_MIN_KEYS 25

#define DDB_ERR_OUT_OF_MEMORY 1
#define DDB_ERR_QUERY_NOT_SUPPORTED 2
#define DDB_ERR_BUFFER_TOO_SMALL 3
#define DDB_ERR_BUFFER_NOT_DISCODB 4
#define DDB_ERR_INVALID_BUFFER_SIZE 5
#define DDB_ERR_STAT_FAILED 6
#define DDB_ERR_MMAP_FAILED 7
#define DDB_ERR_WRITEFAILED 8

struct ddb_cons;
struct ddb;
struct ddb_cursor;

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

struct ddb_cons *ddb_cons_new(void);

int ddb_add(struct ddb_cons *db, const struct ddb_entry *key,
        const struct ddb_entry *values, uint32_t num_values);
char *ddb_finalize(struct ddb_cons *c, uint64_t *length);

struct ddb *ddb_new(void);
int ddb_load(struct ddb *db, int fd);
int ddb_loado(struct ddb *db, int fd, off_t);
int ddb_loads(struct ddb *db, const char *data, uint64_t length);
int ddb_dump(struct ddb *db, int fd);
char *ddb_dumps(struct ddb *db, uint64_t *length);

struct ddb_cursor *ddb_keys(struct ddb *db);
struct ddb_cursor *ddb_values(struct ddb *db);
struct ddb_cursor *ddb_getitem(struct ddb *db,
        const struct ddb_entry *key);
struct ddb_cursor *ddb_query(struct ddb *db,
        const struct ddb_query_clause *clauses, uint32_t num_clauses);

void ddb_free(struct ddb *db);
int ddb_error(struct ddb *db, const char **errstr);
void ddb_free_cursor(struct ddb_cursor *cur);
int ddb_notfound(const struct ddb_cursor *c);
const struct ddb_entry *ddb_next(struct ddb_cursor *cur);
uint64_t ddb_resultset_size(const struct ddb_cursor *cur);



#endif /* __DISCODB_H__ */
