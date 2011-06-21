
#ifndef __DDB_INTERNAL_H__
#define __DDB_INTERNAL_H__

#include <discodb.h>
#include <ddb_types.h>
#include <ddb_huffman.h>
#include <ddb_delta.h>

#define DISCODB_MAGIC 0x4D85BE61D14DE5B

#define COMPRESS_MIN_TOTAL_SIZE (5 * 1024 * 1024)
#define COMPRESS_MIN_AVG_VALUE_SIZE 6

#define F_HASH 1
#define F_MULTISET 2
#define F_COMPRESSED 4

#define HASFLAG(db, f) (db->flags & f)
#define SETFLAG(db, f) (db->flags |= f)

struct ddb_header{
    uint64_t magic;
    uint64_t size;

    uint32_t num_keys;
    uint32_t num_uniq_values;
    uint64_t num_values;
    uint64_t flags;

    uint64_t key2values_offs;
    uint64_t id2value_offs;
    uint64_t hash_offs;
    uint64_t codebook_offs;
} __attribute__((packed));

struct ddb{
    uint64_t size;
    uint64_t mmap_size;

    int errno;
    uint32_t num_keys;
    uint32_t num_uniq_values;
    uint32_t flags;
    uint64_t num_values;

    const uint64_t *key2values;
    const uint64_t *id2value;
    const uint64_t *hash;

    const struct ddb_codebook *codebook;

    const char *buf;
    void *mmap;
};

struct ddb_key_cursor{
    uint32_t i;
};

struct ddb_unique_values_cursor{
    uint32_t i;
};

struct ddb_values_cursor{
    uint32_t i;
    struct ddb_delta_cursor cur;
};

#ifdef DEBUG
#define WINDOW_SIZE 64
#else
#define WINDOW_SIZE 1024 /* bits */
#endif

#define WINDOW_SIZE_BYTES (WINDOW_SIZE >> 3)

struct ddb_cnf_term{
    struct ddb_cursor *cursor;
    valueid_t (*next)(struct ddb_cnf_term*);
    valueid_t cur_id;
    int empty;
};

struct ddb_cnf_clause{
    char unionn[WINDOW_SIZE_BYTES];
    struct ddb_cnf_term *terms;
    uint32_t num_terms;
};

struct ddb_cnf_cursor{
    struct ddb_cnf_clause *clauses;
    struct ddb_cnf_term *terms;
    uint32_t num_clauses;
    uint32_t num_terms;

    char *isect;
    uint32_t isect_offset;
    valueid_t base_id;
};

struct ddb_cursor{
    const struct ddb *db;

    char *decode_buf;
    uint64_t decode_buf_len;
    struct ddb_entry entry;

    union{
        struct ddb_delta_cursor value;
        struct ddb_values_cursor values;
        struct ddb_unique_values_cursor uvalues;
        struct ddb_key_cursor keys;
        struct ddb_cnf_cursor cnf;
    } cursor;
    const struct ddb_entry *(*next)(struct ddb_cursor*);

    uint32_t num_items;
    int errno;
    int no_valuestr;
};

int ddb_get_valuestr(struct ddb_cursor *c, valueid_t id);
const struct ddb_entry *ddb_cnf_cursor_next(struct ddb_cursor *c);

valueid_t ddb_val_next(struct ddb_cnf_term *t);
valueid_t ddb_not_next(struct ddb_cnf_term *t);


#endif /* __DDB_INTERNAL_H__ */
