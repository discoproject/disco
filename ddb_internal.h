
#ifndef __DDB_INTERNAL_H__
#define __DDB_INTERNAL_H__

#include <discodb.h>

#define DISCODB_MAGIC 0x4D85BE61D14DE59

typedef uint32_t keyid_t;
typedef uint32_t valueid_t;

struct ddb_header{
        uint64_t magic;
        uint64_t size;

        uint32_t num_keys;
        uint32_t num_values;
        uint32_t hashash;

        uint64_t toc_offs;
        uint64_t values_toc_offs;
        uint64_t data_offs;
        uint64_t values_offs;
        uint64_t hash_offs;
} __attribute__((packed));

struct ddb{
        const char *buf;
        uint64_t size;

        uint32_t num_keys;
        uint32_t num_values;
        uint32_t hashash;

        const uint64_t *toc;
        const uint64_t *values_toc;
        const char *data;
        const char *hash;
        const char *values;
};

struct ddb_value_cursor{
        valueid_t cur_id;
        const char *deltas;
        uint32_t bits;
        uint32_t num_left;
        uint64_t offset;
};

struct ddb_key_cursor{
        uint32_t i; 
};

struct ddb_values_cursor{
        uint32_t i;
        struct ddb_value_cursor cur;
};

struct ddb_cursor{
        const struct ddb *db;
        struct ddb_entry ent;
        uint32_t num_items;
        union{
                struct ddb_value_cursor value;
                struct ddb_values_cursor values;
                struct ddb_key_cursor keys;
        } cursor;
        const struct ddb_entry *(*next)(struct ddb_cursor*);
};

void ddb_fetch_item(uint32_t i, const uint64_t *toc, const char *data,
        struct ddb_entry *key, struct ddb_value_cursor *val);

void ddb_fetch_nextval(struct ddb_entry *e, 
        const struct ddb *db, struct ddb_value_cursor *c);

/* util.c */

uint32_t read_bits(const char *src, uint64_t offs, uint32_t bits);
void write_bits(char *dst, uint64_t offs, uint32_t val);
uint32_t bits_needed(uint32_t max);

#endif /* __DDB_INTERNAL_H__ */
