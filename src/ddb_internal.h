
#ifndef __DDB_INTERNAL_H__
#define __DDB_INTERNAL_H__

#include <discodb.h>

#define DISCODB_MAGIC 0x4D85BE61D14DE59

#define F_HASH 1
#define F_MULTISET 2

#define HASFLAG(db, f) (db->flags & f)
#define SETFLAG(db, f) (db->flags |= f)

typedef uint32_t keyid_t;
typedef uint32_t valueid_t;

struct ddb_header{
        uint64_t magic;
        uint64_t size;

        uint32_t num_keys;
        uint64_t num_values;
        uint32_t num_uniq_values;
        uint32_t flags;

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
        uint64_t num_values;
        uint32_t num_uniq_values;
        uint32_t flags;

        const uint64_t *toc;
        const uint64_t *values_toc;
        const char *data;
        const char *hash;
        const char *values;

        int errno;
        void *mmap;
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
        struct ddb_entry ent;
        uint32_t num_items;
        union{
                struct ddb_value_cursor value;
                struct ddb_values_cursor values;
                struct ddb_key_cursor keys;
                struct ddb_cnf_cursor cnf;
        } cursor;
        const struct ddb_entry *(*next)(struct ddb_cursor*);
};


void ddb_fetch_item(uint32_t i, const uint64_t *toc, const char *data,
        struct ddb_entry *key, struct ddb_value_cursor *val);

void ddb_resolve_valueid(const struct ddb *db, 
        valueid_t id, struct ddb_entry *e);
void ddb_value_cursor_step(struct ddb_value_cursor *c);

const struct ddb_entry *ddb_cnf_cursor_next(struct ddb_cursor *c);

valueid_t ddb_val_next(struct ddb_cnf_term *t);
valueid_t ddb_not_next(struct ddb_cnf_term *t);

/* util.c */

uint32_t read_bits(const char *src, uint64_t offs, uint32_t bits);
void write_bits(char *dst, uint64_t offs, uint32_t val);
uint32_t bits_needed(uint32_t max);

/* ddb_valuemap.c */

void *ddb_valuemap_init(void);
void ddb_valuemap_free(void *valmap);
valueid_t *ddb_valuemap_lookup(void *valmap, const struct ddb_entry *value,
        int (*eq)(const struct ddb_entry*, valueid_t));


#endif /* __DDB_INTERNAL_H__ */
