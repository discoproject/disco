
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>

#include <cmph.h>

#include <discodb.h>
#include <ddb_internal.h>

#define PAGE_MASK (~(getpagesize() - 1))
#define PAGE_ALIGN(addr) ((intptr_t)(addr) & PAGE_MASK)

static const char *ERR_STR[] = {
        "Ok",
        "Out of memory",
        "Queries are not supported on multisets",
        "Buffer too small",
        "Buffer not discodb",
        "Invalid buffer size",
        "Couldn't get the file size",
        "Memory map failed",
        "Invalid file descriptor",
        "Write failed"
};

void ddb_value_cursor_step(struct ddb_value_cursor *c)
{
        uint32_t v = read_bits(c->deltas, c->offset, c->bits);
        c->cur_id += v;
        c->num_left--;
        c->offset += c->bits;
}

void ddb_resolve_valueid(const struct ddb *db, valueid_t id, struct ddb_entry *e)
{
        e->length = db->values_toc[id] - db->values_toc[id - 1];
        e->data = &db->values[db->values_toc[id - 1]];
}

struct ddb *ddb_new()
{
        struct ddb *db = NULL;
        if (!(db = calloc(1, sizeof(struct ddb))))
                return NULL;
        return db;
}

int ddb_load(struct ddb *db, int fd)
{
        return ddb_loado(db, fd, 0);
}

int ddb_loado(struct ddb *db, int fd, off_t offset)
{
        struct stat nfo;
        off_t mmap_offset = PAGE_ALIGN(offset);

        if (fstat(fd, &nfo)){
                db->errno = DDB_ERR_STAT_FAILED;
                return -1;
        }
        db->mmap = mmap(0, nfo.st_size - mmap_offset, PROT_READ, MAP_SHARED, fd, mmap_offset);

        if (db->mmap == MAP_FAILED){
                db->errno = DDB_ERR_MMAP_FAILED;
                return -1;
        }
        return ddb_loads(db, db->mmap + (offset - mmap_offset), nfo.st_size - offset);
}

int ddb_loads(struct ddb *db, const char *data, uint64_t length)
{
        const struct ddb_header *head = (const struct ddb_header*)data;

        if (length < sizeof(struct ddb_header)){
                db->errno = DDB_ERR_BUFFER_TOO_SMALL;
                return -1;
        }if (head->magic != DISCODB_MAGIC){
                db->errno = DDB_ERR_BUFFER_NOT_DISCODB;
                return -1;
        }
        if (head->size > length){
                db->errno = DDB_ERR_INVALID_BUFFER_SIZE;
                return -1;
        }

        db->buf = data;
        db->size = head->size;
        db->num_keys = head->num_keys;
        db->num_values = head->num_values;
        db->num_uniq_values = head->num_uniq_values;
        db->flags = head->flags;
        db->errno = 0;

        db->toc = (const uint64_t*)&data[head->toc_offs];
        db->values_toc = (const uint64_t*)&data[head->values_toc_offs];
        db->data = &data[head->data_offs];
        db->values = &data[head->values_offs];
        db->hash = &data[head->hash_offs];

        return 0;
}

void ddb_free(struct ddb *db)
{
        if (db && db->mmap)
                munmap(db->mmap, db->size);
        free(db);
}

char *ddb_dumps(struct ddb *db, uint64_t *length)
{
        char *d = NULL;
        if (!(d = malloc(db->size))){
                db->errno = DDB_ERR_OUT_OF_MEMORY;
                return NULL;
        }
        memcpy(d, db->buf, db->size);
        *length = db->size;
        return d;
}

int ddb_dump(struct ddb *db, int fd)
{
        const int bsize = 8192;
        uint64_t offs = 0;

        while (offs < db->size){
                size_t c = db->size - offs > bsize ? bsize: db->size - offs;
                ssize_t n = write(fd, &db->buf[offs], c);
                if (n == -1){
                        db->errno = DDB_ERR_WRITEFAILED;
                        return -1;
                }
                offs += n;
        }
        return 0;
}

static const struct ddb_entry *key_cursor_next(struct ddb_cursor *c)
{
        struct ddb_value_cursor tmp;

        if (c->cursor.keys.i == c->db->num_keys)
                return NULL;

        ddb_fetch_item(c->cursor.keys.i++,
                c->db->toc, c->db->data, &c->ent, &tmp);
        return &c->ent;
}

struct ddb_cursor *ddb_keys(struct ddb *db)
{
        struct ddb_cursor *c = NULL;
        if (!(c = calloc(1, sizeof(struct ddb_cursor)))){
                db->errno = DDB_ERR_OUT_OF_MEMORY;
                return NULL;
        }
        c->db = db;
        c->cursor.keys.i = 0;
        c->next = key_cursor_next;
        c->num_items = db->num_keys;
        return c;
}

static const struct ddb_entry *values_cursor_next(struct ddb_cursor *c)
{
        while (!c->cursor.values.cur.num_left){
                if (c->cursor.values.i == c->db->num_keys)
                        return NULL;
                ddb_fetch_item(c->cursor.values.i++, c->db->toc, c->db->data,
                        &c->ent, &c->cursor.values.cur);
        }
        ddb_value_cursor_step(&c->cursor.values.cur);
        ddb_resolve_valueid(c->db, c->cursor.values.cur.cur_id, &c->ent);
        return &c->ent;
}

struct ddb_cursor *ddb_values(struct ddb *db)
{
        struct ddb_cursor *c = NULL;
        if (!(c = calloc(1, sizeof(struct ddb_cursor)))){
                db->errno = DDB_ERR_OUT_OF_MEMORY;
                return NULL;
        }
        c->db = db;
        c->cursor.values.i = 0;
        c->cursor.values.cur.num_left = 0;
        c->next = values_cursor_next;
        c->num_items = db->num_values;
        return c;
}

static const struct ddb_entry *value_cursor_next(struct ddb_cursor *c)
{
        if (c->cursor.value.num_left){
                ddb_value_cursor_step(&c->cursor.value);
                ddb_resolve_valueid(c->db, c->cursor.value.cur_id, &c->ent);
                return &c->ent;
        }else
                return NULL;
}

static const struct ddb_entry *empty_next(struct ddb_cursor *c)
{
        return NULL;
}

struct ddb_cursor *ddb_getitem(struct ddb *db, const struct ddb_entry *key)
{
        int key_matches(const struct ddb_cursor *c)
        {
                return c->ent.length == key->length &&
                        !memcmp(c->ent.data, key->data, key->length);
        }

        struct ddb_cursor *c = NULL;
        if (!(c = calloc(1, sizeof(struct ddb_cursor)))){
                db->errno = DDB_ERR_OUT_OF_MEMORY;
                return NULL;
        }
        c->db = db;

        if (HASFLAG(db, F_HASH)){
                /* hash exists, perform O(1) lookup */
                uint32_t id = cmph_search_packed((void*)db->hash,
                        key->data, key->length);
                /* XXX Bug in cmph? It seems to sometimes return
                 * IDs that were never hashed with it if given an
                 * unknown key. This shouldn't happen. */
                if (id < db->num_keys)
                    ddb_fetch_item(id, db->toc, db->data,
                        &c->ent, &c->cursor.value);
                if (id >= db->num_keys || !key_matches(c)){
                        c->cursor.value.num_left = 0;
                        c->next = empty_next;
                        return c;
                }
        }else{
                /* no hash, perform linear scan */
                uint32_t i = db->num_keys;
                while (i--){
                        ddb_fetch_item(i, db->toc, db->data,
                                &c->ent, &c->cursor.value);
                        if (key_matches(c))
                                goto found;
                }
                c->cursor.value.num_left = 0;
                c->next = empty_next;
                return c;
        }
found:
        c->num_items = c->cursor.value.num_left;
        c->next = value_cursor_next;
        return c;
}

struct ddb_cursor *ddb_query(struct ddb *db,
        const struct ddb_query_clause *clauses, uint32_t length)
{
        struct ddb_cursor *c = NULL;

        /* CNF queries are not supported for multisets */
        if (HASFLAG(db, F_MULTISET)){
                db->errno = DDB_ERR_QUERY_NOT_SUPPORTED;
                return NULL;
        }

        if (!(c = calloc(1, sizeof(struct ddb_cursor)))){
                db->errno = DDB_ERR_OUT_OF_MEMORY;
                return NULL;
        }

        uint32_t j, k, i = length;
        uint32_t num_terms = 0;
        while (i--)
                num_terms += clauses[i].num_terms;

        c->db = db;
        c->num_items = 0;
        c->next = ddb_cnf_cursor_next;

        c->cursor.cnf.num_clauses = length;
        c->cursor.cnf.num_terms = num_terms;
        c->cursor.cnf.isect_offset = WINDOW_SIZE;

        if (!(c->cursor.cnf.clauses =
                        calloc(length, sizeof(struct ddb_cnf_clause))))
                goto err;
        if (!(c->cursor.cnf.terms =
                        calloc(num_terms, sizeof(struct ddb_cnf_term))))
                goto err;
        if (!(c->cursor.cnf.isect = calloc(1, WINDOW_SIZE_BYTES)))
                goto err;

        for (j = 0, i = 0; i < length; i++){
                c->cursor.cnf.clauses[i].terms = &c->cursor.cnf.terms[j];
                c->cursor.cnf.clauses[i].num_terms = clauses[i].num_terms;

                for (k = 0; k < clauses[i].num_terms; k++){
                        struct ddb_cnf_term *term = &c->cursor.cnf.terms[j++];
                        if (!(term->cursor = ddb_getitem(db, &clauses[i].terms[k].key)))
                                goto err;

                        if (clauses[i].terms[k].not)
                                term->next = ddb_not_next;
                        else
                                term->next = ddb_val_next;

                        term->next(term);
                }
        }
        return c;
err:
        db->errno = DDB_ERR_OUT_OF_MEMORY;
        ddb_free_cursor(c);
        return NULL;
}

void ddb_free_cursor(struct ddb_cursor *c)
{
        if (c && c->next == ddb_cnf_cursor_next){
                if (c->cursor.cnf.terms){
                        int i = c->cursor.cnf.num_terms;
                        while (i--)
                                free(c->cursor.cnf.terms[i].cursor);
                }
                free(c->cursor.cnf.clauses);
                free(c->cursor.cnf.terms);
                free(c->cursor.cnf.isect);
        }
        free(c);
}

uint64_t ddb_resultset_size(const struct ddb_cursor *c)
{
        return c->num_items;
}

int ddb_notfound(const struct ddb_cursor *c)
{
        return c->next == empty_next;
}

const struct ddb_entry *ddb_next(struct ddb_cursor *c)
{
        return c->next(c);
}

int ddb_error(struct ddb *db, const char **errstr)
{
        if (errstr)
                *errstr = ERR_STR[db->errno];
        int e = db->errno;
        db->errno = 0;
        return e;
}

void ddb_fetch_item(uint32_t i, const uint64_t *toc, const char *data,
        struct ddb_entry *key, struct ddb_value_cursor *val)
{
        const char *p = &data[toc[i]];

        key->length = *(uint32_t*)p;
        key->data = &p[4];

        val->num_left = *(uint32_t*)&p[4 + key->length];
        val->cur_id = 0;

        if (val->num_left){
                val->deltas = &p[8 + key->length];
                val->bits = read_bits(val->deltas, 0, 5) + 1;
                val->offset = 5;
        }
}

