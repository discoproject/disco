
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>

#include <cmph.h>

#include <discodb.h>
#include <ddb_internal.h>

#include <ddb_huffman.h>

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

static void *acalloc(size_t size)
{
#ifdef DDB_ALLOC_ALIGN
    void *p;
    if (posix_memalign(&p, sizeof(void*), size))
        return NULL;
    memset(p, 0, size);
    return p;
#else
    return calloc(1, size);
#endif
}

int ddb_get_valuestr(struct ddb_cursor *c, valueid_t id)
{
    if (c->no_valuestr)
        return c->errno;
    
    const struct ddb *db = c->db;
    uint64_t len = db->id2value[id] - db->id2value[id - 1];
    const char *data = &db->buf[db->id2value[id - 1]];

    if (HASFLAG(db, F_COMPRESSED)){
        if (ddb_decompress(db->codebook, data, len, &c->entry.length,
                &c->decode_buf, &c->decode_buf_len)){
            c->errno = DDB_ERR_OUT_OF_MEMORY;
            c->entry.length = 0;
            c->entry.data = NULL;
        }
        c->entry.data = c->decode_buf;
    }else{
        c->entry.length = len;
        c->entry.data = data;
    }
    return c->errno;
}

static void get_item(struct ddb_cursor *c,
                     keyid_t id,
                     struct ddb_delta_cursor *delta)
{
    const struct ddb *db = c->db;
    const char *p = &db->buf[db->key2values[id]];

    c->entry.length = *(uint32_t*)p;
    c->entry.data = &p[4];

    if (delta)
        ddb_delta_cursor(delta, &p[4 + c->entry.length]);
}

struct ddb *ddb_new()
{
    struct ddb *db = NULL;
    if (!(db = acalloc(sizeof(struct ddb))))
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
    db->mmap_size = nfo.st_size - mmap_offset;
    db->mmap = mmap(0, db->mmap_size, PROT_READ, MAP_SHARED, fd, mmap_offset);

    if (db->mmap == MAP_FAILED){
        db->errno = DDB_ERR_MMAP_FAILED;
        return -1;
    }
    return ddb_loads(db, db->mmap + (offset - mmap_offset), nfo.st_size - offset);
}

static const uint64_t *load_sect(const struct ddb *db, uint64_t offset)
{
    return (const uint64_t*)&db->buf[offset];
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

    db->key2values = load_sect(db, head->key2values_offs);
    db->id2value = load_sect(db, head->id2value_offs);
    db->hash = load_sect(db, head->hash_offs);

    db->codebook = (const struct ddb_codebook*)&db->buf[head->codebook_offs];

    return 0;
}

void ddb_free(struct ddb *db)
{
    if (db && db->mmap)
        munmap(db->mmap, db->mmap_size);
    free(db);
}

char *ddb_dumps(struct ddb *db, uint64_t *length)
{
    char *d = NULL;
    if (!(d = acalloc(db->size))){
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
    if (c->cursor.keys.i == c->db->num_keys)
        return NULL;

    get_item(c, c->cursor.keys.i++, NULL);
    return &c->entry;
}

struct ddb_cursor *ddb_keys(struct ddb *db)
{
    struct ddb_cursor *c = NULL;
    if (!(c = acalloc(sizeof(struct ddb_cursor)))){
        db->errno = DDB_ERR_OUT_OF_MEMORY;
        return NULL;
    }
    c->db = db;
    c->cursor.keys.i = 0;
    c->next = key_cursor_next;
    c->num_items = db->num_keys;
    return c;
}

static const struct ddb_entry *unique_values_cursor_next(struct ddb_cursor *c)
{
    if (c->cursor.uvalues.i > c->db->num_uniq_values)
        return NULL;
    if (ddb_get_valuestr(c, c->cursor.uvalues.i++))
        return NULL;
    return &c->entry;
}

struct ddb_cursor *ddb_unique_values(struct ddb *db)
{
    struct ddb_cursor *c = NULL;
    if (!(c = acalloc(sizeof(struct ddb_cursor)))){
        db->errno = DDB_ERR_OUT_OF_MEMORY;
        return NULL;
    }
    c->db = db;
    c->cursor.uvalues.i = 1;
    c->next = unique_values_cursor_next;
    c->num_items = db->num_uniq_values;
    return c;
}

static const struct ddb_entry *values_cursor_next(struct ddb_cursor *c)
{
    /* skip empty values */
    while (!c->cursor.values.cur.num_left){
        if (c->cursor.values.i == c->db->num_keys)
            return NULL;
        get_item(c, c->cursor.values.i++, &c->cursor.values.cur);
    }
    ddb_delta_cursor_next(&c->cursor.values.cur);
    if (ddb_get_valuestr(c, c->cursor.values.cur.cur_id))
        return NULL;
    return &c->entry;
}

struct ddb_cursor *ddb_values(struct ddb *db)
{
    struct ddb_cursor *c = NULL;
    if (!(c = acalloc(sizeof(struct ddb_cursor)))){
        db->errno = DDB_ERR_OUT_OF_MEMORY;
        return NULL;
    }
    c->db = db;
    c->cursor.values.i = 0;
    c->next = values_cursor_next;
    c->num_items = db->num_values;
    return c;
}

static const struct ddb_entry *value_cursor_next(struct ddb_cursor *c)
{
    if (c->cursor.value.num_left){
        ddb_delta_cursor_next(&c->cursor.value);
        if (ddb_get_valuestr(c, c->cursor.value.cur_id))
            return NULL;
        return &c->entry;
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
        return c->entry.length == key->length &&
            !memcmp(c->entry.data, key->data, key->length);
    }

    struct ddb_cursor *c = NULL;
    if (!(c = acalloc(sizeof(struct ddb_cursor)))){
        db->errno = DDB_ERR_OUT_OF_MEMORY;
        return NULL;
    }
    c->db = db;

    if (HASFLAG(db, F_HASH)){
        /* hash exists, perform O(1) lookup */
        uint32_t id = cmph_search_packed((void*)db->hash,
            key->data, key->length);
        if (id < db->num_keys)
            get_item(c, id, &c->cursor.value);
        if (id >= db->num_keys || !key_matches(c)){
            c->num_items = c->cursor.value.num_left = 0;
            c->next = empty_next;
            return c;
        }
    }else{
        /* no hash, perform linear scan */
        uint32_t i = db->num_keys;
        while (i--){
            get_item(c, i, &c->cursor.value);
            if (key_matches(c))
                goto found;
        }
        c->num_items = c->cursor.value.num_left = 0;
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

    if (!(c = acalloc(sizeof(struct ddb_cursor)))){
        db->errno = DDB_ERR_OUT_OF_MEMORY;
        return NULL;
    }

    uint32_t j, k, i = length;
    uint32_t num_terms = 0;
    while (i--)
        num_terms += clauses[i].num_terms;

    c->db = db;
    c->num_items = 0;

    if (length)
        c->next = ddb_cnf_cursor_next;
    else{
        c->next = empty_next;
        return c;
    }

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

            if (clauses[i].terms[k].nnot)
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

int ddb_free_cursor(struct ddb_cursor *c)
{
    if (c){
        int errno = c->errno;
        if(c->next == ddb_cnf_cursor_next){
            if (c->cursor.cnf.terms){
                int i = c->cursor.cnf.num_terms;
                while (i--)
                    free(c->cursor.cnf.terms[i].cursor);
            }
            free(c->cursor.cnf.clauses);
            free(c->cursor.cnf.terms);
            free(c->cursor.cnf.isect);
        }
        free(c->decode_buf);
        free(c);
        return errno;
    }
    return 0;
}

uint64_t ddb_resultset_size(const struct ddb_cursor *c)
{
    return c->num_items;
}

uint64_t ddb_cursor_count(struct ddb_cursor *c, int *err)
{
    uint64_t n = 0;
    c->no_valuestr = 1;
    *err = 0;
    while (ddb_next(c, err) && !*err)
        ++n;
    return n;
}

int ddb_notfound(const struct ddb_cursor *c)
{
    return c->next == empty_next;
}

const struct ddb_entry *ddb_next(struct ddb_cursor *c, int *err)
{
    const struct ddb_entry *e = c->next(c);
    *err = c->errno;
    return e;
}

int ddb_error(const struct ddb *db, const char **errstr)
{
    if (errstr)
        *errstr = ERR_STR[db->errno];
    return db->errno;
}

void ddb_features(const struct ddb *db, ddb_features_t features)
{
    features[DDB_NUM_KEYS] = db->num_keys;
    features[DDB_NUM_UNIQUE_VALUES] = db->num_uniq_values;
    features[DDB_NUM_VALUES] = db->num_values;
    features[DDB_TOTAL_SIZE] = db->size;

    features[DDB_VALUES_SIZE] =
        db->id2value[db->num_uniq_values] - db->id2value[0];
    features[DDB_ITEMS_SIZE] =
        db->key2values[db->num_keys] - db->key2values[0];

    features[DDB_IS_COMPRESSED] = HASFLAG(db, F_COMPRESSED);
    features[DDB_IS_HASHED] = HASFLAG(db, F_HASH);
    features[DDB_IS_MULTISET] = HASFLAG(db, F_MULTISET);
}

