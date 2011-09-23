
#include <string.h>
#include <limits.h>
#include <stdio.h>

#include <cmph.h>

#include <discodb.h>
#include <ddb_internal.h>

#include <ddb_profile.h>
#include <ddb_map.h>
#include <ddb_deltalist.h>
#include <ddb_delta.h>
#include <ddb_cmph.h>

/* Idea:
   DiscoDB's memory footprint can be huge in the worst case. Consider e.g.

   DiscoDB((title, str(i)) for i, title in enumerate(file('wikipedia-titles')))

   which is pretty much the worst case: all keys and values are unique, so
   keys_map and values_map just waste space for nothing. Of course there's no way 
   DiscoDB could know this in advance.

   We could provide an alternative interface where the user can maintain the
   key/value -> id mapping and hence use all the domain information to conserve
   memory. The interface could look as follows:

   uint64_t value_id = ddb_cons_new_value(const struct ddb_entry *value);
   uint64_t key_id = ddb_cons_new_key(const struct ddb_entry *key);
   int ret = ddb_cons_add_id(struct ddb_cons *db, uint64_t key_id, uint64_t value_id);

   In this scenario DiscoDB does not need to maintain internal mappings at all,
   only two flat arrays for keys (id -> deltalist) and (id -> key) and one for
   values (id -> value).

   This would be especially convenient in the situations where keys and/or values
   are unique or grouped - neither user nor discodb needs to maintain a mapping,
   just a one-time id would suffice.
*/

#define BUFFER_INC (1024 * 1024 * 64)

struct ddb_cons{
    struct ddb_map *values_map;
    struct ddb_map *keys_map;
    uint64_t uvalues_total_size;
#ifdef DDB_PROFILE
    uint64_t counter;
#endif
};

struct ddb_packed{
    uint64_t toc_offs;
    uint64_t offs;
    uint64_t size;

    char *buffer;
    struct ddb_header *head;

    struct ddb_codebook codebook[DDB_CODEBOOK_SIZE];
};

static void print_mem_usage(const struct ddb_cons *db);

static int _buffer_grow(struct ddb_packed *p, uint64_t size)
{
    if (p->offs + size > p->size){
        p->size += size + BUFFER_INC;
        if (!(p->head = (struct ddb_header*)
                (p->buffer = realloc(p->buffer, p->size))))
            return -1;
    }
    return 0;
}

static void buffer_shrink(struct ddb_packed *p)
{
    p->buffer = realloc(p->buffer, p->offs);
}

static struct ddb_packed *buffer_init(void)
{
    struct ddb_packed *p;
    if (!(p = calloc(1, sizeof(struct ddb_packed))))
        return NULL;
    p->offs = sizeof(struct ddb_header);
    if (_buffer_grow(p, sizeof(struct ddb_header))){
        free(p);
        return NULL;
    }
    return p;
}

static int buffer_new_section(struct ddb_packed *p, uint64_t num_items)
{
    if (_buffer_grow(p, num_items * 8))
        return -1;
    p->toc_offs = p->offs;
    p->offs += num_items * 8;
    return 0;
}

static void buffer_toc_mark(struct ddb_packed *p)
{
    memcpy(&p->buffer[p->toc_offs], &p->offs, 8);
    p->toc_offs += 8;
}

static int buffer_write_data(struct ddb_packed *p,
                             const char *src, uint64_t size)
{
    if (_buffer_grow(p, size))
        return -1;
    memcpy(&p->buffer[p->offs], src, size);
    p->offs += size;
    return 0;
}

static int pack_key2values(struct ddb_packed *pack,
                           const struct ddb_entry *keys,
                           const struct ddb_map *keys_map,
                           int unique_items)
{
    valueid_t *values = NULL;
    uint64_t values_size = 0;
    char *dbuf = NULL;
    uint64_t dbuf_size = 0;
    int i, ret = -1;
    uint32_t num = pack->head->num_keys;

    if (buffer_new_section(pack, num + 1))
        goto end;

    for (i = 0; i < num; i++){
        uint64_t *ptr = ddb_map_lookup_str(keys_map, &keys[i]);
        uint64_t size, num_values;
        uint32_t num_written;
        int duplicates = 0;

        const struct ddb_deltalist *d = (const struct ddb_deltalist*)*ptr;
        if (ddb_deltalist_to_array(d, &num_values, &values, &values_size))
            goto end;

        if (num_values > UINT32_MAX)
            goto end;

        if (ddb_delta_encode(values,
                             (uint32_t)num_values,
                             &dbuf,
                             &dbuf_size,
                             &size,
                             &num_written,
                             &duplicates,
                             unique_items))
            goto end;

        pack->head->num_values += num_written;
        if (duplicates){
            SETFLAG(pack->head, F_MULTISET);
        }

        buffer_toc_mark(pack);
        if (buffer_write_data(pack, (const char*)&keys[i].length, 4))
            goto end;
        if (buffer_write_data(pack, keys[i].data, keys[i].length))
            goto end;
        if (buffer_write_data(pack, dbuf, size))
            goto end;
    }
    buffer_toc_mark(pack);
    ret = 0;
end:
    free(values);
    free(dbuf);
    return ret;
}

#ifdef HUFFMAN_DEBUG
static int ccmp(const char *x, const char *y, uint32_t len)
{
    uint32_t i = 0;
    for (i = 0; i < len; i++)
        if (x[i] != y[i]){
            fprintf(stderr, "%u) GOT %c SHOULD BE %c\n", i, x[i], y[i]);
            return 1;
        }
    return 0;
}
#endif

static int pack_id2value(struct ddb_packed *pack,
                         const struct ddb_map *values_map,
                         int disable_compr)
{
    struct ddb_map *code = NULL;
    struct ddb_map_cursor *c = NULL;
    struct ddb_entry key;
    uint32_t size;
    int err = -1;

    char *buf = NULL;
    const char *val = NULL;
    uint64_t buf_len = 0;

    if (buffer_new_section(pack, ddb_map_num_items(values_map) + 1))
        goto end;

    if (!disable_compr){
        SETFLAG(pack->head, F_COMPRESSED);
        if (!(code = ddb_create_codemap(values_map)))
            goto end;
        if (ddb_save_codemap(code, pack->codebook))
            goto end;
    }

    if (!(c = ddb_map_cursor_new(values_map)))
        goto end;

    #ifdef HUFFMAN_DEBUG
    uint32_t dsize;
    char *dbuf = NULL;
    uint64_t dbuf_len = 0;
    #endif

    while (ddb_map_next_str(c, &key)){
        if (disable_compr){
            val = key.data;
            size = key.length;
        }else{
            if (ddb_compress(code, key.data, key.length,
                             &size, &buf, &buf_len))
                goto end;
            val = buf;
            #ifdef HUFFMAN_DEBUG
            ddb_decompress(pack->codebook, buf, size,
                &dsize, &dbuf, &dbuf_len);
            if (dsize != key.length || ccmp(dbuf, key.data, dsize)){
                fprintf(stderr, "ORIG: <%.*s> DECOMP: <%.*s> (%u and %u)\n",
                    key.length, key.data, dsize, dbuf, dsize, key.length);
                exit(1);
            }
            #endif
        }

        buffer_toc_mark(pack);
        if (buffer_write_data(pack, val, size))
            goto end;
    }
    buffer_toc_mark(pack);

    /* write dummy data in the end, to make sure that ddb_decompress
       never exceed the section limits */
    size = 0;
    if (buffer_write_data(pack, (const char*)&size, 4))
        goto end;

    err = 0;
end:
    #ifdef HUFFMAN_DEBUG
    free(dbuf);
    #endif
    ddb_map_cursor_free(c);
    ddb_map_free(code);
    free(buf);
    return err;
}

static int pack_codebook(struct ddb_packed *pack)
{
    buffer_new_section(pack, 0);
    return buffer_write_data(pack,
        (char*)pack->codebook, sizeof(pack->codebook));
}

static struct ddb_entry *pack_hash(struct ddb_packed *pack,
                                   struct ddb_map *keys_map)
{
    char *hash = NULL;
    struct ddb_entry *order = NULL;
    struct ddb_map_cursor *c = NULL;
    struct ddb_entry key;
    uint32_t i = 0;
    int err = -1;

    if (!(order = malloc(pack->head->num_keys * sizeof(struct ddb_entry))))
        goto end;

    if (pack->head->num_keys > DDB_HASH_MIN_KEYS){
        uint32_t hash_size = 0;
        if (!(hash = ddb_build_cmph(keys_map, &hash_size)))
            goto end;
        buffer_new_section(pack, 0);
        if (buffer_write_data(pack, hash, hash_size))
            goto end;
        SETFLAG(pack->head, F_HASH);
    }

    if (!(c = ddb_map_cursor_new(keys_map)))
        goto end;
    while (ddb_map_next_str(c, &key)){
        if (hash)
            i = cmph_search_packed(hash, key.data, key.length);
        order[i++] = key;
    }
    err = 0;
end:
    ddb_map_cursor_free(c);
    free(hash);
    if (err){
        free(order);
        return NULL;
    }
    return order;
}

static int pack_header(struct ddb_packed *pack, const struct ddb_cons *cons)
{
    struct ddb_header *head = pack->head;
    memset(head, 0, sizeof(struct ddb_header));

    buffer_new_section(pack, 0);
    head->magic = DISCODB_MAGIC;
    head->flags = 0;
    head->num_keys = ddb_map_num_items(cons->keys_map);
    head->num_uniq_values = ddb_map_num_items(cons->values_map);
    /* num_values is set in key2values after removing duplicates (maybe) */
    head->num_values = 0;
    return 0;
}

static int maybe_disable_compression(const struct ddb_cons *cons)
{
    /* It doesn't make sense to compress a small set of values as
     * the huffman codebook has 0.5M overhead. */
    if (cons->uvalues_total_size < COMPRESS_MIN_TOTAL_SIZE)
        return DDB_OPT_DISABLE_COMPRESSION;
    /* Compression ignores values less than 4 bytes. Values that
     * are exactly 4 bytes are handled by duplicate removal, so
     * compressing them is useless. Hence, values need to be at
     * least 5 bytes to benefit from compression. */
    double num = ddb_map_num_items(cons->values_map);
    if (cons->uvalues_total_size / num < COMPRESS_MIN_AVG_VALUE_SIZE)
        return DDB_OPT_DISABLE_COMPRESSION;
    return 0;
}


char *ddb_cons_finalize(struct ddb_cons *cons, uint64_t *length, uint64_t flags)
{
    struct ddb_packed *pack = NULL;
    struct ddb_entry *order = NULL;
    char *buf;
    int disable_compression, err = 1;
    DDB_TIMER_DEF

    if (!(pack = buffer_init()))
        goto err;

    if (pack_header(pack, cons))
        goto err;

#ifdef DDB_PROFILE
    print_mem_usage(cons);
#endif

    DDB_TIMER_START
    pack->head->hash_offs = pack->offs;
    if (!(order = pack_hash(pack, cons->keys_map)))
        goto err;
    DDB_TIMER_END("hash")

    DDB_TIMER_START
    pack->head->key2values_offs = pack->offs;
    if (pack_key2values(pack, order, cons->keys_map,
            flags & DDB_OPT_UNIQUE_ITEMS))
        goto err;
    DDB_TIMER_END("key2values")

    DDB_TIMER_START
    flags |= maybe_disable_compression(cons);
    disable_compression = flags & DDB_OPT_DISABLE_COMPRESSION;
    pack->head->id2value_offs = pack->offs;
    if (pack_id2value(pack, cons->values_map, disable_compression))
        goto err;
    else{
        ddb_map_free(cons->values_map);
        cons->values_map = NULL;
    }
    DDB_TIMER_END("id2values")

    if (!disable_compression){
        DDB_TIMER_START
        pack->head->codebook_offs = pack->offs;
        if (pack_codebook(pack))
            goto err;
        DDB_TIMER_END("save_codebook")
    }
    pack->head->size = *length = pack->offs;
    buffer_shrink(pack);
    err = 0;
err:
    buf = pack->buffer;
    free(order);
    free(pack);
    if (err){
        free(buf);
        return NULL;
    }
    return buf;
}

struct ddb_cons *ddb_cons_new()
{
    struct ddb_cons* db;
    if (!(db = calloc(1, sizeof(struct ddb_cons))))
        return NULL;

    db->keys_map = ddb_map_new(UINT_MAX);
    db->values_map = ddb_map_new(UINT_MAX);

    if (!(db->keys_map && db->values_map)){
        ddb_map_free(db->keys_map);
        ddb_map_free(db->values_map);
        free(db);
        return NULL;
    }
    return db;
}

void ddb_cons_free(struct ddb_cons *cons)
{
    struct ddb_map_cursor *c;
    struct ddb_entry key;
    uint64_t *ptr;

    ddb_map_free(cons->values_map);

    if (!(cons->keys_map && (c = ddb_map_cursor_new(cons->keys_map))))
        /* memory leak! not everything was freed */
        return;

    while (ddb_map_next_item_str(c, &key, &ptr))
        free((void*)*ptr);

    ddb_map_cursor_free(c);
    ddb_map_free(cons->keys_map);
    free(cons);
}


int ddb_cons_add(struct ddb_cons *db,
            const struct ddb_entry *key,
            const struct ddb_entry *value)
{
    uint64_t *val_ptr;
    uint64_t *key_ptr;
    valueid_t value_id;
    struct ddb_deltalist *value_list;

    if (!(key_ptr = ddb_map_insert_str(db->keys_map, key)))
        return -1;
    if (!*key_ptr && !(*key_ptr = (uint64_t)ddb_deltalist_new()))
        return -1;
    value_list = (struct ddb_deltalist*)*key_ptr;

    if (value){
        if (!(val_ptr = ddb_map_insert_str(db->values_map, value)))
            return -1;
        if (!*val_ptr){
            *val_ptr = ddb_map_num_items(db->values_map);
            db->uvalues_total_size += value->length;
        }
        value_id = *val_ptr;
        if (ddb_deltalist_append(value_list, value_id))
            return -1;
    }

#ifdef DDB_PROFILE
    if (!(++db->counter & 1048575))
        print_mem_usage(db);
#endif

    return 0;
}

#ifdef DDB_PROFILE
static int keys_mem_usage(const struct ddb_map *map,
                          uint64_t *total_alloc,
                          uint64_t *total_used)
{
    struct ddb_map_cursor *c;
    struct ddb_entry key;
    uint64_t *ptr;

    if (!(c = ddb_map_cursor_new(map)))
        return -1;

    *total_alloc = *total_used = 0;
    while (ddb_map_next_item_str(c, &key, &ptr)){
        uint64_t alloc, used, segments;
        struct ddb_deltalist *d = (struct ddb_deltalist*)*ptr;
        ddb_deltalist_mem_usage(d, &segments, &alloc, &used);
        *total_alloc += alloc;
        *total_used += used;
    }
    ddb_map_cursor_free(c);
    return 0;
}

static void print_map_mem_usage(const struct ddb_map_stat *stat)
{
    printf("num items: %llu\n", stat->num_items);
    printf("num leaves: %llu\n", stat->num_leaves);
    printf("leaves alloc: %llu\n", stat->leaves_alloc);
    printf("leaves used: %llu\n", stat->leaves_used);
    printf("num keys: %llu\n", stat->num_keys);
    printf("keys alloc: %llu\n", stat->keys_alloc);
    printf("keys used: %llu\n", stat->keys_used);
    printf("membuf alloc: %llu\n", stat->membuf_alloc);
    printf("membuf used: %llu\n", stat->membuf_used);
}

static void print_mem_usage(const struct ddb_cons *db)
{
    struct ddb_map_stat stat;
    uint64_t alloc = 0, used = 0;

    printf("-- keys --\n");
    keys_mem_usage(db->keys_map, &alloc, &used);
    ddb_map_mem_usage(db->keys_map, &stat);
    print_map_mem_usage(&stat);
    printf("LISTS: alloc %llu used %llu\n", alloc, used);
    printf("-- values --\n");
    ddb_map_mem_usage(db->values_map, &stat);
    print_map_mem_usage(&stat);
}
#endif

