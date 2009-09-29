
#include <string.h>

#include <cmph.h>

#include <discodb.h>
#include <ddb_internal.h>

#define BUF_INC (1024 * 1024 * 10)

struct ddb_cons_sect{
        char *ptr;
        uint64_t size;
        uint64_t offset;
};

struct ddb_cons{
        struct ddb_cons_sect toc;
        struct ddb_cons_sect data;
        struct ddb_cons_sect values;
        struct ddb_cons_sect values_toc;

        uint32_t is_multiset;
        uint32_t num_keys;
        uint64_t num_values;
        
        void *valmap;

        char *tmpbuf;
        uint32_t tmpbuf_len;

        valueid_t next_id;
        valueid_t *idbuf;
        uint32_t idbuf_len;
};

static int ddb_write(struct ddb_cons_sect *d, const void *buf, uint64_t len)
{
        if (d->offset + len > d->size){
                d->size += len + BUF_INC;
                if (!(d->ptr = realloc(d->ptr, d->size)))
                        return -1;
        }
        memcpy(&d->ptr[d->offset], buf, len);
        d->offset += len;
        return 0;
}

static int id_cmp(const void *p1, const void *p2)
{
        const valueid_t x = *(const valueid_t*)p1;
        const valueid_t y = *(const valueid_t*)p2;

        if (x > y)
                return 1;
        else if (x < y)
                return -1;
        return 0;
}

static void ddb_cons_free(struct ddb_cons *db)
{
        free(db->toc.ptr);
        free(db->data.ptr);
        free(db->values.ptr);
        free(db->values_toc.ptr);

        ddb_valuemap_free(db->valmap);

        free(db->tmpbuf);
        free(db->idbuf);
        free(db);
}

static uint32_t allocate_bits(struct ddb_cons *db, uint32_t size_in_bits)
{
        uint32_t len = size_in_bits >> 3;
        if (size_in_bits & 7)
                len += 1;
        /* + 8 is for write_bits and read_bits which may try to access
         * at most 7 bytes out of array bounds */
        if (len + 8 > db->tmpbuf_len){
                db->tmpbuf_len = len + 8;
                if (!(db->tmpbuf = realloc(db->tmpbuf, len + 8)))
                        return 0;
        }
        memset(db->tmpbuf, 0, len + 8);
        return len;
}


static uint32_t encode_values(struct ddb_cons *db, uint32_t num_values)
{
        /* find maximum delta -> bits needed per id */
        uint32_t i, max_diff = db->idbuf[0];
        for (i = 1; i < num_values; i++){
                uint32_t d = db->idbuf[i] - db->idbuf[i - 1];
                if (!d)
                        db->is_multiset = 1;
                if (d > max_diff)
                        max_diff = d;
        }
        
        uint64_t offs = 0;
        uint32_t prev = 0;
        uint32_t bits = bits_needed(max_diff);
        uint32_t size;
        if (!(size = allocate_bits(db, 5 + bits * num_values)))
                return 0;
        
        /* values field:
           [ bits_needed (5 bits) | delta-encoded values (bits * num_values) ]
        */
         
        write_bits(db->tmpbuf, offs, bits - 1);
        offs = 5;
        for (i = 0; i < num_values; i++){
                write_bits(db->tmpbuf, offs, db->idbuf[i] - prev);
                prev = db->idbuf[i];
                offs += bits;
        }
        return size;
}

static int write_offset(const struct ddb_cons_sect *d, struct ddb_cons_sect *toc)
{
        return ddb_write(toc, &d->offset, 8);
}

static valueid_t new_value(struct ddb_cons *db, const struct ddb_entry *e)
{
        int err = 0;
        if (++db->next_id == DDB_MAX_NUM_VALUES)
                return 0;
        
        err |= write_offset(&db->values, &db->values_toc);
        err |= ddb_write(&db->values, e->data, e->length);
        if (err)
                return 0;
        return db->next_id;
}

static char *pack(const struct ddb_cons *db, char *hash, 
        uint32_t hash_size, uint64_t *length)
{
        struct ddb_cons_sect hash_sect = {.ptr = hash, .offset = hash_size};
        struct ddb_cons_sect sect[] =
                {db->toc, db->values_toc, db->data, db->values, hash_sect};

        int i, num_sect = sizeof(sect) / sizeof(struct ddb_cons_sect);
        *length = sizeof(struct ddb_header);
        for (i = 0; i < num_sect; i++)
                *length += sect[i].offset;
        
        char *p = NULL;
        if (!(p = malloc(*length)))
                return NULL;

        struct ddb_header *head = (struct ddb_header*)p;
        
        uint64_t *offsets[] = 
                {&head->toc_offs, &head->values_toc_offs, 
                 &head->data_offs, &head->values_offs, &head->hash_offs};

        head->magic = DISCODB_MAGIC;
        head->size = *length;
        head->num_keys = db->num_keys;
        head->num_values = db->num_values;
        head->num_uniq_values = db->next_id;
        head->flags = 0;
       
        if (hash_size){
                SETFLAG(head, F_HASH);
        }
        if (db->is_multiset){
                SETFLAG(head, F_MULTISET);
        }

        uint64_t offs = sizeof(struct ddb_header);
        for (i = 0; i < num_sect; i++){
                *offsets[i] = offs;
                memcpy(&p[offs], sect[i].ptr, sect[i].offset);
                offs += sect[i].offset;
        }
        
        return p;
}

static int reorder_items(struct ddb_cons *db, char *hash)
{
        uint64_t *new_toc = NULL;
        if (!(new_toc = malloc(db->toc.offset)))
                return -1;

        const uint64_t *old_toc = (const uint64_t*)db->toc.ptr;
        uint32_t i = db->num_keys;
        while (i--){
                struct ddb_entry key;
                struct ddb_value_cursor val;
                ddb_fetch_item(i, old_toc, db->data.ptr, &key, &val);
                uint32_t id = cmph_search_packed(hash, key.data, key.length);
                new_toc[id] = old_toc[i];
        }
        free(db->toc.ptr);
        db->toc.ptr = (char*)new_toc;
        return 0;
}

static char *build_hash(const struct ddb_cons *db, uint32_t *size)
{
        struct hash_cursor {
                const uint64_t *toc;
                const char *data;
                uint32_t i;
                char *buf;
                uint32_t buf_size;
        } cur = {.toc = (const uint64_t*)db->toc.ptr,
                 .data = db->data.ptr,
                 .buf = NULL,
                 .buf_size = 0,
                 .i = 0};

        int hash_failed = 0;

        void xdispose(void *data, char *key, cmph_uint32 l) { }
        void xrewind(void *data) { ((struct hash_cursor*)data)->i = 0; }
        int xread(void *data, char **p, cmph_uint32 *len)
        {
                struct hash_cursor *hc = (struct hash_cursor*)data;
                struct ddb_entry key;
                struct ddb_value_cursor val;

                ddb_fetch_item(hc->i++, hc->toc, hc->data, &key, &val);

                if (key.length > hc->buf_size){
                        hc->buf_size = key.length;
                        if (!(hc->buf = realloc(hc->buf, hc->buf_size)))
                                hash_failed = 1;
                }
                if (hash_failed){
                        *len = 0;
                        *p = NULL;
                }else{
                        memcpy(hc->buf, key.data, key.length);
                        *len = key.length;
                        *p = hc->buf;
                }
                return key.length;
        }

        cmph_io_adapter_t r;
        r.data = &cur;
        r.nkeys = db->num_keys;
        r.read = xread;
        r.dispose = xdispose;
        r.rewind = xrewind;

        cmph_config_t *c = cmph_config_new(&r);
        cmph_config_set_algo(c, CMPH_CHD);
    
        if (getenv("DDB_DEBUG"))
                cmph_config_set_verbosity(c, 5);
        
        char *hash = NULL;
        cmph_t *g = cmph_new(c);
        *size = 0;
        if (g && !hash_failed){
                *size = cmph_packed_size(g);
                if ((hash = malloc(*size)))
                        cmph_pack(g, hash);
        }
        if (g)
                cmph_destroy(g);
        cmph_config_destroy(c);
        free(cur.buf);
        return hash;
}

struct ddb_cons *ddb_cons_new()
{
        struct ddb_cons* db;
        if (!(db = calloc(1, sizeof(struct ddb_cons))))
                return NULL;
        db->valmap = ddb_valuemap_init();
        return db;
}

int ddb_add(struct ddb_cons *db, const struct ddb_entry *key, 
        const struct ddb_entry *values, uint32_t num_values)
{
        int valeq(const struct ddb_entry *val, valueid_t id)
        {
                const uint64_t *toc = (const uint64_t*)db->values_toc.ptr;
                uint64_t offset = toc[id - 1];
                const char *p = &db->values.ptr[offset];
                uint32_t size = 0;
                if (id == db->next_id)
                        size = db->values.offset - offset;
                else
                        size = toc[id] - offset;
                if (size != val->length)
                        return 0;
                else
                        return memcmp(p, val->data, size) == 0;
        }
        uint32_t i;
        
        if (num_values > db->idbuf_len){
                db->idbuf_len = num_values;
                if (!(db->idbuf = realloc(db->idbuf, num_values * 4)))
                        goto err;
        }

        /* find IDs for values */
        for (i = 0; i < num_values; i++){
                valueid_t *id;
                if (!(id = ddb_valuemap_lookup(db->valmap, &values[i], valeq)))
                        goto err;
                if (!*id && !(*id = new_value(db, &values[i])))
                        goto err;
                db->idbuf[i] = *id;
        }

        /* sort by ascending IDs */
        qsort(db->idbuf, num_values, 4, id_cmp);

        /* delta-encode value ID list */
        uint32_t size = 0;
        if (num_values && !(size = encode_values(db, num_values)))
                goto err;
        
        /* write data 
         
           attribute entry:
           [ key_len | key | num_values | val_bits | delta-encoded values ] 
        */
        int r = 0;
        r |= write_offset(&db->data, &db->toc);
        r |= ddb_write(&db->data, &key->length, 4);
        r |= ddb_write(&db->data, key->data, key->length);
        r |= ddb_write(&db->data, &num_values, 4);
        if (size)
                r |= ddb_write(&db->data, db->tmpbuf, size);
        if (r)
                goto err;

        db->num_values += num_values;
        if (++db->num_keys == DDB_MAX_NUM_KEYS)
                goto err;

        return 0;
err:
        ddb_cons_free(db);
        return -1;
}

char *ddb_finalize(struct ddb_cons *db, uint64_t *length)
{
        static char pad[7];
        char *packed = NULL;
        char *hash = NULL; 
        int err = 0;

        /* write final offsets to tocs */
        err |= write_offset(&db->data, &db->toc);
        err |= write_offset(&db->values, &db->values_toc);

        /* write zero padding to data, so read_bits is always safe */ 
        err |= ddb_write(&db->data, pad, 7);
        if (err)
                goto end;

        ddb_valuemap_free(db->valmap);
        db->valmap = NULL;

        uint32_t hash_size = 0;
        if (db->num_keys > DDB_HASH_MIN_KEYS){
                if (!(hash = build_hash(db, &hash_size)))
                        goto end;
                if (reorder_items(db, hash))
                        goto end;
        }
        packed = pack(db, hash, hash_size, length);
end:
        free(hash);
        ddb_cons_free(db);
        return packed;
}

