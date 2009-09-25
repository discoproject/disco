
#include <string.h>

#include <Judy.h>
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

        uint32_t num_keys;
        Pvoid_t valmap;

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

static int copy_to_buf(struct ddb_cons *db, const void *src, uint32_t len)
{
        if (len > db->tmpbuf_len){
                db->tmpbuf_len = len;
                if (!(db->tmpbuf = realloc(db->tmpbuf, len)))
                        return -1;
        }
        memcpy(db->tmpbuf, src, len);
        return 0;
}

static uint32_t allocate_bits(struct ddb_cons *db, uint32_t size_in_bits)
{
        uint32_t len = size_in_bits >> 3;
        if (!len)
                len = 1;
        /* + 8 is for write_bits and read_bits which may try to access
         * at most 7 bytes out of array bounds */
        if (len + 8 > db->tmpbuf_len){
                db->tmpbuf_len = len + 8;
                if (!(db->tmpbuf = realloc(db->tmpbuf, len + 8)))
                        return 0;
        }
        memset(db->tmpbuf, 0, len);
        return len;
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

static void ddb_free(struct ddb_cons *db)
{
        Word_t tmp;

        free(db->toc.ptr);
        free(db->data.ptr);
        free(db->values.ptr);
        free(db->values_toc.ptr);

        JHSFA(tmp, db->valmap);

        free(db->tmpbuf);
        free(db->idbuf);
        free(db);
}

static uint32_t encode_values(struct ddb_cons *db, uint32_t num_values)
{
        /* find maximum delta -> bits needed per id */
        uint32_t i, max_diff = db->idbuf[0];
        for (i = 1; i < num_values; i++){
                uint32_t d = db->idbuf[i] - db->idbuf[i - 1];
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
         
        write_bits(db->tmpbuf, offs, bits);
        offs += 5;
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

static const char *pack(const struct ddb_cons *db, char *hash, 
        uint32_t hash_size, uint64_t *length)
{
        struct ddb_cons_sect hash_sect = {.ptr = hash, .size = hash_size};
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
        head->num_values = db->next_id;
        head->hashash = hash_size > 0 ? 1: 0;
        
        uint64_t offs = sizeof(struct ddb_header);
        for (i = 0; i < num_sect; i++){
                *offsets[i] = offs;
                memcpy(&p[offs], sect[i].ptr, sect[i].offset);
                offs += sect[i].offset;
        }
        
        return p;
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
        cmph_config_destroy(c);
        cmph_destroy(g);
        free(cur.buf);
        return hash;
}

struct ddb_cons *ddb_new()
{
        return (struct ddb_cons*)calloc(1, sizeof(struct ddb_cons));
}

int ddb_add(struct ddb_cons *db, const struct ddb_entry *key, 
        const struct ddb_entry *values, uint32_t num_values)
{
        uint32_t i = num_values; 
        
        if (num_values > db->idbuf_len){
                db->idbuf_len = num_values;
                if (!(db->idbuf = realloc(db->idbuf, num_values * 4)))
                        goto err;
        }

        /* find IDs for values */
        while (i--){
                Word_t *id = NULL;
                Word_t tmp = 0;
                if (values[i].length < DDB_MIN_BLOB_SIZE){
                        if (copy_to_buf(db, values[i].data, values[i].length))
                                goto err;
                        JHSI(id, db->valmap, db->tmpbuf, values[i].length);
                }else
                        id = &tmp;
                if (!*id && !(*id = new_value(db, &values[i])))
                        goto err;
                db->idbuf[i] = *id;
        }

        /* sort by ascending IDs */
        qsort(db->idbuf, num_values, 4, id_cmp);

        /* delta-encode value ID list */
        uint32_t size = 0;
        if (!(size = encode_values(db, num_values)))
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
        r |= ddb_write(&db->data, db->tmpbuf, size);
        if (r)
                goto err;

        if (++db->num_keys == DDB_MAX_NUM_KEYS)
                goto err;

        return 0;
err:
        ddb_free(db);
        return -1;
}

const char *ddb_finalize(struct ddb_cons *db, uint64_t *length)
{
        static char pad[7];
        const char *packed = NULL;
        int err = 0;

        /* write final offsets to tocs */
        err |= write_offset(&db->data, &db->toc);
        err |= write_offset(&db->values, &db->values_toc);

        /* write zero padding to data, so read_bits is always safe */ 
        err |= ddb_write(&db->data, pad, 7);
        if (err)
                goto end;

        Word_t tmp;
        JHSFA(tmp, db->valmap);
        db->valmap = NULL;

        uint32_t hash_size = 0;
        char *hash = NULL; 
        if (db->num_keys > DDB_HASH_MIN_KEYS)
                if (!(hash = build_hash(db, &hash_size))){
                        ddb_free(db);
                        return NULL;
                }
        
        packed = pack(db, hash, hash_size, length);
end:
        ddb_free(db);
        return packed;
}

