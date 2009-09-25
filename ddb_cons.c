
/* GNU_SOURCE for open_memstream() */
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>

#include <Judy.h>
#include <cmph.h>

#include <discodb.h>
#include <ddb_internal.h>

struct ddb_cons_sect{
        FILE *f;
        char *ptr;
        size_t size;
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

        fclose(db->toc.f);
        free(db->toc.ptr);
        
        fclose(db->data.f);
        free(db->data.ptr);
        
        fclose(db->values.f);
        free(db->values.ptr);
        
        fclose(db->values_toc.f);
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

static void write_offset(struct ddb_cons_sect *d, struct ddb_cons_sect *toc)
{
        uint64_t offs = ftello(d->f);
        fwrite(&offs, 8, 1, toc->f);
}

static valueid_t new_value(struct ddb_cons *db, const struct ddb_entry *e)
{
        if (++db->next_id == DDB_MAX_NUM_VALUES)
                return 0;
        
        write_offset(&db->values, &db->values_toc);
        fwrite(e->data, e->length, 1, db->values.f);
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
                *length += sect[i].size;
        
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
                memcpy(&p[offs], sect[i].ptr, sect[i].size);
                offs += sect[i].size;
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

static void sect_new(struct ddb_cons_sect *sect)
{
        sect->f = open_memstream(&sect->ptr, &sect->size);
}

struct ddb_cons *ddb_new()
{
        struct ddb_cons *db = NULL;

        if (!(db = calloc(1, sizeof(struct ddb_cons))))
                return NULL;

        sect_new(&db->toc);
        sect_new(&db->data);
        sect_new(&db->values);
        sect_new(&db->values_toc);
        
        return db;
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
                if (copy_to_buf(db, values[i].data, values[i].length))
                        goto err;
                JHSI(id, db->valmap, db->tmpbuf, values[i].length);
                if (id == PJERR)
                        goto err;
                else if (*id)
                        db->idbuf[i] = (uint32_t)*id;
                else{
                        if (!(*id = new_value(db, &values[i])))
                                goto err;
                        db->idbuf[i] = *id;
                }
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
        write_offset(&db->data, &db->toc);
        fwrite(&key->length, 4, 1, db->data.f);
        fwrite(key->data, key->length, 1, db->data.f);
        fwrite(&num_values, 4, 1, db->data.f);
        fwrite(db->tmpbuf, size, 1, db->data.f);
        
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

        /* write final offsets to tocs */
        write_offset(&db->data, &db->toc);
        write_offset(&db->values, &db->values_toc);

        /* write zero padding to data, so read_bits is always safe */ 
        fwrite(pad, 7, 1, db->data.f);

        fclose(db->toc.f);
        fclose(db->data.f);
        fclose(db->values.f);
        fclose(db->values_toc.f);

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
        
        const char *packed = pack(db, hash, hash_size, length);
        ddb_free(db);
        return packed;
}

