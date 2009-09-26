
#include <stdlib.h>
#include <string.h>

#include <cmph.h>

#include <discodb.h>
#include <ddb_internal.h>

struct ddb *ddb_loads(const char *data, uint64_t length)
{
        const struct ddb_header *head = (const struct ddb_header*)data;
        
        if (length < sizeof(struct ddb_header))
                return NULL;
        if (head->magic != DISCODB_MAGIC)
                return NULL;
        if (head->size != length)
                return NULL;

        struct ddb *db = NULL;
        if (!(db = malloc(sizeof(struct ddb))))
                return NULL;

        db->buf = data;
        db->size = head->size;
        db->num_keys = head->num_keys;
        db->num_values = head->num_values;
        db->hashash = head->hashash;

        db->toc = (const uint64_t*)&data[head->toc_offs];
        db->values_toc = (const uint64_t*)&data[head->values_toc_offs];
        db->data = &data[head->data_offs];
        db->values = &data[head->values_offs];
        db->hash = &data[head->hash_offs];
        
        return db;
}

const char *ddb_dumps(const struct ddb *db, uint64_t *length)
{
        char *d = NULL;
        if (!(d = malloc(db->size)))
                return NULL;
        memcpy(d, db->buf, db->size);
        *length = db->size;
        return d;
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

struct ddb_cursor *ddb_keys(const struct ddb *db)
{
        struct ddb_cursor *c = NULL;
        if (!(c = malloc(sizeof(struct ddb_cursor))))
                return NULL;
        c->db = db;
        c->cursor.keys.i = 0;
        c->next = key_cursor_next;
        c->num_items = db->num_keys;
        return c;
}

static const struct ddb_entry *values_cursor_next(struct ddb_cursor *c)
{
        if (!c->cursor.values.cur.num_left){
                if (c->cursor.values.i == c->db->num_keys)
                        return NULL;
                ddb_fetch_item(c->cursor.values.i++, c->db->toc, c->db->data, 
                        &c->ent, &c->cursor.values.cur);
        }
        ddb_fetch_nextval(&c->ent, c->db, &c->cursor.values.cur);
        return &c->ent;
}

struct ddb_cursor *ddb_values(const struct ddb *db)
{
        struct ddb_cursor *c = NULL;
        if (!(c = malloc(sizeof(struct ddb_cursor))))
                return NULL;
        c->db = db;
        c->cursor.values.i = 0;
        c->cursor.values.cur.num_left = 0;
        c->next = values_cursor_next;
        c->num_items = 0;
        return c;
}

static const struct ddb_entry *value_cursor_next(struct ddb_cursor *c)
{
        if (c->cursor.value.num_left){
                ddb_fetch_nextval(&c->ent, c->db, &c->cursor.value);
                return &c->ent;
        }else
                return NULL;
}

struct ddb_cursor *ddb_getitem(const struct ddb *db, const struct ddb_entry *key)
{
        int key_matches(const struct ddb_cursor *c)
        {
                return c->ent.length == key->length &&
                        !memcmp(c->ent.data, key->data, key->length);
        }

        struct ddb_cursor *c = NULL;
        if (!(c = malloc(sizeof(struct ddb_cursor))))
                return NULL;

        if (db->hashash){
                uint32_t id = cmph_search_packed((void*)db->hash, key->data, key->length);
                ddb_fetch_item(id, db->toc, db->data, &c->ent, &c->cursor.value);
                if (!key_matches(c))
                        return NULL;
        }else{
                uint32_t i = db->num_keys;
                while (i--){
                        ddb_fetch_item(i, db->toc, db->data,
                                &c->ent, &c->cursor.value);
                        if (key_matches(c))
                                goto found;
                }
                return NULL;
        }
found:
        c->db = db;
        c->num_items = c->cursor.value.num_left;
        c->next = value_cursor_next;
        return c;
}

uint32_t ddb_resultset_size(const struct ddb_cursor *c)
{
        return c->num_items;        
}

const struct ddb_entry *ddb_next(struct ddb_cursor *c)
{
        return c->next(c);
}

void ddb_fetch_item(uint32_t i, const uint64_t *toc, const char *data,
        struct ddb_entry *key, struct ddb_value_cursor *val)
{
        const char *p = &data[toc[i]];

        key->length = *(uint32_t*)p;
        key->data = &p[4]; 

        val->num_left = *(uint32_t*)&p[4 + key->length];
        val->deltas = &p[8 + key->length];
        val->bits = read_bits(val->deltas, 0, 5);
        val->offset = 5;
        val->cur_id = 0;
}

void ddb_fetch_nextval(struct ddb_entry *e, 
        const struct ddb *db, struct ddb_value_cursor *c)
{
        uint32_t v = read_bits(c->deltas, c->offset, c->bits);
        c->cur_id += v;
        c->num_left--;
        c->offset += c->bits;
        
        e->length = db->values_toc[c->cur_id] - db->values_toc[c->cur_id - 1];
        e->data = &db->values[db->values_toc[c->cur_id - 1]];
}
