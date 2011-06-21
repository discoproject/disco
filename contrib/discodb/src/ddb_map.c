
#include <string.h>

#include <ddb_internal.h>
#include <ddb_hash.h>

#include <ddb_list.h>
#include <ddb_membuffer.h>
#include <ddb_map.h>

/*
 * ddb_map maps values to value IDs.
 *
 * A 24-bit hash is generated for a value, which is mapped to a 3-level 256-ary
 * trie. The first two levels of the trie are compressed in a single array.
 * New leaves are generated as needed. */

#define IDX1(x) (x & 0x0000ffff)
#define IDX2(x) ((x & 0x00ff0000) >> 16)
#define LEAF_SIZE_INCREMENT 2

struct leaf{
    struct ddb_map_item *items;
    uint32_t num_items;
    uint32_t size;
};

struct ddb_map{
    struct leaf **leaves;
    uint32_t num_items;
    uint32_t max_num_items;

    struct ddb_membuffer *key_buffer;
    struct ddb_list *keys;
};

struct ddb_map_item{
    uint64_t key;
    uint64_t value;
};

struct ddb_map_cursor{
    const struct ddb_map *map;
    uint64_t *keys;
    uint32_t length;
    uint32_t i;
};

static uint64_t *new_item(
    struct ddb_map *map,
    struct leaf *leaf,
    const struct ddb_entry *str_key,
    uint32_t int_key)
{
    if (map->num_items < map->max_num_items)
        ++map->num_items;
    else
        return NULL;

    if (++leaf->num_items > leaf->size){
        leaf->size += LEAF_SIZE_INCREMENT;
        if (!(leaf->items = realloc(leaf->items,
                leaf->size * sizeof(struct ddb_map_item))))
            return NULL;
    }

    struct ddb_map_item *item = &leaf->items[leaf->num_items - 1];
    if (str_key)
        item->key = (uint64_t)ddb_membuffer_copy_ns(
            map->key_buffer, str_key->data, str_key->length);
    else
        item->key = int_key;

    if (!(map->keys = ddb_list_append(map->keys, item->key)))
        return NULL;

    item->value = 0;
    return &item->value;
}

static uint64_t *lookup_leaf(
    const struct ddb_map *map,
    uint32_t hash,
    const struct ddb_entry *str_key,
    uint32_t int_key,
    struct leaf **leafout)
{
    uint32_t idx = IDX1(hash);
    if (!map->leaves[idx])
        if (!(map->leaves[idx] = calloc(256, sizeof(struct leaf))))
            return NULL;

    struct leaf *leaf = &map->leaves[idx][IDX2(hash)];
    uint32_t i = leaf->num_items;

    if (leafout)
        *leafout = leaf;

    if (str_key)
        while (i--){
            const struct ddb_netstring *e =
                (const struct ddb_netstring*)leaf->items[i].key;
            if (e->length == str_key->length &&
                    memcmp(e->data, str_key->data, e->length) == 0)
                return &leaf->items[i].value;
        }
    else
        while (i--)
            if (leaf->items[i].key == int_key)
                return &leaf->items[i].value;

    return NULL;
}

uint64_t *ddb_map_lookup_int(const struct ddb_map *map, uint32_t key)
{
    uint32_t hash = SuperFastHash((const char*)&key, 4);
    return lookup_leaf(map, hash, NULL, key, NULL);
}

uint64_t *ddb_map_lookup_str(const struct ddb_map *map,
                             const struct ddb_entry *str_key)
{
    uint32_t hash = SuperFastHash(str_key->data, str_key->length);
    return lookup_leaf(map, hash, str_key, 0, NULL);
}

uint64_t *ddb_map_insert_int(struct ddb_map *map, uint32_t key)
{
    struct leaf *leaf = NULL;
    uint32_t hash = SuperFastHash((const char*)&key, 4);
    uint64_t *v = lookup_leaf(map, hash, NULL, key, &leaf);
    if (!v)
        v = new_item(map, leaf, NULL, key);
    return v;
}

uint64_t *ddb_map_insert_str(struct ddb_map *map,
                                const struct ddb_entry *str_key)
{
    struct leaf *leaf = NULL;
    uint32_t hash = SuperFastHash(str_key->data, str_key->length);
    uint64_t *v = lookup_leaf(map, hash, str_key, 0, &leaf);
    if (!v)
        v = new_item(map, leaf, str_key, 0);
    return v;
}

struct ddb_map *ddb_map_new(uint32_t max_num_items)
{
    struct ddb_map *map;
    if (!(map = calloc(1, sizeof(struct ddb_map))))
        return NULL;
    if (!(map->leaves = calloc(65536, sizeof(struct leaf**))))
        goto err;
    if (!(map->key_buffer = ddb_membuffer_new()))
        goto err;
    if (!(map->keys = ddb_list_new()))
        goto err;
    map->max_num_items = max_num_items;
    return map;
err:
    ddb_map_free(map);
    return NULL;
}

struct ddb_map_cursor *ddb_map_cursor_new(const struct ddb_map *map)
{
    struct ddb_map_cursor *c;
    if (!(c = malloc(sizeof(struct ddb_map_cursor))))
        return NULL;

    c->i = 0;
    c->keys = ddb_list_pointer(map->keys, &c->length);
    c->map = map;
    return c;
}

void ddb_map_cursor_free(struct ddb_map_cursor *c)
{
    free(c);
}

int ddb_map_next_int(struct ddb_map_cursor *c, uint32_t *key)
{
    if (c->i == c->length)
        return 0;
    *key = c->keys[c->i++];
    return 1;
}

int ddb_map_next_str(struct ddb_map_cursor *c, struct ddb_entry *key)
{
    if (c->i == c->length)
        return 0;
    struct ddb_netstring *e = (struct ddb_netstring*)c->keys[c->i++];
    key->length = e->length;
    key->data = e->data;
    return 1;
}

int ddb_map_next_item_str(struct ddb_map_cursor *c,
                          struct ddb_entry *key,
                          uint64_t **ptr)
{
    if (ddb_map_next_str(c, key)){
        *ptr = ddb_map_lookup_str(c->map, key);
        return 1;
    }else
        return 0;
}

int ddb_map_next_item_int(struct ddb_map_cursor *c,
                          uint32_t *key,
                          uint64_t **ptr)
{
    if (ddb_map_next_int(c, key)){
        *ptr = ddb_map_lookup_int(c->map, *key);
        return 1;
    }else
        return 0;
}

uint32_t ddb_map_num_items(const struct ddb_map *map)
{
    return map->num_items;
}

void ddb_map_free(struct ddb_map *map)
{
    if (!map)
        return;
    int i = 65536;
    while (i--)
        if (map->leaves[i]){
            int j = 256;
            while (j--)
                free(map->leaves[i][j].items);
            free(map->leaves[i]);
        }
    ddb_membuffer_free(map->key_buffer);
    ddb_list_free(map->keys);
    free(map->leaves);
    free(map);
}

void ddb_map_mem_usage(const struct ddb_map *map, struct ddb_map_stat *stats)
{
    int i = 65536;
    memset(stats, 0, sizeof(struct ddb_map_stat));
    stats->leaves_alloc = stats->leaves_used = sizeof(struct ddb_map)
                                             + i * sizeof(struct leaf*);
    while (i--)
        if (map->leaves[i]){
            int j = 256;
            while (j--){
                struct leaf *f = &map->leaves[i][j];
                if (f->items){
                    ++stats->num_leaves;
                    stats->leaves_alloc +=
                        f->size * sizeof(struct ddb_map_item);
                    stats->leaves_used +=
                        f->num_items * sizeof(struct ddb_map_item);
                    stats->num_items += f->num_items;
                }
            }
            stats->leaves_alloc += 256 * sizeof(struct leaf);
            stats->leaves_used += 256 * sizeof(struct leaf);

        }
    ddb_list_pointer(map->keys, &stats->num_keys);
    ddb_list_mem_usage(map->keys, &stats->keys_alloc, &stats->keys_used);
    ddb_membuffer_mem_usage(map->key_buffer,
                            &stats->membuf_alloc,
                            &stats->membuf_used);
}
