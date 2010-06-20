
#include <string.h>

#include <ddb_internal.h>
#include <ddb_hash.h>

#include <ddb_map.h>

/*
 * ddb_map maps values to value IDs.
 *
 * A 24-bit hash is generated for a value, which is mapped to a 3-level 256-ary
 * trie. The first two levels of the trie are compressed in a single array.
 * New leaves are generated as needed. */

#define IDX1(x) (x & 0x0000ffff)
#define IDX2(x) ((x & 0x00ff0000) >> 16)

struct leaf{
    struct ddb_map_item *items;
    uint32_t size;
};

struct ddb_map{
    struct leaf **leaves;
    uint32_t num_items;
    uint32_t max_num_items;
};

struct ddb_map_item{
    union{
        uint32_t int_key;
        struct ddb_entry str_key;
    };
    ddb_value_t value;
};

static ddb_value_t *new_item(struct leaf *leaf,
    const struct ddb_entry *str_key,
    unsigned long int_key)
{
    struct ddb_map_item *orig = leaf->items;
    leaf->items = realloc(leaf->items,
        ++leaf->size * sizeof(struct ddb_map_item));
    if (orig == leaf->items) /* realloc failed */
        return NULL;


    struct ddb_map_item *item = &leaf->items[leaf->size - 1];
    if (str_key)
        item->str_key = *str_key;
    else
        item->int_key = int_key;
    item->value = 0;
    return &item->value;
}

static ddb_value_t *lookup_leaf(
    struct ddb_map *map,
    uint32_t hash,
    const struct ddb_entry *str_key,
    unsigned long int_key)
{
    uint32_t idx = IDX1(hash);
    if (!map->leaves[idx])
        if (!(map->leaves[idx] = calloc(256, sizeof(struct leaf))))
            return NULL;

    struct leaf *leaf = &map->leaves[idx][IDX2(hash)];
    uint32_t i = leaf->size;
    if (str_key)
        while (i--){
            struct ddb_map_item *e = &leaf->items[i];
            if (e->str_key.length == str_key->length &&
                memcmp(e->str_key.data, str_key->data, str_key->length) == 0)
                return &e->value;
        }
    else
        while (i--)
            if (leaf->items[i].int_key == int_key)
                return &leaf->items[i].value;

    /* No match: Create a new entry for the value */
    if (map->num_items < map->max_num_items){
        ++map->num_items;
        return new_item(leaf, str_key, int_key);
    }else
        return NULL;
}

ddb_value_t *ddb_map_lookup_int(struct ddb_map *map, unsigned long key)
{
    uint32_t hash = SuperFastHash((const char*)&key, 4);
    return lookup_leaf(map, hash, NULL, key);
}

ddb_value_t *ddb_map_lookup_str(struct ddb_map *map,
    const struct ddb_entry *str_key)
{
    uint32_t hash = SuperFastHash(str_key->data, str_key->length);
    return lookup_leaf(map, hash, str_key, 0);
}

struct ddb_map *ddb_map_new(uint32_t max_num_items)
{
    struct ddb_map *map;
    if (!(map = malloc(sizeof(struct ddb_map))))
        return NULL;
    if (!(map->leaves = calloc(65536, sizeof(struct leaf**)))){
        free(map);
        return NULL;
    }
    map->num_items = 0;
    map->max_num_items = max_num_items;
    return map;
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
    free(map->leaves);
    free(map);
}

