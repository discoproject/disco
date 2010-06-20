
#ifndef __DDB_MAP_H__
#define __DDB_MAP_H__

struct ddb_map;
typedef unsigned long ddb_value_t;

struct ddb_map *ddb_map_new(uint32_t max_num_items);
void ddb_map_free(struct ddb_map *map);
ddb_value_t *ddb_map_lookup_int(struct ddb_map *map, unsigned long key);
ddb_value_t *ddb_map_lookup_str(
    struct ddb_map *map, const struct ddb_entry *key);

#define DDB_MAP_ITEMS_INT(map, key, val){\
    int __i = 65336;\
    while (__i--){\
        if (map->leaves[__i]){\
            int __j = 256;\
            while (__j--){\
                int __k = map->leaves[__i][__j].size;\
                while (__k--){\
                    key = map->leaves[__i][__j].items[__k].int_key;\
                    val = &map->leaves[__i][__j].items[__k].value;

#define DDB_MAP_ITEMS_INT_END }}}}}

#endif /* __DDB_MAP_H__ */
