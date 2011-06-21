
#ifndef __DDB_MAP_H__
#define __DDB_MAP_H__

#include <stdint.h>

#include <ddb_internal.h>

struct ddb_map;
struct ddb_map_cursor;

struct ddb_map_stat{
    uint64_t num_leaves;
    uint32_t num_keys;
    uint64_t num_items;
    uint64_t leaves_alloc;
    uint64_t leaves_used;
    uint64_t keys_alloc;
    uint64_t keys_used;
    uint64_t membuf_alloc;
    uint64_t membuf_used;
};

struct ddb_map *ddb_map_new(uint32_t max_num_items);

void ddb_map_free(struct ddb_map *map);

uint32_t ddb_map_num_items(const struct ddb_map *map);

uint64_t *ddb_map_insert_int(struct ddb_map *map, uint32_t key);

uint64_t *ddb_map_insert_str(struct ddb_map *map,
                             const struct ddb_entry *key);

uint64_t *ddb_map_lookup_int(const struct ddb_map *map, uint32_t key);

uint64_t *ddb_map_lookup_str(const struct ddb_map *map,
                             const struct ddb_entry *key);


struct ddb_map_cursor *ddb_map_cursor_new(const struct ddb_map *map);

int ddb_map_next_int(struct ddb_map_cursor *c, uint32_t *key);

int ddb_map_next_str(struct ddb_map_cursor *c, struct ddb_entry *key);

int ddb_map_next_item_int(struct ddb_map_cursor *c,
                          uint32_t *key,
                          uint64_t **ptr);

int ddb_map_next_item_str(struct ddb_map_cursor *c,
                          struct ddb_entry *key,
                          uint64_t **ptr);

void ddb_map_cursor_free(struct ddb_map_cursor *c);

void ddb_map_mem_usage(const struct ddb_map *map, struct ddb_map_stat *stats);

#endif /* __DDB_MAP_H__ */
