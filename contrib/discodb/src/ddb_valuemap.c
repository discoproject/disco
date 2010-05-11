
#include <ddb_internal.h>
#include <ddb_hash.h>

/*
 * Valuemap maps values to value IDs.
 *
 * A 24-bit hash is generated for a value, which is mapped to a 3-level 256-ary
 * trie. The first two levels of the trie are compressed in a single array.
 * New leaves are generated as needed. */

#define IDX1(x) (x & 0x0000ffff)
#define IDX2(x) ((x & 0x00ff0000) >> 16)

struct valitem{
        valueid_t *ids;        
        uint32_t size;
};

void *ddb_valuemap_init()
{
        return calloc(65536, sizeof(void*));
}

void ddb_valuemap_free(void *valmap)
{
        if (!valmap)
                return;
        struct valitem **map = (struct valitem**)valmap;
        int i = 65536;
        while (i--){
                if (map[i]){
                        int j = 256;
                        while (j--)
                                if (map[i][j].size > 1)
                                        free(map[i][j].ids);
                        free(map[i]);
                }
        }
        free(valmap);
}

static valueid_t *lookup_leaf(struct valitem *leaf, const struct ddb_entry *value,
        int (*eq)(const struct ddb_entry*, valueid_t))
{
        if (!leaf->size){
                /* empty leaf */
                /* as a special case, the first ID is stored in the ids pointer
                 * itself */
                leaf->size = 1;
                return (valueid_t*)&leaf->ids;
        }else if (leaf->size == 1 && eq(value, (valueid_t)(uint64_t)leaf->ids))
                /* handle the above special case: ID is stored in the pointer
                 * and it matches with the value */
                return (valueid_t*)&leaf->ids;
        else if (leaf->size > 1){
                /* normal case, check all the items in the ID chain */
                uint32_t i = leaf->size;
                while (i--)
                        if (eq(value, leaf->ids[i]))
                                return &leaf->ids[i];
        }
        
        /* No match: Create a new entry for the value */

        if (leaf->size++ == 1){
                /* special case: move the first id from the pointer 
                 * to the actual array */
                valueid_t v = (valueid_t)(uint64_t)leaf->ids;
                if (!(leaf->ids = malloc(leaf->size * sizeof(valueid_t))))
                        return NULL;
                leaf->ids[0] = v;
        }else if (!(leaf->ids = realloc(leaf->ids,
                                leaf->size * sizeof(valueid_t))))
                        return NULL;
        
        leaf->ids[leaf->size - 1] = 0;
        return &leaf->ids[leaf->size - 1]; 
}

valueid_t *ddb_valuemap_lookup(void *valmap, const struct ddb_entry *value,
        int (*eq)(const struct ddb_entry*, valueid_t))
{
        struct valitem **map = (struct valitem**)valmap;
        uint32_t hash = SuperFastHash(value->data, value->length);
        uint32_t idx = IDX1(hash);
        
        if (map[idx])
                return lookup_leaf(&map[idx][IDX2(hash)], value, eq);
        else{
                if (!(map[idx] = calloc(256, sizeof(struct valitem))))
                        return NULL;
                return lookup_leaf(&map[idx][IDX2(hash)], value, eq);
        }
}

