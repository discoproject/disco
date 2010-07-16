
#include <string.h>
#include <stdint.h>

#include <cmph.h>

#include <ddb_map.h>
#include <ddb_cmph.h>

char *ddb_build_cmph(const struct ddb_map *keys_map, uint32_t *size)
{
    char *buf = NULL;
    uint32_t buf_len = 0;
    uint32_t hash_failed = 0;
    struct ddb_entry key;
    struct ddb_map_cursor *c = ddb_map_cursor_new(keys_map);

    void xdispose(void *data, char *key, cmph_uint32 l) { }
    void xrewind(void *data){
        ddb_map_cursor_free(c);
        c = ddb_map_cursor_new(keys_map);
    }
    int xread(void *data, char **p, cmph_uint32 *len)
    {
        if (c){
            ddb_map_next_str(c, &key);
            if (key.length > buf_len){
                buf_len = key.length;
                if (!(buf = realloc(buf, buf_len)))
                    hash_failed = 1;
            }
        }else
            hash_failed = 1;

        if (hash_failed){
            *len = 0;
            *p = NULL;
        }else{
            memcpy(buf, key.data, key.length);
            *len = key.length;
            *p = buf;
        }
        return *len;
    }

    cmph_io_adapter_t r;
    r.data = NULL;
    r.nkeys = ddb_map_num_items(keys_map);
    r.read = xread;
    r.dispose = xdispose;
    r.rewind = xrewind;

    cmph_config_t *cmph = cmph_config_new(&r);
    cmph_config_set_algo(cmph, CMPH_CHD);

    if (getenv("DDB_DEBUG_CMPH"))
        cmph_config_set_verbosity(cmph, 5);

    char *hash = NULL;
    cmph_t *g = cmph_new(cmph);
    *size = 0;
    if (g && !hash_failed){
        *size = cmph_packed_size(g);
        if ((hash = malloc(*size)))
            cmph_pack(g, hash);
    }
    if (g)
        cmph_destroy(g);
    ddb_map_cursor_free(c);
    cmph_config_destroy(cmph);
    free(buf);
    return hash;
}
