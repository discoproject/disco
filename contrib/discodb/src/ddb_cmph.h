
#ifndef __DDB_CMPH_H__
#define __DDB_CMPH_H__

#include <stdint.h>
#include <ddb_map.h>

char *ddb_build_cmph(const struct ddb_map *keys_map, uint32_t *size);

#endif /* __DDB_CMPH_H__ */
