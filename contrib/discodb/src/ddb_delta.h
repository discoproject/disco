
#ifndef __DDB_DELTA_H__
#define __DDB_DELTA_H__

#include <stdint.h>

#include <ddb_list.h>

struct ddb_delta_cursor{
    const char *deltas;
    uint32_t bits;
    uint32_t num_left;
    uint64_t offset;
    uint64_t cur_id;
};

void ddb_delta_cursor_next(struct ddb_delta_cursor *c);

void ddb_delta_cursor(struct ddb_delta_cursor *c, const char *src);

int ddb_delta_encode(const struct ddb_list *values,
                     char **buf,
                     uint64_t *buf_size,
                     uint64_t *size,
                     int *duplicates);

#endif /* __DDB_DELTA_H__ */

