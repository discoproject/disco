
#ifndef __DDB_DELTALIST_H__
#define __DDB_DELTALIST_H__

#include <ddb_types.h>

struct ddb_deltalist;

struct ddb_deltalist *ddb_deltalist_new(void);

void ddb_deltalist_free(struct ddb_deltalist *d);

int ddb_deltalist_append(struct ddb_deltalist *d, valueid_t e);

int ddb_deltalist_to_array(const struct ddb_deltalist *d,
                           uint64_t *num_values,
                           valueid_t **values,
                           uint64_t *values_size);

void ddb_deltalist_mem_usage(const struct ddb_deltalist *d,
                             uint64_t *segments,
                             uint64_t *alloc,
                             uint64_t *used);

#endif /* __DDB_LIST_H__ */
