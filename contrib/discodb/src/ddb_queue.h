
#ifndef __DDB_QUEUE__
#define __DDB_QUEUE__

#include <stdint.h>

struct ddb_queue;

struct ddb_queue *ddb_queue_new(uint32_t max_length);
void ddb_queue_free(struct ddb_queue *q);
void ddb_queue_push(struct ddb_queue *q, void *e);
void *ddb_queue_pop(struct ddb_queue *q);
void *ddb_queue_peek(const struct ddb_queue *q);
int ddb_queue_length(const struct ddb_queue *q);

#endif /* __DDB_QUEUE__ */
