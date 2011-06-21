
#ifndef __DDB_MEMBUFFER__
#define __DDB_MEMBUFFER__

struct ddb_netstring{
    uint64_t length;
    char data[0];
};

struct ddb_membuffer;

struct ddb_membuffer *ddb_membuffer_new(void);

void ddb_membuffer_free(struct ddb_membuffer *mb);

char *ddb_membuffer_copy(struct ddb_membuffer *mb,
                         const char *src,
                         uint64_t length);

char *ddb_membuffer_copy_ns(struct ddb_membuffer *mb,
                            const char *src,
                            uint64_t length);

void ddb_membuffer_mem_usage(const struct ddb_membuffer *mb,
                             uint64_t *alloc,
                             uint64_t *used);

#endif /* __DDB_MEMBUFFER__ */
