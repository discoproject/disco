
#ifndef __DDB_LIST_H__
#define __DDB_LIST_H__

struct ddb_list;

struct ddb_list *ddb_list_new(void);
void ddb_list_free(struct ddb_list *list);
struct ddb_list *ddb_list_append(struct ddb_list *list, uint64_t e);
uint64_t *ddb_list_pointer(const struct ddb_list *list, uint32_t *length);
void ddb_list_mem_usage(const struct ddb_list *list,
                        uint64_t *alloc,
                        uint64_t *used);

#endif /* __DDB_LIST_H__ */
