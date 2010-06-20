
#ifndef __DDB_LIST_H__
#define __DDB_LIST_H__

struct ddb_list;

struct ddb_list *ddb_list_new();
int ddb_list_append(struct ddb_list *list, uint32_t e);
uint32_t *ddb_list_pointer(const struct ddb_list *list, uint32_t *length);

#endif /* __DDB_LIST_H__ */
