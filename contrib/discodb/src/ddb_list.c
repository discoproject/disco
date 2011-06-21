
#include <stdlib.h>
#include <stdint.h>

#include <ddb_list.h>

struct ddb_list{
    uint32_t size;
    uint32_t i;
    uint64_t list[0];
};

struct ddb_list *ddb_list_new()
{
    struct ddb_list *list;
    if (!(list = malloc(sizeof(struct ddb_list) + 8)))
        return NULL;
    list->size = 1;
    list->i = 0;
    return list;
}

void ddb_list_free(struct ddb_list *list)
{
    free(list);
}

struct ddb_list *ddb_list_append(struct ddb_list *list, uint64_t e)
{
    if (list->i == list->size){
        list->size *= 2;
        if (!(list = realloc(list, sizeof(struct ddb_list) + list->size * 8)))
            return NULL;
    }
    list->list[list->i++] = e;
    return list;
}

uint64_t *ddb_list_pointer(const struct ddb_list *list, uint32_t *length)
{
    *length = list->i;
    return (uint64_t*)list->list;
}

void ddb_list_mem_usage(const struct ddb_list *list,
                        uint64_t *alloc,
                        uint64_t *used)
{
    *alloc = list->size * 8 + sizeof(struct ddb_list);
    *used = list->i * 8 + sizeof(struct ddb_list);
}
