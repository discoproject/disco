
#include <stdlib.h>
#include <stdint.h>

#include <ddb_list.h>

struct ddb_list{
    uint32_t size;
    uint32_t i;
    uint32_t list[0];
};

struct ddb_list *ddb_list_new()
{
    struct ddb_list *list;
    if (!(list = malloc(sizeof(struct ddb_list) + 4)))
        return NULL;
    list->size = 1;
    list->i = 0;
    return list;
}

int ddb_list_append(struct ddb_list *list, uint32_t e)
{
    if (list->i == list->size){
        struct ddb_list *new =
            realloc(list, sizeof(struct ddb_list) + list->size * 8);
        if (new == list)
            return -1;
        list->size *= 2;
        list = new;
    }
    list->list[list->i++] = e;
    return 0;
}

uint32_t *ddb_list_pointer(const struct ddb_list *list, uint32_t *length)
{
    *length = list->i;
    return (uint32_t*)list->list;
}


