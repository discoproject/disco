
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <ddb_membuffer.h>

#define DDB_MB_PAGE_SIZE (10 * 1024 * 1024)
#define MAX(a,b) ((a)>(b)?(a):(b))

struct page{
    uint64_t offset;
    uint64_t free;
    struct page *next;
    char buffer[0];
};

struct ddb_membuffer{
    struct page *current;
    struct page *first;
};

static struct page *new_page(struct ddb_membuffer *mb, uint64_t size)
{
    struct page *p;

    size = MAX(size, DDB_MB_PAGE_SIZE);
    if (!(p = malloc(size + sizeof(struct page))))
        return NULL;

    p->offset = 0;
    p->free = size;
    p->next = NULL;

    struct page *n = mb->first;
    if (n){
        while (n->next)
            n = n->next;
        n->next = p;
    }
    return p;
}

static char *copy_entry(struct page *page, const char *src, uint64_t length)
{
    char *dst = &page->buffer[page->offset];
    memcpy(dst, src, length);
    page->offset += length;
    page->free -= length;
    return dst;
}

struct ddb_membuffer *ddb_membuffer_new()
{
    struct ddb_membuffer *mb;
    if (!(mb = calloc(1, sizeof(struct ddb_membuffer))))
        return NULL;
    if (!(mb->first = mb->current = new_page(mb, DDB_MB_PAGE_SIZE))){
        free(mb);
        return NULL;
    }
    return mb;
}

static void set_emptiest(struct ddb_membuffer *mb)
{
    struct page *p = mb->first;
    uint64_t max = 0;
    do{
        if (p->free > max){
            max = p->free;
            mb->current = p;
        }
        p = p->next;
    }while (p);
}

static char *membuffer_copy(struct ddb_membuffer *mb,
                            const char *src,
                            uint64_t length,
                            int prefix_length)
{
    uint64_t len = length + (prefix_length ? 8: 0);
    struct page *p = mb->current;
    int new = 0;
    char *dst;

    if (p->free < len){
        if (!(p = new_page(mb, len)))
            return NULL;
        new = 1;
    }

    if (prefix_length){
        dst = copy_entry(p, (const char*)&length, 8);
        copy_entry(p, src, length);
    }else
        dst = copy_entry(p, src, length);

    if (new)
        set_emptiest(mb);

    return dst;
}

char *ddb_membuffer_copy(struct ddb_membuffer *mb,
                         const char *src,
                         uint64_t length)
{
    return membuffer_copy(mb, src, length, 0);
}

char *ddb_membuffer_copy_ns(struct ddb_membuffer *mb,
                            const char *src,
                            uint64_t length)
{
    return membuffer_copy(mb, src, length, 1);
}

void ddb_membuffer_free(struct ddb_membuffer *mb)
{
    if (mb){
        struct page *p = mb->first;
        while (p){
            struct page *n = p->next;
            free(p);
            p = n;
        }
        free(mb);
    }
}

void ddb_membuffer_mem_usage(const struct ddb_membuffer *mb,
                             uint64_t *alloc,
                             uint64_t *used)
{
    struct page *p = mb->first;
    *alloc = *used = sizeof(struct ddb_membuffer);
    do{
        *alloc += DDB_MB_PAGE_SIZE;
        *used += p->offset;
        p = p->next;
    }while (p);
}

