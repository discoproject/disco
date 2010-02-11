
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#include "disco.h"

void die(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "**<ERR>");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    exit(1);
}

void msg(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "**<MSG>");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

void *dxmalloc(unsigned int size)
{
    void *p = malloc(size);
    if (!p)
        die("Memory allocation failed (%u bytes)", size);
    return p;
}

void copy_entry(p_entry **dst, const p_entry *src)
{
    if (!*dst || (*dst)->sze < src->len){
        free(*dst);
        *dst = dxmalloc(sizeof(p_entry) + src->len + 1);
        (*dst)->sze = src->len;
    }
    (*dst)->len = src->len;
    memcpy((*dst)->data, src->data, src->len + 1); 
}

static void read_pentry(p_entry **e, unsigned int len)
{
    if (!*e || (*e)->sze < len){
        free(*e);
        *e = dxmalloc(sizeof(p_entry) + len + 1);
        (*e)->sze = len;
    }
    if (len){
        STDIN_READ((*e)->data, len);
    }
    (*e)->len = len;
    (*e)->data[len] = 0;
}

int read_kv(p_entry **key, p_entry **val)
{
    unsigned int len;
    if (!fread(&len, 4, 1, stdin))
        return 0;
    read_pentry(key, len);
    STDIN_READ(&len, 4);
    read_pentry(val, len);
    return 1;
}

void write_num_prefix(int num)
{
    STDOUT_WRITE(&num, 4);
    fflush(stdout);
}

void write_entry(const p_entry *e)
{
    STDOUT_WRITE(&e->len, 4);
    if (e->len){
        STDOUT_WRITE(e->data, e->len);
    }
}

void write_kv(const p_entry *key, const p_entry *val)
{
    write_entry(key);
    write_entry(val);
    fflush(stdout);
}

static p_entry *read_netstr_entry(unsigned int *bytes)
{
    unsigned int len, n, tmp;
    if (!fscanf(stdin, "%u %n", &len, &n))
        die("Couldn't parse item length");
    p_entry *e = NULL;
    read_pentry(&e, len);
    if (len){
        STDIN_READ(&tmp, 1);
    }else
        --n;
    *bytes += len + n + 1;
    return e;
}      

Pvoid_t read_parameters()
{
    Pvoid_t params = NULL;
    unsigned int len, bytes = 0;
    unsigned char tmp;
 
    if (!fscanf(stdin, "%u", &len))
        die("Couldn't parse parameter set size");
    /* Read a newline after the size spec. Earlier I did 
     * fscanf(stdin, "%u\n", &len)
     * on the line above, but this was a stupid idea. Fscanf interpretes
     * *any* whitespace character as a sign to read *any number* of
     * following whitespace characters, which obviously caused great havoc
     * here.
     * */
    fread(&tmp, 1, 1, stdin);
    while (bytes < len){
        p_entry *key = read_netstr_entry(&bytes);      
        p_entry *val = read_netstr_entry(&bytes);
        Word_t *ptr;
        JSLI(ptr, params, (unsigned char*)key->data);
        *ptr = (Word_t)val;
        free(key); 
    }
    if (bytes > len)
        die("Invalid parameter set size");
    return params; 
}

