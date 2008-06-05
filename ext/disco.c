
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include "disco.h"

#define STDIN_READ(buf, len)\
        if (!fread(buf, len, 1, stdin))\
                die("Couldn't read %u bytes from stdin", len);

#define STDOUT_WRITE(buf, len)\
        if (!fwrite(buf, len, 1, stdout))\
                die("Couldn't write %u bytes to stdout", len);


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

void *xmalloc(unsigned int size)
{
       void *p = malloc(size);
       if (!p)
               die("Memory allocation failed (%u bytes)", size);
       return p;
}

static p_entry *read_pentry(unsigned int len)
{
        p_entry *e = xmalloc(sizeof(p_entry) + len + 1);
        if (len){
                STDIN_READ(e->data, len);
        }
        e->len = len;
        e->data[len] = 0;
        return e; 
}

int read_kv(p_entry **key, p_entry **val)
{
        unsigned int len;
        if (!fread(&len, 4, 1, stdin))
                return 0;
        *key = read_pentry(len);
        STDIN_READ(&len, 4);
        *val = read_pentry(len);
        return 1;
}

void write_num_prefix(int num)
{
        STDOUT_WRITE(&num, 4);
        fflush(stdout);
}

void write_kv(const p_entry *key, const p_entry *val)
{
        STDOUT_WRITE(&key->len, 4);
        if (key->len){
                STDOUT_WRITE(key->data, key->len);
        }
        STDOUT_WRITE(&val->len, 4);
        if (val->len){
                STDOUT_WRITE(val->data, val->len);
        }
        fflush(stdout);
}

static p_entry *read_netstr_entry(unsigned int *bytes)
{
        unsigned int len, n, tmp;
        if (!fscanf(stdin, "%u %n", &len, &n))
                die("Couldn't parse item length");
        p_entry *e = read_pentry(len);
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
        if (!fscanf(stdin, "%u\n", &len))
                die("Couldn't parse parameter set size");
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

