
#ifndef __DISCO_H__
#define __DISCO_H__

#include <Judy.h>

typedef struct{
        unsigned int len;
        char data[0];
} p_entry;

void msg(const char *fmt, ...);
void die(const char *fmt, ...);
void *xmalloc(unsigned int size);

int read_kv(p_entry **key, p_entry **val);
void write_kv(const p_entry *key, const p_entry *val);
void write_num_prefix(int num);
Pvoid_t read_parameters();

#endif
