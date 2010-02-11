
#ifndef __DISCO_H__
#define __DISCO_H__

#include <Judy.h>

#define STDIN_READ(buf, len)\
    if (!fread(buf, len, 1, stdin))\
        die("Couldn't read %u bytes from stdin", len);

#define STDOUT_WRITE(buf, len)\
    if (!fwrite(buf, len, 1, stdout))\
        die("Couldn't write %u bytes to stdout", len);

typedef struct{
    unsigned int len;
    unsigned int sze;
    char data[0];
} p_entry;

void msg(const char *fmt, ...);
void die(const char *fmt, ...);
void *dxmalloc(unsigned int size);

void copy_entry(p_entry **dst, const p_entry *src);
int read_kv(p_entry **key, p_entry **val);
void write_entry(const p_entry *e);
void write_kv(const p_entry *key, const p_entry *val);
void write_num_prefix(int num);
Pvoid_t read_parameters();

#endif
