#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#include <ddb_list.h>
#include <ddb_internal.h>

#include <ddb_bits.h>
#include <ddb_delta.h>

#define BUF_INCREMENT 1048576

static uint32_t bits_needed(uint32_t max)
{
    uint32_t x = max;
    uint32_t bits = x ? 0: 1;
    while (x){
        x >>= 1;
        ++bits;
    }
    return bits;
}

static uint32_t allocate_bits(char **buf, uint64_t *buf_size,
                              uint32_t size_in_bits)
{
    uint32_t len = size_in_bits >> 3;
    if (size_in_bits & 7)
        len += 1;
    /* + 8 is for write_bits and read_bits which may try to access
     * at most 7 bytes out of array bounds */
    if (len + 8 > *buf_size){
        while (len + 8 > *buf_size)
            *buf_size += BUF_INCREMENT;
        free(*buf);
        *buf = NULL;
        if (!(*buf = malloc(*buf_size)))
            return 0;
    }
    memset(*buf, 0, len + 8);
    return len;
}

static int id_cmp(const void *p1, const void *p2)
{
    const valueid_t x = *(const valueid_t*)p1;
    const valueid_t y = *(const valueid_t*)p2;

    if (x > y)
        return 1;
    else if (x < y)
        return -1;
    return 0;
}

void ddb_delta_cursor_next(struct ddb_delta_cursor *c)
{
    if (c->num_left){
        uint32_t v = read_bits(c->deltas, c->offset, c->bits);
        c->cur_id += v;
        c->offset += c->bits;
        c->num_left--;
    }
}

void ddb_delta_cursor(struct ddb_delta_cursor *c, const char *src)
{
    c->num_left = *(uint32_t*)src;
    c->cur_id = 0;

    if (c->num_left){
        c->deltas = &src[4];
        c->bits = read_bits(c->deltas, 0, 5) + 1;
        c->offset = 5;
    }
}

int ddb_delta_encode(valueid_t *values,
                     uint32_t num_values,
                     char **buf,
                     uint64_t *buf_size,
                     uint64_t *size,
                     uint32_t *num_written,
                     int *duplicates,
                     int unique_values)
{
    uint32_t i, j = 0, bits = 0, prev = 0, max_diff = 0;
    uint64_t offs = 0;
    *duplicates = 0;

    if (num_values){
        qsort(values, num_values, sizeof(valueid_t), id_cmp);

        /* find maximum delta -> bits needed per id */
        max_diff = values[0];
        for (i = 1; i < num_values; i++){
            uint32_t d = values[i] - values[i - 1];
            if (d > max_diff)
                max_diff = d;
        }
        bits = bits_needed(max_diff);
        if (!(allocate_bits(buf, buf_size, 32 + 5 + bits * num_values)))
            return -1;
    }else{
        if (!(allocate_bits(buf, buf_size, 32)))
            return -1;
    }

    /* values field:
       [ num_vals (32 bits) | bits_needed (5 bits) |
         delta-encoded values (bits * num_vals) ]
    */
    offs = 32;
    if (num_values){
        write_bits(*buf, offs, bits - 1);
        offs += 5;
        for (i = 0; i < num_values; i++){
            uint32_t d = values[i] - prev;
            if (!d && i){
                if (unique_values)
                    continue;
                else
                    *duplicates = 1;
            }
            write_bits(*buf, offs, d);
            prev = values[i];
            offs += bits;
            ++j;
        }
    }
    *num_written = j;
    memcpy(*buf, &j, 4);
    *size = (offs >> 3) + ((offs & 7) ? 1: 0);
    return 0;
}
