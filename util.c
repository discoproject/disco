
#include <string.h>

#include <discodb.h>

uint32_t read_bits(const uint8_t *src, uint64_t offs, uint32_t bits)
{
        uint64_t *src_w = (uint64_t*)&src[offs >> 3];
        return (*src_w >> (offs & 7)) & ((1 << bits) - 1);
}

void write_bits(uint8_t *dst, uint64_t offs, uint32_t val)
{
        uint64_t *dst_w = (uint64_t*)&dst[offs >> 3];
        *dst_w |= val << (offs & 7);
}


int copy_to_buf(discodb_t *db, const void *src, uint32_t len)
{
        if (len > db->tmpbuf_len){
                db->tmpbuf_len = len;
                if (!(db->tmpbuf = realloc(db->tmpbuf, len)))
                        return -1;
        }
        memcpy(db->tmpbuf, src, len);
        return 0;
}

uint32_t allocate_bits(discodb_t *db, uint32_t size_in_bits)
{
        uint32_t len = size_in_bits >> 3;
        if (!len)
                len = 1;
        if (len > db->tmpbuf_len){
                db->tmpbuf_len = len;
                if (!(db->tmpbuf = realloc(db->tmpbuf, len)))
                        return 0;
        }
        memset(db->tmpbuf, 0, len);
        return len;
}

uint32_t bits_needed(uint32_t max)
{
        uint32_t x = max;
        uint32_t bits = x ? 0: 1;
        while (x){
                x >>= 1;
                ++bits;
        }
        return bits;
}

int id_cmp(const void *p1, const void *p2)
{
        if (((const ddb_attr_t*)p1)->id > ((const ddb_attr_t*)p2)->id)
                return 1;
        else if (((const ddb_attr_t*)p1)->id < ((const ddb_attr_t*)p2)->id)
                return -1;
        return 0;
}
