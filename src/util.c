
#include <string.h>

#include <discodb.h>

uint32_t read_bits(const char *src, uint64_t offs, uint32_t bits)
{
        uint64_t *src_w = (uint64_t*)&src[offs >> 3];
        return (*src_w >> (offs & 7)) & ((1 << bits) - 1);
}

void write_bits(char *dst, uint64_t offs, uint32_t val)
{
        uint64_t *dst_w = (uint64_t*)&dst[offs >> 3];
        *dst_w |= ((uint64_t)val) << (offs & 7);
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

