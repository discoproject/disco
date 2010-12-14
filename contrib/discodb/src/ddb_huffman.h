
#ifndef __DDB_HUFFMAN__
#define __DDB_HUFFMAN__

#include <stdint.h>

#define DDB_CODEBOOK_SIZE 65536
#define DDB_HUFF_CODE(x) ((x) & 65535)
#define DDB_HUFF_BITS(x) (((x) & (65535 << 16)) >> 16)

struct ddb_codebook{
    uint32_t symbol;
    uint32_t bits;
} __attribute__((packed));

struct ddb_map *ddb_create_codemap(const struct ddb_map *keys);

int ddb_save_codemap(
    struct ddb_map *codemap,
    struct ddb_codebook book[DDB_CODEBOOK_SIZE]);

int ddb_compress(
    const struct ddb_map *codemap,
    const char *src,
    uint32_t src_len,
    uint32_t *size,
    char **buf,
    uint64_t *buf_len);

int ddb_decompress(
    const struct ddb_codebook book[DDB_CODEBOOK_SIZE],
    const char *src,
    uint32_t src_len,
    uint32_t *size,
    char **buf,
    uint64_t *buf_len);

#endif /* __DDB_HUFFMAN__ */
