
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#include <ddb_profile.h>
#include <ddb_internal.h>
#include <ddb_queue.h>
#include <ddb_huffman.h>
#include <ddb_map.h>
#include <ddb_bits.h>

#define MIN(a,b) ((a)>(b)?(b):(a))
#define MAX_CANDIDATES 16777216

struct hnode{
    uint32_t code;
    uint32_t num_bits;
    uint32_t symbol;
    uint64_t weight;
    struct hnode *left;
    struct hnode *right;
};

struct sortpair{
    uint32_t freq;
    uint32_t symbol;
};


static void allocate_codewords(struct hnode *node, uint32_t code, int depth)
{
    if (node == NULL)
        return;
    if (depth < 16 && (node->right || node->left)){
        allocate_codewords(node->left, code, depth + 1);
        allocate_codewords(node->right, code | (1 << depth), depth + 1);
    }else{
        node->code = code;
        node->num_bits = depth;
    }
}

static struct hnode *pop_min_weight(struct hnode *symbols,
        int *num_symbols, struct ddb_queue *nodes)
{
    const struct hnode *n = (const struct hnode*)ddb_queue_peek(nodes);
    if (!*num_symbols || (n && n->weight < symbols[*num_symbols - 1].weight))
        return ddb_queue_pop(nodes);
    else if (*num_symbols)
        return &symbols[--*num_symbols];
    return NULL;
}

static int huffman_code(struct hnode *symbols, int num)
{
    struct ddb_queue *nodes = NULL;
    struct hnode *newnodes = NULL;
    int new_i = 0;

    if (!num)
        return 0;
    if (!(nodes = ddb_queue_new(num * 2)))
        return -1;
    if (!(newnodes = malloc(num * sizeof(struct hnode)))){
        ddb_queue_free(nodes);
        return -1;
    }

    while (num || ddb_queue_length(nodes) > 1){
        struct hnode *new = &newnodes[new_i++];
        new->left = pop_min_weight(symbols, &num, nodes);
        new->right = pop_min_weight(symbols, &num, nodes);
        new->weight = (new->left ? new->left->weight: 0) +
                      (new->right ? new->right->weight: 0);
        ddb_queue_push(nodes, new);
    }
    allocate_codewords(ddb_queue_pop(nodes), 0, 0);
    free(newnodes);
    ddb_queue_free(nodes);
    return 0;
}

static void print_symbol(uint32_t symbol)
{
    int j;
    for (j = 0; j < 4; j++)
        fprintf(stderr, "%c", ((char*)&symbol)[j]);
}

static void print_codeword(const struct hnode *node)
{
    int j;
    for (j = 0; j < node->num_bits; j++)
        fprintf(stderr, "%d", (node->code & (1 << j) ? 1: 0));
}

static void output_stats(const struct hnode *book,
    uint32_t num_symbols, uint64_t tot)
{
    fprintf(stderr, "#codewords: %u\n", num_symbols);
    uint64_t cum = 0;
    uint32_t i;
    for (i = 0; i < num_symbols; i++){
        uint32_t f = book[i].weight;
        cum += f;
        print_symbol(book[i].symbol);
        fprintf(stderr, " %u %2.3f %2.3f | ", f, 100. * f / tot, 100. * cum / tot);
        print_codeword(&book[i]);
        fprintf(stderr, "\n");
    }
}

static struct ddb_map *collect_frequencies(const struct ddb_map *keys)
{
    struct ddb_map *freqs = NULL;
    struct ddb_map_cursor *c = NULL;
    struct ddb_entry key;
    uint64_t *ptr;
    uint32_t i;
    int err = -1;

    if (!(freqs = ddb_map_new(MAX_CANDIDATES)))
        goto err;
    if (!(c = ddb_map_cursor_new(keys)))
        goto err;

    while (ddb_map_next_str(c, &key)){
        if (key.length < 4)
            continue;
        for (i = 0; i < key.length - 4; i++){
            uint32_t word = (*(uint32_t*)&key.data[i]);
            if (!(ptr = ddb_map_insert_int(freqs, word)))
                goto err;
            if (ptr)
                ++*ptr;
        }
    }
    err = 0;
err:
    ddb_map_cursor_free(c);
    if (err){
        ddb_map_free(freqs);
        return NULL;
    }
    return freqs;
}

static int sort_symbols(const struct ddb_map *freqs,
    uint64_t *totalfreq, struct hnode *book)
{
    int cmp(const void *p1, const void *p2)
    {
        const struct sortpair *x = (const struct sortpair*)p1;
        const struct sortpair *y = (const struct sortpair*)p2;

        if (x->freq > y->freq)
                return -1;
        else if (x->freq < y->freq)
                return 1;
        return 0;
    }

    int ret = -1;
    uint32_t i = *totalfreq = 0;
    struct ddb_map_cursor *c = NULL;
    struct sortpair *pairs;
    uint32_t symbol;
    uint64_t *freq;
    uint32_t num_symbols = ddb_map_num_items(freqs);
    if (!(pairs = calloc(num_symbols, sizeof(struct sortpair))))
        goto err;
    if (!(c = ddb_map_cursor_new(freqs)))
        goto err;

    while (ddb_map_next_item_int(c, &symbol, &freq)){
        pairs[i].symbol = symbol;
        pairs[i++].freq = *freq;
        *totalfreq += *freq;
    }
    qsort(pairs, num_symbols, sizeof(struct sortpair), cmp);

    num_symbols = ret = MIN(DDB_CODEBOOK_SIZE, num_symbols);
    for (i = 0; i < num_symbols; i++){
        book[i].symbol = pairs[i].symbol;
        book[i].weight = pairs[i].freq;
    }
err:
    ddb_map_cursor_free(c);
    free(pairs);
    return ret;
}

static struct ddb_map *make_codebook(struct hnode *nodes, int num_symbols)
{
    struct ddb_map *book;
    if (!(book = ddb_map_new(num_symbols)))
        return NULL;
    int i = num_symbols;
    while (i--){
        if (!nodes[i].num_bits)
            continue;
        uint64_t *ptr = ddb_map_insert_int(book, nodes[i].symbol);
        if (ptr)
            *ptr = nodes[i].code | (nodes[i].num_bits << 16);
        else{
            ddb_map_free(book);
            return NULL;
        }
    }
    return book;
}

int ddb_save_codemap(
    struct ddb_map *codemap,
    struct ddb_codebook book[DDB_CODEBOOK_SIZE])
{
    uint32_t symbol;
    uint64_t *ptr;
    struct ddb_map_cursor *c = NULL;
    if (!(c = ddb_map_cursor_new(codemap)))
        return -1;

    while (ddb_map_next_item_int(c, &symbol, &ptr)){
        uint32_t code = DDB_HUFF_CODE(*ptr);
        int n = DDB_HUFF_BITS(*ptr);
        int j = 1 << (16 - n);
        while (j--){
            int k = code | (j << n);
            book[k].symbol = symbol;
            book[k].bits = n;
        }
    }
    ddb_map_cursor_free(c);
    return 0;
}

struct ddb_map *ddb_create_codemap(const struct ddb_map *keys)
{
    struct hnode *nodes = NULL;
    struct ddb_map *freqs = NULL;
    struct ddb_map *book = NULL;
    uint64_t total_freq;
    int num_symbols;
    DDB_TIMER_DEF

    if (!(nodes = calloc(DDB_CODEBOOK_SIZE, sizeof(struct hnode))))
        goto err;

    DDB_TIMER_START
    if (!(freqs = collect_frequencies(keys)))
        goto err;
    DDB_TIMER_END("huffman/collect_frequencies")

    DDB_TIMER_START
    if ((num_symbols = sort_symbols(freqs, &total_freq, nodes)) < 0)
        goto err;
    DDB_TIMER_END("huffman/sort_symbols")

    DDB_TIMER_START
    if (huffman_code(nodes, num_symbols))
        goto err;
    DDB_TIMER_END("huffman/huffman_code")

    if (getenv("DDB_DEBUG_HUFFMAN"))
        output_stats(nodes, num_symbols, total_freq);

    DDB_TIMER_START
    book = make_codebook(nodes, num_symbols);
    DDB_TIMER_END("huffman/make_codebook")
err:
    ddb_map_free(freqs);
    free(nodes);
    return book;
}

int ddb_compress(const struct ddb_map *codemap, const char *src,
            uint32_t src_len, uint32_t *size, char **buf, uint64_t *buf_len)
{
    void write_literal(uint64_t *offs, const char byte)
    {
        /* literal: prefix by a zero bit (offs + 1) */
        #ifdef HUFFMAN_DEBUG
        fprintf(stderr, "ENC LITERAL: %c\n", byte);
        #endif
        write_bits(*buf, *offs + 1, byte & 255);
        *offs += 9;
    }
    uint32_t code, i = 0;
    /* each byte takes 9 bits in the worst case
     * + 3 bits length (1 byte) + 8 to make write_bits safe */
    const uint64_t worstcase = 1 + 8 + ((1 + src_len) * 1.125);
    uint64_t offs = 3; /* length residual takes the first 3 bits */
    if (*buf_len < worstcase){
        *buf_len = worstcase;
        if (!(*buf = realloc(*buf, *buf_len)))
            return -1;
    }
    memset(*buf, 0, worstcase);

    if (src_len >= 4)
        for (;i < src_len - 4; i++){
            uint32_t key = *(uint32_t*)&src[i];
            uint64_t *ptr = ddb_map_lookup_int(codemap, key);
            if (ptr){
                uint32_t bits = DDB_HUFF_BITS(*ptr);
                /* codeword: prefix code by an up bit */
                code = 1 | (DDB_HUFF_CODE(*ptr) << 1);
                write_bits(*buf, offs, code);

                #ifdef HUFFMAN_DEBUG
                fprintf(stderr, "%u (%lu) ENC VALUE[%u] (c %u) (b %u): ",i, offs, key,(code >> 1),bits);
                print_symbol(key);
                fprintf(stderr, "\n");
                #endif
                offs += bits + 1;
                i += 3;
            }else{
                write_literal(&offs, src[i]);
            }
        }
    for (;i < src_len; i++)
        write_literal(&offs, src[i]);

    if ((offs >> 3) >= UINT_MAX)
        return -1;
    *size = offs >> 3;
    if (offs & 7){
        ++*size;
        /* length residual */
        write_bits(*buf, 0, 8 - (offs & 7));
    }
    return 0;
}

int ddb_decompress(
    const struct ddb_codebook book[DDB_CODEBOOK_SIZE],
    const char *src,
    uint32_t src_len,
    uint32_t *size,
    char **buf,
    uint64_t *buf_len)
{
    char *p = *buf;
    uint32_t k = 0;
    uint64_t num_bits = src_len * 8LLU - read_bits(src, 0, 3);
    uint64_t offs = 3;
#if 0
    fprintf(stderr, "CODEBOOK:\n");
    int i;
    for (i = 0; i < DDB_CODEBOOK_SIZE; i++){
        struct hnode node = {.symbol = book[i].symbol,
                             .code = i,
                             .num_bits = book[i].bits};
        fprintf(stderr, "%d] ", i);
        print_symbol(node.symbol);
        fprintf(stderr, " | ");
        print_codeword(&node);
        fprintf(stderr, "\n");
    }
#endif

    while (offs < num_bits){
        uint32_t val = read_bits(src, offs, 17);
        if (k + 4 > *buf_len){
            *buf_len += 1024 * 1024;
            if (!(*buf = p = realloc(*buf, *buf_len)))
                return -1;
        }
        if (val & 1){
            val >>= 1;
            memcpy(p + k, &book[val].symbol, 4);
            #ifdef HUFFMAN_DEBUG
            fprintf(stderr, "%u (%lu) VALUE[%u] (b %u): ", k, offs, val, book[val].bits);
            print_symbol(book[val].symbol);
            fprintf(stderr, "\n");
            #endif
            offs += book[val].bits + 1;
            k += 4;
        }else{
            p[k] = (val >> 1) & 255;
            #ifdef HUFFMAN_DEBUG
            fprintf(stderr, "%u (%lu) LITERAL: %c\n", k, offs, (val >> 1) & 255);
            #endif
            offs += 9;
            ++k;
        }
    }
    *size = k;
    return 0;
}


