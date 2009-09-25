
#include <ddb_internal.h>

void ddb_fetch_item(uint32_t i, const uint64_t *toc, const char *data,
        struct ddb_entry *key, struct ddb_value_cursor *val)
{
        const char *p = &data[toc[i]];

        key->length = *(uint32_t*)p;
        key->data = &p[4]; 

        val->num_left = *(uint32_t*)&p[4 + key->length];
        val->deltas = &p[8 + key->length];
        val->bits = read_bits(val->deltas, 0, 5);
        val->offset = 5;
}
