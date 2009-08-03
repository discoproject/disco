
#define _GNU_SOURCE

#include <unistd.h>
#include <cmph.h>

#include <discodb.h>

discodb_t *discodb_new(const char *path_prefix)
{
        discodb_t *db = NULL;

        if (!(db = calloc(1, sizeof(discodb_t))))
                return NULL;
                
        if (asprintf(&db->toc_name, "%s-toc.temp", path_prefix) == -1){
                db->toc_name = NULL;
                goto err;
        }
        if (asprintf(&db->data_name, "%s-data.temp", path_prefix) == -1){
                db->data_name = NULL;
                goto err;
        }
        if (asprintf(&db->valmap_name, "%s-valmap.temp", path_prefix) == -1){
                db->valmap_name = NULL;
                goto err;
        }
        if (asprintf(&db->valtoc_name, "%s-valtoc.temp", path_prefix) == -1){
                db->valtoc_name = NULL;
                goto err;
        }
        if (!(db->toc_f = fopen(db->toc_name, "w+")))
                goto err;
        if (!(db->data_f = fopen(db->data_name, "w+")))
                goto err;
        if (!(db->valmap_f = fopen(db->valmap_name, "w+")))
                goto err;
        if (!(db->valtoc_f = fopen(db->valtoc_name, "w+")))
                goto err;
        
        db->next_id = 1;
        return db;
err:
        discodb_free(db);
        return NULL;
}

void discodb_free(discodb_t *db)
{
        Word_t tmp;

        if (db->toc_f){
                fclose(db->toc_f);
                unlink(db->toc_name);
        }
        if (db->data_f){
                fclose(db->data_f);
                unlink(db->data_name);
        }
        if (db->valmap_f){
                fclose(db->valmap_f);
                unlink(db->valmap_name);
        }
        if (db->valtoc_f){
                fclose(db->valtoc_f);
                unlink(db->valtoc_name);
        }

        free(db->toc_name);
        free(db->data_name);
        free(db->valmap_name);
        free(db->valtoc_name);

        JHSFA(tmp, db->valmap);

        free(db->tmpbuf);
        free(db);
}

static uint32_t new_val(discodb_t *db, Word_t *id, const ddb_attr_t *attr)
{
        /* write ID -> value mapping */
        if (fwrite(&db->valmap_offs, 4, 1, db->valtoc_f) != 1)
                return 0;
        if (fwrite(attr->data, attr->len, 1, db->valmap_f) != 1)
                return 0;
        db->valmap_offs += attr->len;
        *id = db->next_id++;
        return *id;
}

static uint32_t encode_values(discodb_t *db,
        const ddb_attr_t *values, uint32_t num_values)
{
        /* find maximum delta -> bits needed per id */
        uint32_t i, max_diff = values[0].id;
        for (i = 1; i < num_values; i++){
                uint32_t d = values[i].id - values[i - 1].id;
                if (d > max_diff)
                        max_diff = d;
        }
        
        uint64_t offs = 0;
        uint32_t prev = 0;
        uint32_t bits = bits_needed(max_diff);
        uint32_t size;
        if (!(size = allocate_bits(db, 5 + bits * num_values)))
                return 0;

        /* values field:
           [ bits_needed (5 bits) | delta-encoded values (bits * num_values) ]
        */
         
        write_bits(db->tmpbuf, offs, bits);
        offs += 5;
        for (i = 0; i < num_values; i++){
                write_bits(db->tmpbuf, offs, values[i].id - prev);
                prev = values[i].id;
                offs += bits;
        }
        
        return size;
}

int discodb_add(discodb_t *db, const ddb_attr_t *key,
        ddb_attr_t *values, uint32_t num_values)
{
        uint32_t i = num_values;
        
        /* find IDs for values */
        while (i--){
                Word_t *id = NULL;
                if (copy_to_buf(db, values[i].data, values[i].len))
                        return -1;
                JHSI(id, db->valmap, db->tmpbuf, values[i].len);
                if (id == PJERR)
                        return -1;
                else if (*id)
                        values[i].id = (uint32_t)*id;
                else if (!(values[i].id = new_val(db, id, &values[i])))
                        return -1;
        }

        /* sort by ascending IDs */
        qsort(values, num_values, sizeof(ddb_attr_t), id_cmp);

        /* delta-encode value ID list */
        uint32_t size = 0;
        if (!(size = encode_values(db, values, num_values)))
                return -1;
        
        /* write data 
         
           attribute entry:
           [ key_len | key | val_len | val_bits | delta-encoded values ] 
        */
        if (fwrite(&db->data_offs, 4, 1, db->toc_f) != 1)
                return -1;
        if (fwrite(&key->len, 4, 1, db->data_f) != 1)
                return -1;
        if (fwrite(key->data, key->len, 1, db->data_f) != 1)
                return -1;
        if (fwrite(&size, 4, 1, db->data_f) != 1)
                return -1;
        if (fwrite(db->tmpbuf, size, 1, db->data_f) != 1)
                return -1;
        
        db->data_offs += 4 + key->len + 4 + size;
        ++db->num_keys;

        return 0;
}

#define READ_SAFE(dst, sze)\
        if (fread(dst, sze, 1, db->data_f) != 1){\
                hash_fail = 1;\
                *len = 0;\
                return 0;\
        }

static int build_hash(discodb_t *db)
{
        int hash_fail = 0;

        void xdispose(void *data, char *key, cmph_uint32 l) {}
        void xrewind(void *data) { rewind(db->data_f); }
        int xread(void *data, char **p, cmph_uint32 *len)
        {
                uint32_t x;
                READ_SAFE(len, 4)
                if (!(*p = malloc(*len))){
                        hash_fail = 1;
                        *len = 0;
                        return 0;
                }
                READ_SAFE(*p, *len)
                READ_SAFE(&x, 4)
                if (fseek(db->data_f, x, SEEK_CUR)){
                        hash_fail = 1;
                        *len = 0;
                        return 0;
                }
                return *len;
        }

        cmph_io_adapter_t r;
        r.data = NULL;
        r.nkeys = db->num_keys;
        r.read = xread;
        r.dispose = xdispose;
        r.rewind = xrewind;
        
        rewind(db->data_f);

        cmph_config_t *c = cmph_config_new(&r);
        cmph_config_set_algo(c, CMPH_CHD);
    
        if (getenv("DEBUG"))
                cmph_config_set_verbosity(c, 5);
        
        cmph_t *g = cmph_new(c);
        if (!g)
                printf("HASH FAILED\n");

        printf("HASH SIZE %u\n", cmph_packed_size(g));


        return 0;

}

int discodb_build(discodb_t *db, int outfd)
{       
        build_hash(db);
        return 0;
}

ddb_attr_t *read_file(const char *fname, ddb_attr_t *key, uint32_t *num_values)
{
        FILE *f = fopen(fname, "r");
        size_t lc = 0;
        size_t r = 0;
        char *line = NULL;
        size_t len = 0;
        
        ddb_attr_t *values = NULL;
        
        printf("reading %s\n", fname);
        while ((r = getline(&line, &len, f)) != -1) {
                lc++;
                free(line);
                line = NULL;
        }
        printf("%lu lines\n", lc);
        rewind(f);
        key->len = getline((char**)&key->data, &len, f);
        values = calloc(1, (lc - 1) * sizeof(ddb_attr_t));
        lc = 0;
        while ((r = getline((char**)&values[lc].data, &len, f)) != -1){
                values[lc].len = r;
                ++lc;
        }
        *num_values = lc;
        
        return values;
}

int main(int argc, char **argv)
{

        ddb_attr_t key = {NULL, 0, 0};
        discodb_t *db = discodb_new("ddb");
        if (!db){
                printf("DB init failed\n");
                exit(1);
        }
        uint32_t n;
        
        while (--argc){
                if (key.data)
                        free(key.data);
                key.data = NULL;
                ddb_attr_t *values = read_file(argv[argc], &key, &n);
                //int i = 0;
                //printf("VALUES (%u):\n", n);
                //for (i = 0; i < n; i++)
                //        printf("VAL (%s) LEN (%u)\n", values[i].data, values[i].len);
                if (discodb_add(db, &key, values, n))
                        printf("ERROR!\n");
        }

        discodb_build(db, 0);
        //discodb_free(db);

        return 0;
}
