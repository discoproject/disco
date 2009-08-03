
#define _GNU_SOURCE

#include <discodb.h>

discodb_t *discodb_new(const char *path_prefix)
{
        char *toc_name = NULL;
        char *data_name = NULL;
        char *valmap_name = NULL;
        char *valtoc_name = NULL;
        discodb_t *db = NULL;

        if (!(db = calloc(1, sizeof(discodb_t))))
                return NULL;
                
        if (asprintf(&toc_name, "%s-toc.temp", path_prefix) == -1){
                toc_name = NULL;
                goto err;
        }
        if (asprintf(&data_name, "%s-data.temp", path_prefix) == -1){
                data_name = NULL;
                goto err;
        }
        if (asprintf(&valmap_name, "%s-valmap.temp", path_prefix) == -1){
                valmap_name = NULL;
                goto err;
        }
        if (asprintf(&valtoc_name, "%s-valtoc.temp", path_prefix) == -1){
                valtoc_name = NULL;
                goto err;
        }
        if (!(db->toc_f = fopen(toc_name, "w")))
                goto err;
        if (!(db->data_f = fopen(data_name, "w")))
                goto err;
        if (!(db->valmap_f = fopen(valmap_name, "w")))
                goto err;
        if (!(db->valtoc_f = fopen(valtoc_name, "w")))
                goto err;
        
        db->next_id = 1;

        free(toc_name);
        free(data_name);
        free(valmap_name);
        free(valtoc_name);
        return db;

err:
        free(toc_name);
        free(data_name);
        free(valmap_name);
        free(valtoc_name);
        if (db && db->toc_f)
                fclose(db->toc_f);
        if (db && db->data_f)
                fclose(db->data_f);
        if (db && db->valmap_f)
                fclose(db->valmap_f);
        if (db && db->valtoc_f)
                fclose(db->valtoc_f);
        free(db);
        return NULL;      
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
        uint32_t size = encode_values(db, values, num_values);
        
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
        uint32_t n;
        
        while (--argc){
                ddb_attr_t *values = read_file(argv[argc], &key, &n);
                //int i = 0;
                //printf("VALUES (%u):\n", n);
                //for (i = 0; i < n; i++)
                //        printf("VAL (%s) LEN (%u)\n", values[i].data, values[i].len);
                if (discodb_add(db, &key, values, n))
                        printf("ERROR!\n");
        }

        return 0;
}
