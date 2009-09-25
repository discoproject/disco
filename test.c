
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <discodb.h>

struct ddb_entry *read_file(const char *fname, struct ddb_entry *key, uint32_t *num_values)
{
        FILE *f = fopen(fname, "r");
        size_t lc = 0;
        size_t r = 0;
        char *line = NULL;
        size_t len = 0;
        
        struct ddb_entry *values = NULL;
        
        printf("reading %s\n", fname);
        while ((r = getline(&line, &len, f)) != -1) {
                lc++;
                free(line);
                line = NULL;
        }
        printf("%lu lines\n", lc);
        rewind(f);
        key->length = getline((char**)&key->data, &len, f);
        values = calloc(1, (lc - 1) * sizeof(struct ddb_entry));
        lc = 0;
        while ((r = getline((char**)&values[lc].data, &len, f)) != -1){
                values[lc].length = r;
                ++lc;
        }
        *num_values = lc;
        
        return values;
}

int main(int argc, char **argv)
{
        struct ddb_entry key = {NULL, 0};
        struct ddb_cons *db = ddb_new();
        if (!db){
                printf("DB init failed\n");
                exit(1);
        }
        uint32_t n;
        
        while (--argc){
                struct ddb_entry *values = read_file(argv[argc], &key, &n);
                //int i = 0;
                //printf("VALUES (%u):\n", n);
                //for (i = 0; i < n; i++)
                //        printf("VAL (%s) LEN (%u)\n", values[i].data, values[i].len);
                if (ddb_add(db, &key, values, n)){
                        printf("ERROR!\n");
                        exit(1);
                }
                free(values);
                free(key.data);
        }

        uint64_t size;
        const char *data = ddb_finalize(db, &size);
        if (!data){
                printf("FINALIZATION failed\n");
                exit(1);
        }

        //discodb_free(db);

        return 0;
}
