
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

struct ddb_entry *gen_values(const char *fname, struct ddb_entry *key, uint32_t *num_values)
{
        static int size = 1024 * 1024 * 100;
        key->data = fname;
        key->length = strlen(fname);
        int i = *num_values = 10;
        struct ddb_entry *values = malloc(i * sizeof(struct ddb_entry));
        while (i--){
                char *p = malloc(size);
                int j = size;
                while (j--)
                        p[j] = (char)rand();
                values[i].length = size;
                values[i].data = p;
        }
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
                //struct ddb_entry *values = gen_values(argv[argc], &key, &n);
                
                if (ddb_add(db, &key, values, n)){
                        printf("ERROR!\n");
                        exit(1);
                }
                while (n--)
                        free(values[n].data);
                free(values);
                //free(key.data);
        }

        uint64_t size;
        const char *data = ddb_finalize(db, &size);
        if (!data){
                printf("FINALIZATION failed\n");
                exit(1);
        }
        printf("SIZE %lu\n", size);

        //discodb_free(db);

        return 0;
}
