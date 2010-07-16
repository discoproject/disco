
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <discodb.h>

int main(int argc, char **argv)
{
    if (argc < 2){
            fprintf(stderr, "Usage:\n");
            fprintf(stderr, "create_discodb discodb.out input.txt\n");
            fprintf(stderr, "where input.txt contain a key-value pair on each line, devided by space.\n");
            exit(1);
    }

    FILE *in;
    FILE *out;
    char *key;
    char *value;
    uint32_t lc = 0;
    uint64_t size;
    char *data;
    struct ddb_cons *db = ddb_cons_new();
    uint64_t flags = getenv("DONT_COMPRESS") ? DDB_OPT_DISABLE_COMPRESSION: 0;

    if (!db){
            fprintf(stderr, "DB init failed\n");
            exit(1);
    }

    if (!(in = fopen(argv[2], "r"))){
            fprintf(stderr, "Couldn't open %s\n", argv[2]);
            exit(1);
    }
    while(fscanf(in, "%as %as\n", &key, &value) == 2){
        struct ddb_entry key_e = {.data = key, .length = strlen(key)};
        struct ddb_entry val_e = {.data = value, .length = strlen(value)};
        if (ddb_add(db, &key_e, &val_e)){
            fprintf(stderr, "Adding '%s':'%s' failed\n", key, value);
            exit(1);
        }
        free(key);
        free(value);
        ++lc;
    }
    fclose(in);
    fprintf(stderr, "%u key-value pairs read. Packing the index..\n", lc);

    if (!(data = ddb_finalize(db, &size, flags))){
        fprintf(stderr, "Packing the index failed\n");
        exit(1);
    }
    ddb_cons_free(db);

    if (!(out = fopen(argv[1], "w"))){
        fprintf(stderr, "Opening file %s failed\n", argv[1]);
        exit(1);
    }
    if (!fwrite(data, size, 1, out)){
        fprintf(stderr, "Writing file %s failed\n", argv[1]);
        exit(1);
    }
    fclose(out);

    free(data);
    fprintf(stderr, "Ok! Index written to %s\n", argv[1]);
    return 0;
}
