
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <discodb.h>

size_t getline_(char **lineptr, size_t *n, FILE *stream)
{
  char *bufptr = NULL;
  char *p = bufptr;
  size_t size;
  int c;

  if (lineptr == NULL) {
    return -1;
  }
  if (stream == NULL) {
    return -1;
  }
  if (n == NULL) {
    return -1;
  }
  bufptr = *lineptr;
  size = *n;

  c = fgetc(stream);
  if (c == EOF) {
    return -1;
  }
  if (bufptr == NULL) {
    bufptr = malloc(128);
    if (bufptr == NULL) {
      return -1;
    }
    size = 128;
  }
  p = bufptr;
  while(c != EOF) {
    if ((p - bufptr) > (size - 1)) {
      size = size + 128;
      bufptr = realloc(bufptr, size);
      if (bufptr == NULL) {
        return -1;
      }
    }
    *p++ = c;
    if (c == '\n') {
      break;
    }
    c = fgetc(stream);
  }

  *p++ = '\0';
  *lineptr = bufptr;
  *n = size;

  return p - bufptr - 1;
}

static struct ddb_entry *read_file(const char *fname,
        struct ddb_entry *key, uint32_t *num_values)
{
        FILE *f;
        size_t lc = 0;
        size_t r = 0;
        char *line = NULL;
        size_t len = 0;

        if (!(f = fopen(fname, "r"))){
                fprintf(stderr, "Couldn't open %s\n", fname);
                exit(1);
        }

        struct ddb_entry *values = NULL;

        while ((r = getline_(&line, &len, f)) != -1) {
                lc++;
                free(line);
                line = NULL;
        }
        if (lc < 1){
                fprintf(stderr, "Not enough lines in %s.\n", fname);
                fprintf(stderr, "At least one line (key) is needed. The next lines should contain values.\n");
                exit(1);
        }
        rewind(f);
        key->length = getline_((char**)&key->data, &len, f) - 1;
        values = calloc(1, (lc - 1) * sizeof(struct ddb_entry));
        lc = 0;
        while ((r = getline_((char**)&values[lc].data, &len, f)) != -1){
                values[lc].length = r - 1;
                ++lc;
        }
        *num_values = lc;
        fclose(f);
        return values;
}

int main(int argc, char **argv)
{
        if (argc < 2){
                fprintf(stderr, "Usage:\n");
                fprintf(stderr, "create_discodb [discodb.out] [file_1] ... [file_N]\n");
                fprintf(stderr, "where input files contain key on the first line and values on the next lines.\n");
                exit(1);
        }

        struct ddb_cons *db = ddb_cons_new();
        if (!db){
                fprintf(stderr, "DB init failed\n");
                exit(1);
        }
        int i = 0;
        for (i = 2; i < argc; i++){
                uint32_t n;
                struct ddb_entry key = {NULL, 0};
                struct ddb_entry *values = read_file(argv[i], &key, &n);

                if (ddb_add(db, &key, values, n)){
                        fprintf(stderr, "Adding entries from %s failed: out of memory\n",
                                argv[i]);
                        exit(1);
                }
                while(n--)
                        free((char*)values[n].data);
                free(values);
                free((char*)key.data);
        }
        fprintf(stderr, "%u files read. Packing the index..\n", argc - 2);
        uint64_t size;
        char *data;
        if (!(data = ddb_finalize(db, &size))){
                fprintf(stderr, "Packing the index failed: duplicate keys or out of memory\n");
                exit(1);
        }

        FILE *out;
        if (!(out = fopen(argv[1], "w"))){
                fprintf(stderr, "Opening file %s failed\n", argv[1]);
                exit(1);
        }
        fwrite(data, size, 1, out);
        fclose(out);

        free(data);

        fprintf(stderr, "Ok! Index written to %s\n", argv[1]);
        return 0;
}
