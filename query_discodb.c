
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include <discodb.h>

static void print_cursor(struct ddb_cursor *cur)
{
        if (ddb_empty_cursor(cur)){
                fprintf(stderr, "Not found\n");
                exit(1);
        }
        int i = 0;
        const struct ddb_entry *e;
        while ((e = ddb_next(cur))){
                printf("%.*s\n", e->length, e->data);
                ++i;
        }
        ddb_free_cursor(cur);
}

static struct ddb_query_clause *parse_cnf(char **tokens, int num, int *num_clauses)
{
        int k, i, j, t;
        *num_clauses = 1;
        for (i = 0; i < num; i++)
                if (!strcmp(tokens[i], "&"))
                        ++*num_clauses;

        struct ddb_query_term *terms = calloc(num - (*num_clauses - 1),
                sizeof(struct ddb_query_term));
        struct ddb_query_clause *clauses = calloc(*num_clauses,
                sizeof(struct ddb_query_clause));

        clauses[0].terms = terms;
        
        for (t = 0, k = 0, j = 0, i = 0; i < num; i++){
                if (!strcmp(tokens[i], "&")){
                        clauses[j++].num_terms = i - k;
                        clauses[j].terms = &terms[t];
                        k = t;
                        continue;
                }
                if (tokens[i][0] == '-'){
                        terms[t].not = 1;
                        terms[t].key.data = &tokens[i][1];
                        terms[t].key.length = strlen(&tokens[i][1]);
                }else{
                        terms[t].key.data = tokens[i];
                        terms[t].key.length = strlen(tokens[i]);
                }
                ++t;
        }
        clauses[j].num_terms = t - k;
#ifdef DEBUG
        for (i = 0; i < *num_clauses; i++){
                printf("Clause:\n");
                for (j = 0; j < clauses[i].num_terms; j++){
                        if (clauses[i].terms[j].not)
                                printf("NOT ");
                        printf("%.*s\n", clauses[i].terms[j].key.length,
                                clauses[i].terms[j].key.data);
                }
                printf("---\n");
        }
#endif
        return clauses;
}

static struct ddb *open_discodb(const char *file)
{
        struct stat nfo;
        int fd;
        
        if ((fd = open(file, O_RDONLY)) == -1){
                fprintf(stderr, "Couldn't open discodb %s\n", file);
                exit(1);
        }
        if (fstat(fd, &nfo)){
                fprintf(stderr, "Couldn't get the file size for %s\n", file);
                exit(1);
        }
        
        const char *p = mmap(0, nfo.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED){
                fprintf(stderr, "Loading %s failed\n", file);
                exit(1);
        }
        struct ddb *db;
        if (!(db = ddb_loads(p, nfo.st_size))){
                fprintf(stderr, "Invalid discodb %s\n", file);
                exit(1);
        }
        return db;
}

static void usage()
{
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "query_discodb [discodb] [-keys|-values|-item|-cnf] [query]\n");
        fprintf(stderr, "cnf format example: a b & -c d & e\n");
        exit(1);
}

int main(int argc, char **argv)
{
        if (argc < 3)
                usage();

        struct ddb *db = open_discodb(argv[1]);
        
        if (!strcmp(argv[2], "-keys"))
                print_cursor(ddb_keys(db));
        
        else if (!strcmp(argv[2], "-values"))
                print_cursor(ddb_values(db));
        
        else if (!strcmp(argv[2], "-item")){
                if (argc < 4){
                        fprintf(stderr, "Specify query\n");
                        exit(1);
                }
                struct ddb_entry e;
                e.data = argv[3];
                e.length = strlen(argv[3]);
                print_cursor(ddb_getitem(db, &e));
        }else if (!strcmp(argv[2], "-cnf")){
                if (argc < 4){
                        fprintf(stderr, "Specify query\n");
                        exit(1);
                }
                int num_q = 0;
                struct ddb_query_clause *q = parse_cnf(&argv[3], argc - 3, &num_q);
                print_cursor(ddb_query(db, q, num_q));
                free(q[0].terms);
                free(q);
        }else
                usage();

        ddb_free(db);
        return 0;
}
