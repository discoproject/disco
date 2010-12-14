
#include <string.h>
#include <stdio.h>

#include <discodb.h>
#include <ddb_internal.h>

static inline void set_bit(char *b, uint32_t offset)
{
    b[offset >> 3] |= (1 << (offset & 7));
}

static inline int test_bit(const char *b, uint32_t offset)
{
    return b[offset >> 3] & (1 << (offset & 7));
}

static inline void isect(char *dst, const char *src, uint32_t len)
{
    int i = len >> 3;
    while (i--)
        ((uint64_t*)dst)[i] &= ((const uint64_t*)src)[i];
}

static inline int isempty(const char *b, uint32_t len)
{
    int i = len >> 3;
    while (i--)
        if (((const uint64_t*)b)[i])
            return 0;
    return 1;
}

#ifdef DEBUG
static void print_binary(const char *dst, uint32_t len)
{
    int i;
    for (i = 0; i < (len << 3); i++)
        if (test_bit(dst, i))
            printf("1");
        else
            printf("0");
    printf("\n");
}
#endif

static int find_max_clause(struct ddb_cnf_cursor *cnf)
{
    cnf->base_id = 0;
    uint32_t j, i = cnf->num_clauses;
    while (i--){
        const struct ddb_cnf_clause *clause = &cnf->clauses[i];
        int allempty = 1;
        valueid_t cmin = DDB_MAX_NUM_VALUES;
        for (j = 0; j < clause->num_terms; j++)
            if (!clause->terms[j].empty){
                allempty = 0;
                if (clause->terms[j].cur_id < cmin)
                    cmin = clause->terms[j].cur_id;
            }
        if (allempty)
            return 0;
        if (cmin > cnf->base_id)
            cnf->base_id = cmin;
    }
    return 1;
}

static int clause_unions(struct ddb_cnf_cursor *cnf)
{
    uint32_t j, i = cnf->num_clauses;
    const valueid_t maxid = cnf->base_id + WINDOW_SIZE;
    while (i--){
        struct ddb_cnf_clause *clause = &cnf->clauses[i];
        memset(clause->unionn, 0, WINDOW_SIZE_BYTES);
        int allempty = 1;
        for (j = 0; j < clause->num_terms; j++){
            struct ddb_cnf_term *t = &clause->terms[j];
            if (!t->empty){
                allempty = 0;
                while (t->cur_id < cnf->base_id && !t->empty)
                    t->next(t);
                while (t->cur_id < maxid && !t->empty){
                    set_bit(clause->unionn, t->cur_id - cnf->base_id);
                    t->next(t);
                }
            }
        }
#ifdef DEBUG
        printf("dbg UNION[%u] ", i);
        print_binary(clause->unionn, WINDOW_SIZE_BYTES);
#endif
        if (allempty)
            return 0;
    }
    return 1;
}

static int intersect_clauses(struct ddb_cnf_cursor *cnf)
{
    if (!cnf->num_clauses)
        return 1;

    cnf->isect_offset = 0;
    memcpy(cnf->isect, cnf->clauses[0].unionn, WINDOW_SIZE_BYTES);
    int i;
    for (i = 1; i < cnf->num_clauses; i++){
        isect(cnf->isect, cnf->clauses[i].unionn, WINDOW_SIZE_BYTES);
#ifdef DEBUG
        printf("dbg ISECT[%u] ", i);
        print_binary(cnf->isect, WINDOW_SIZE_BYTES);
#endif
    }
    return isempty(cnf->isect, WINDOW_SIZE_BYTES);
}

static const struct ddb_entry *next_isect_entry(struct ddb_cursor *c)
{
    struct ddb_cnf_cursor *cnf = &c->cursor.cnf;
    for(; cnf->isect_offset < WINDOW_SIZE; cnf->isect_offset++)
        if (test_bit(cnf->isect, cnf->isect_offset)){
            if (ddb_get_valuestr(c, cnf->base_id + cnf->isect_offset))
                return NULL;
            ++cnf->isect_offset;
            return &c->entry;
        }
    return NULL;
}

const struct ddb_entry *ddb_cnf_cursor_next(struct ddb_cursor *c)
{
    struct ddb_cnf_cursor *cnf = &c->cursor.cnf;
    const struct ddb_entry *e;
    if ((e = next_isect_entry(c)))
        return e;

    while (1){
#ifdef DEBUG
        printf("dbg NEXT WINDOW\n");
#endif
        if (!find_max_clause(cnf))
            return NULL;
#ifdef DEBUG
        ddb_get_valuestr(c, cnf->base_id);
        printf("dbg MAX %.*s (%u)\n", c->entry.length, c->entry.data, cnf->base_id);
#endif
        if (!clause_unions(cnf))
            return NULL;
        if (!intersect_clauses(cnf))
            return next_isect_entry(c);
    }
}

valueid_t ddb_not_next(struct ddb_cnf_term *t)
{
    struct ddb_delta_cursor *v = &t->cursor->cursor.value;
    if (t->empty)
        return 0;
    if (!t->cur_id++ && v->num_left)
        /* first step */
        ddb_delta_cursor_next(v);
    while (t->cur_id == v->cur_id){
        ++t->cur_id;
        if (v->num_left)
            ddb_delta_cursor_next(v);
    }
    if (t->cur_id >= t->cursor->db->num_uniq_values + 1){
        t->empty = 1;
        t->cur_id = 0;
    }
    return t->cur_id;
}

valueid_t ddb_val_next(struct ddb_cnf_term *t)
{
    if (t->empty)
        return 0;
    else if (t->cursor->cursor.value.num_left){
        ddb_delta_cursor_next(&t->cursor->cursor.value);
        t->cur_id = t->cursor->cursor.value.cur_id;
        return t->cur_id;
    }else{
        t->empty = 1;
        t->cur_id = 0;
        return 0;
    }
}
