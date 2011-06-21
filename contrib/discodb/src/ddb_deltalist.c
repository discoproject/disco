
#include <stdint.h>
#include <stdlib.h>

#include <ddb_deltalist.h>

/*
 ddb_deltalist -> dsegment -> dsegment -> ... -> dsegment -> dsegment -> ...
                  (default)   (not-full)         (full)      (full)

 - Each segment hosts a delta-encoded list of entries in ascending order
   (maxval is the current largest value in the list).
 - New entry is added as follows
   1. First check the global minimum, min_maxval, to see if no segment can match.
      - If yes, go to 2)
      - If no, create a new segment to the head.
   2. First try to add a new entry to the default first segment.
      - If not successful, try to find a suitable segment,
        until full segments are reached.
   3. Successful segment is moved to the head (~LRU policy).
   4. Add entry to the head segment
      - If segment is full, move it to the tail.
      - Create a new segment to the head.
*/

#define VALUES_INC 1000000

struct dsegment{
    struct dsegment *next;
    valueid_t maxval;
    uint16_t i;
    uint16_t size;
    uint16_t deltas[0];
};

struct ddb_deltalist{
    struct dsegment *head;
    valueid_t min_maxval;
};

static struct dsegment *new_segment(struct ddb_deltalist *d)
{
    struct dsegment *s;
    if (!(s = calloc(1, sizeof(struct dsegment))))
        return NULL;
    s->next = d->head;
    d->head = s;
    return s;
}

struct ddb_deltalist *ddb_deltalist_new()
{
    struct ddb_deltalist *d;
    if (!(d = calloc(1, sizeof(struct ddb_deltalist))))
        return NULL;
    d->min_maxval = UINT32_MAX;
    return d;
}

void ddb_deltalist_free(struct ddb_deltalist *d)
{
    struct dsegment *s = d->head;
    while (s){
        struct dsegment *n = s->next;
        free(s);
        s = n;
    }
    free(d);
}

static struct dsegment *segment_full(struct ddb_deltalist *d)
{
    if (d->head->next){
        struct dsegment *tail = d->head, *s = d->head;
        while (tail->next)
            tail = tail->next;
        d->head = s->next;
        tail->next = s;
        s->next = NULL;
    }
    return new_segment(d);
}

static struct dsegment *grow_segment(struct ddb_deltalist *d)
{
    struct dsegment *s = d->head;
    switch (s->size){
        case 0:
            s->size = 4;
            break;
        case 32768:
            s->size = UINT16_MAX;
            break;
        case UINT16_MAX:
            return segment_full(d);
        default:
            s->size *= 2;
    }
    if (!(s = realloc(s, sizeof(struct dsegment) + s->size * 2)))
        return NULL;
    d->head = s;
    return s;
}

static struct dsegment *matching_segment(struct ddb_deltalist *d, valueid_t e)
{
    struct dsegment *s = d->head;
    struct dsegment *prev = NULL;
    while (s && !(s->size == UINT16_MAX && s->i == UINT16_MAX)){
        if (e > s->maxval && e - s->maxval <= UINT16_MAX){
            if (prev){
                /* move matching segment to head */
                prev->next = s->next;
                s->next = d->head;
                d->head = s;
            }
            return s;
        }
        prev = s;
        s = s->next;
    }
    return new_segment(d);
}

int ddb_deltalist_append(struct ddb_deltalist *d, valueid_t e)
{
    struct dsegment *match;

    if (d->min_maxval > e || !d->head){
        if (!(match = new_segment(d)))
            return -1;
    }else{
        if (!(match = matching_segment(d, e)))
            return -1;
        if (match->i >= match->size)
            if (!(match = grow_segment(d)))
                return -1;
    }
    if (!match->i){
        ++match->i;
        match->maxval = e;
    }else{
        match->deltas[match->i++ - 1] = e - match->maxval;
        match->maxval = e;
    }
    if (match->maxval < d->min_maxval)
        d->min_maxval = match->maxval;
    return 0;
}

static valueid_t *decode(const struct dsegment *s, valueid_t *buf)
{
    if (s->i > 0){
        valueid_t i = s->i - 1;
        valueid_t v = s->maxval;
        while (i--){
            *buf++ = v;
            v -= s->deltas[i];
        }
        *buf++ = v;
    }
    return buf;
}

int ddb_deltalist_to_array(const struct ddb_deltalist *d,
                           uint64_t *num_values,
                           valueid_t **values,
                           uint64_t *values_size)
{
    const struct dsegment *s = d->head;
    *num_values = 0;
    while (s){
        *num_values += s->i;
        s = s->next;
    }
    if (*num_values > *values_size){
        while (*values_size < *num_values)
            *values_size += VALUES_INC;
        free(*values);
        *values = NULL;
        if (!(*values = malloc(*values_size * sizeof(valueid_t))))
            return -1;
    }
    valueid_t *arr = *values;
    s = d->head;
    while (s){
        arr = decode(s, arr);
        s = s->next;
    }
    return 0;
}

void ddb_deltalist_mem_usage(const struct ddb_deltalist *d,
                             uint64_t *segments,
                             uint64_t *alloc,
                             uint64_t *used)
{
   *segments = 0;
   *alloc = *used = sizeof(struct ddb_deltalist);
   const struct dsegment *s = d->head;
   while (s){
        ++*segments;
        *alloc += sizeof(struct dsegment) + s->size * 2;
        *used += sizeof(struct dsegment) + (s->i ? (s->i - 1) * 2: 0);
        s = s->next;
   }
}

#ifdef DELTALIST_TEST

#include <stdlib.h>

#define DDB_PROFILE
#include <ddb_profile.h>
#include <ddb_list.h>

static int lst_cmp(const void *p1, const void *p2)
{
    const uint64_t x = *(const uint64_t*)p1;
    const uint64_t y = *(const uint64_t*)p2;

    if (x > y)
        return 1;
    else if (x < y)
        return -1;
    return 0;
}

static int dlst_cmp(const void *p1, const void *p2)
{
    const valueid_t x = *(const valueid_t*)p1;
    const valueid_t y = *(const valueid_t*)p2;

    if (x > y)
        return 1;
    else if (x < y)
        return -1;
    return 0;
}

void create_and_check(struct ddb_list *lst)
{
    uint64_t dlen;
    uint32_t len;
    uint64_t *ptr = ddb_list_pointer(lst, &len);
    struct ddb_deltalist *d = ddb_deltalist_new();
    int i;
    DDB_TIMER_DEF;
    DDB_TIMER_START;
    for (i = 0; i < len; i++)
        if (ddb_deltalist_append(d, ptr[i])){
            printf("KABOOM!\n");
            exit(1);
        }
    DDB_TIMER_END("deltalist creation");

    valueid_t *values = NULL;
    uint64_t values_size = 0;
    if (ddb_deltalist_to_array(d, &dlen, &values, &values_size)){
        printf("KABOOM 2\n");
        exit(1);
    }
    if (dlen != len){
        printf("length mismatch: got %u, should be %u\n", dlen, len);
        exit(1);
    }
    qsort(ptr, len, 8, lst_cmp);
    qsort(values, len, sizeof(valueid_t), dlst_cmp);
    for (i = 0; i < len; i++)
        if (values[i] != ptr[i]){
            printf("value mismatch: got %u, should be %llu\n", values[i], ptr[i]);
            exit(1);
        }
    uint64_t alloc, used, segments;
    ddb_list_mem_usage(lst, &alloc, &used);
    printf("LIST allocated %llu, used %llu\n", alloc, used);
    ddb_deltalist_mem_usage(d, &segments, &alloc, &used);
    printf("DELTALIST %u segments, allocated %llu, used %llu\n",
        segments, alloc, used);

    free(values);
    ddb_list_free(lst);
    ddb_deltalist_free(d);
    printf("OK\n");
}

static struct ddb_list *empty_list()
{
    struct ddb_list *a = ddb_list_new();
    return a;
}

static struct ddb_list *one_list()
{
    struct ddb_list *a = ddb_list_new();
    return ddb_list_append(a, 123456);
}

static struct ddb_list *tenmillion_list()
{
    DDB_TIMER_DEF;
    DDB_TIMER_START;
    struct ddb_list *a = ddb_list_new();
    int i;
    for (i = 0; i < 10000000; i++){
        a = ddb_list_append(a, i);
    }
    DDB_TIMER_END("list creation");
    return a;
}

static struct ddb_list *pathological_list()
{
    DDB_TIMER_DEF;
    DDB_TIMER_START;
    struct ddb_list *a = ddb_list_new();
    int i = 100000;
    while (i--)
        a = ddb_list_append(a, i);
    DDB_TIMER_END("list creation");
    return a;
}

static struct ddb_list *gap_list()
{
    DDB_TIMER_DEF;
    DDB_TIMER_START;
    struct ddb_list *a = ddb_list_new();
    int i;
    for (i = 0; i < 1000; i++)
        a = ddb_list_append(a, i * (UINT16_MAX + 1));
    DDB_TIMER_END("list creation");
    return a;
}

static struct ddb_list *random_list()
{
    DDB_TIMER_DEF;
    DDB_TIMER_START;
    struct ddb_list *a = ddb_list_new();
    int i;
    for (i = 0; i < 1000000; i++)
        a = ddb_list_append(a, (int)(1e7 * (rand() / (float)RAND_MAX)));
    DDB_TIMER_END("list creation");
    return a;
}

int main(int argc, char **argv)
{
    printf("-- EMPTY LIST --\n");
    create_and_check(empty_list());
    printf("-- ONE ENTRY --\n");
    create_and_check(one_list());
    printf("-- TEN MILLION ENTRIES --\n");
    create_and_check(tenmillion_list());
    printf("-- GAP LIST --\n");
    create_and_check(gap_list());
    printf("-- RANDOM LIST --\n");
    create_and_check(random_list());
    printf("-- PATHOLOGICAL LIST --\n");
    create_and_check(pathological_list());
    return 0;
}

#endif
