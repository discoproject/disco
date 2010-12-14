
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <ddb_queue.h>

#ifdef QUEUE_MAIN
#define NDEBUG 1
#include <assert.h>
#endif

struct ddb_queue{
    void **q;
    uint32_t max;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

struct ddb_queue *ddb_queue_new(uint32_t max_length)
{
    struct ddb_queue *q = NULL;
    if (!max_length)
        return NULL;
    if (!(q = malloc(sizeof(struct ddb_queue))))
        return NULL;
    if (!(q->q = malloc(max_length * sizeof(void*))))
        return NULL;
    q->max = max_length;
    q->head = q->tail = q->count = 0;
    return q;
}

void ddb_queue_free(struct ddb_queue *q)
{
    free(q->q);
    free(q);
}

void ddb_queue_push(struct ddb_queue *q, void *e)
{
    if (q->max == q->count++){
        fprintf(stderr, "ddb_queue_push: max=%d, count=%d "
            "(this should never happen!)", q->max, q->count);
        abort();
    }
    q->q[q->head++ % q->max] = e;
}

void *ddb_queue_pop(struct ddb_queue *q)
{
    if (!q->count)
        return NULL;
    --q->count;
    return q->q[q->tail++ % q->max];
}

int ddb_queue_length(const struct ddb_queue *q)
{
    return q->count;
}

void *ddb_queue_peek(const struct ddb_queue *q)
{
    if (!q->count)
        return NULL;
    return q->q[q->tail % q->max];
}

#ifdef QUEUE_MAIN
int main(int argc, char **argv)
{
    static int num[] = {1, 2, 3};
    struct ddb_queue *q = ddb_queue_new(3);

    ddb_queue_push(q, &num[0]);
    ddb_queue_push(q, &num[1]);
    ddb_queue_push(q, &num[2]);
    printf("%d\n", *(int*)ddb_queue_pop(q));
    ddb_queue_push(q, &num[0]);
    printf("%d\n", *(int*)ddb_queue_pop(q));
    printf("%d\n", *(int*)ddb_queue_pop(q));
    ddb_queue_push(q, &num[1]);
    printf("%d\n", *(int*)ddb_queue_pop(q));
    printf("%d\n", *(int*)ddb_queue_peek(q));
    printf("%d\n", *(int*)ddb_queue_pop(q));
    printf("LENGTH %d\n", ddb_queue_length(q));

    ddb_queue_free(q);
    return 0;
}
#endif
