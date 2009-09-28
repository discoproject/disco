#include "discodb.h"

typedef struct {
  PyObject_VAR_HEAD
  PyObject   *obuffer;
  char       *cbuffer;
  struct ddb *discodb;
} DiscoDB;

#pragma mark General Object Protocol

static void       DiscoDB_dealloc (DiscoDB *);
static PyObject * DiscoDB_new     (PyTypeObject *, PyObject *, PyObject *);
static PyObject * DiscoDB_repr    (DiscoDB *);

#pragma mark Mapping Formal / Informal Protocol

static PyObject * DiscoDB_copy    (PyTypeObject *);
static int        DiscoDB_contains(DiscoDB *,      PyObject *);
static Py_ssize_t DiscoDB_length  (DiscoDB *);
static PyObject * DiscoDB_getitem (DiscoDB *,      PyObject *);
static PyObject * DiscoDB_iter    (PyObject *);
static PyObject * DiscoDB_items   (DiscoDB *);
static PyObject * DiscoDB_keys    (DiscoDB *);
static PyObject * DiscoDB_values  (DiscoDB *);
static PyObject * DiscoDB_query   (DiscoDB *,      PyObject *);

#pragma mark Serialization / Deserialization Informal Protocol

static PyObject * DiscoDB_dumps   (DiscoDB *);
static PyObject * DiscoDB_dump    (DiscoDB *, PyObject *);
static PyObject * DiscoDB_loads   (PyTypeObject *, PyObject *);
static PyObject * DiscoDB_load    (PyObject *);

#pragma mark ddb helpers

static struct ddb       *ddb_alloc         (void);
static struct ddb_cons  *ddb_cons_alloc    (void);
static struct ddb_entry *ddb_entry_alloc   (size_t);
static        void       ddb_cursor_dealloc(struct ddb_cursor *);
static        int        ddb_has_error     (struct ddb *);

#define DiscoDB_CLEAR(op) do { free(op); op = NULL; } while(0)
