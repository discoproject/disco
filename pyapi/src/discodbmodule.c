#include <Python.h>
#include "structmember.h"

#include "discodb.h"
#include "discodbmodule.h"

#pragma mark discodb Module Methods

static PyMethodDef discodb_methods[] = {
  {NULL}                         /* Sentinel          */
};

#pragma mark DiscoDB Object Definition

PyDoc_STRVAR(DiscoDB_doc, "DiscoDB(iter) -> new DiscoDB from k, v_list in iter");

static PySequenceMethods DiscoDB_as_sequence = {
  NULL,                          /* sq_length         */
  NULL,                          /* sq_concat         */
  NULL,                          /* sq_repeat         */
  NULL,                          /* sq_item           */
  NULL,                          /* sq_slice          */
  NULL,                          /* sq_ass_item       */
  NULL,                          /* sq_ass_slice      */
  (objobjproc)DiscoDB_contains,  /* sq_contains       */
  NULL,                          /* sq_inplace_concat */
  NULL,                          /* sq_inplace_repeat */
};

static PyMappingMethods DiscoDB_as_mapping = {
  (lenfunc)   DiscoDB_length,    /* mp_length         */
  (binaryfunc)DiscoDB_getitem,   /* mp_subscript      */
  NULL,                          /* mp_ass_subscript  */
};

static PyMethodDef DiscoDB_methods[] = {
  {"copy", (PyCFunction)DiscoDB_copy, METH_NOARGS,
   "d.copy() -> a copy of d."},
  {"items", (PyCFunction)DiscoDB_items, METH_NOARGS,
   "d.items() an iterator over the items of d."},
  {"keys", (PyCFunction)DiscoDB_keys, METH_NOARGS,
   "d.keys() an iterator over the keys of d."},
  {"values", (PyCFunction)DiscoDB_values, METH_NOARGS,
   "d.values() -> an iterator over the values of d."},
  {"query", (PyCFunction)DiscoDB_query, METH_O,
   "d.query(q) -> an iterator over the values of d whose keys satisfy q."},
  {"dumps", (PyCFunction)DiscoDB_dumps, METH_NOARGS,
   "d.dumps() -> a serialization of d."},
  {"dump", (PyCFunction)DiscoDB_dump, METH_O,
   "d.dump(o) -> write serialization of d to file object o."},
  {"loads", (PyCFunction)DiscoDB_loads, METH_CLASS | METH_O,
   "D.loads(s) -> a deserialized instance of D from serialization s."},
  {"load", (PyCFunction)DiscoDB_load, METH_CLASS | METH_O,
   "D.load(o) -> a deserialized instance of D from file object o."},
  {NULL}                         /* Sentinel          */
};

static PyMemberDef DiscoDB_members[] = {
  {NULL}                         /* Sentinel          */
};

static PyTypeObject DiscoDBType = {
  PyVarObject_HEAD_INIT(&PyType_Type, 0)
  "_discodb.DiscoDB",            /* tp_name           */
  sizeof(DiscoDB),               /* tp_basicsize      */
  0,                             /* tp_itemsize       */
  (destructor)DiscoDB_dealloc,   /* tp_dealloc        */
  0,                             /* tp_print          */
  0,                             /* tp_getattr       */
  0,                             /* tp_setattr       */
  0,                             /* tp_compare        */
  0, // (reprfunc)DiscoDB_repr   /* tp_repr           */
  0,                             /* tp_as_number      */
  &DiscoDB_as_sequence,          /* tp_as_sequence    */
  &DiscoDB_as_mapping,           /* tp_as_mapping     */
  0,                             /* tp_hash           */
  0,                             /* tp_call           */
  0,                             /* tp_str            */
  0,                             /* tp_getattro       */
  0,                             /* tp_setattro       */
  0,                             /* tp_as_buffer      */
  Py_TPFLAGS_DEFAULT |
  Py_TPFLAGS_BASETYPE,           /* tp_flags          */
  DiscoDB_doc,                   /* tp_doc            */
  0,                             /* tp_traverse       */
  0,                             /* tp_clear          */
  0,                             /* tp_richcompare    */
  0,                             /* tp_weaklistoffset */
  DiscoDB_iter,                  /* tp_iter           */
  0,                             /* tp_iternext       */
  DiscoDB_methods,               /* tp_methods        */
  DiscoDB_members,               /* tp_members        */
  0,                             /* tp_getset         */
  0,                             /* tp_base           */
  0,                             /* tp_dict           */
  0,                             /* tp_descr_get      */
  0,                             /* tp_descr_set      */
  0,                             /* tp_dictoffset     */
  0,                             /* tp_init           */
  0,                             /* tp_alloc          */
  DiscoDB_new,                   /* tp_new            */
  0,                             /* tp_free           */
};

#pragma mark General Object Protocol

static void
DiscoDB_dealloc(DiscoDB *self)
{
  Py_CLEAR(self->obuffer);
  free(self->cbuffer);
  ddb_free(self->discodb);
  self->ob_type->tp_free((PyObject *)self);
}

static PyObject *
DiscoDB_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  DiscoDB *self = (DiscoDB *)type->tp_alloc(type, 0);
  PyObject
    *arg = NULL,
    *item = NULL,
    *items = NULL,
    *iteritems = NULL,
    *itervalues = NULL,
    *vpack = NULL,
    *value = NULL,
    *values = NULL,
    *valueseq = NULL;
  struct ddb_cons *ddb_cons;
  struct ddb_entry
    *kentry = NULL,
    *ventries = NULL;
  uint64_t n;


  if (self != NULL) {
    if (!PyArg_ParseTuple(args, "|O", &arg))
      goto Done;

    if (arg == NULL)                /* null constructor */
      items = PyTuple_New(0);
    else if (PyMapping_Check(arg))  /* copy constructor */
      items = PyMapping_Items(arg);
    else                            /* iter constructor */
      Py_INCREF(items = arg);

    iteritems = PyObject_GetIter(items);
    if (iteritems == NULL)
      goto Done;
    /* Ignores `kwds`, but could chain them to `iteritems`. */

    ddb_cons = ddb_cons_alloc();
    if (!ddb_cons)
      goto Done;

    while ((item = PyIter_Next(iteritems))) {
      kentry = ddb_entry_alloc(1);
      if (!kentry)
        goto Done;

      if (!PyArg_ParseTuple(item, "s#O", &kentry->data, &kentry->length, &values))
        goto Done;

      Py_XINCREF(values);

      if (values == NULL)
        values = PyTuple_New(0);

      valueseq = PySequence_Fast(values, "Values could not be converted to a sequence.");
      if (valueseq == NULL)
        goto Done;

      itervalues = PyObject_GetIter(valueseq);
      if (itervalues == NULL)
        goto Done;

      ventries = ddb_entry_alloc(PySequence_Length(valueseq));
      if (!ventries)
        goto Done;

      for (n = 0; (value = PyIter_Next(itervalues)); n++) {
        vpack = Py_BuildValue("(O)", value);
        if (vpack == NULL)
          goto Done;

        if (!PyArg_ParseTuple(vpack, "s#", &ventries[n].data, &ventries[n].length))
          goto Done;

        Py_CLEAR(vpack);
        Py_CLEAR(value);
      }

      ddb_add(ddb_cons, kentry, ventries, n);

      Py_CLEAR(itervalues);
      Py_CLEAR(item);
      Py_CLEAR(values);
      Py_CLEAR(valueseq);
      DiscoDB_CLEAR(kentry);
      DiscoDB_CLEAR(ventries);
    }
  }

  self->obuffer = NULL;
  self->cbuffer = ddb_finalize(ddb_cons, &n);
  self->discodb = ddb_loads(self->cbuffer, n);

 Done:
  Py_CLEAR(item);
  Py_CLEAR(items);
  Py_CLEAR(iteritems);
  Py_CLEAR(itervalues);
  Py_CLEAR(vpack);
  Py_CLEAR(value);
  Py_CLEAR(values);
  Py_CLEAR(valueseq);
  DiscoDB_CLEAR(kentry);
  DiscoDB_CLEAR(ventries);

  if (PyErr_Occurred()) {
    Py_CLEAR(self);
    return NULL;
  }
  return (PyObject *)self;
}

static PyObject *
DiscoDB_repr(DiscoDB *self)
{
  return NULL;
}

#pragma mark Mapping Formal / Informal Protocol

static PyObject *
DiscoDB_copy(PyTypeObject *self)
{
  return NULL;
}

static int
DiscoDB_contains(register DiscoDB *self, register PyObject *key)
{
  return 0;
}

static Py_ssize_t
DiscoDB_length(DiscoDB *self)
{
  return 0;
}

static PyObject *
DiscoDB_getitem(register DiscoDB *self, register PyObject *key)
{
  return NULL;
}

static PyObject *
DiscoDB_iter(PyObject *self)
{
  return NULL;
}

static PyObject *
DiscoDB_items(DiscoDB *self)
{
  return NULL;
}

static PyObject *
DiscoDB_keys(DiscoDB *self)
{
  return NULL;
}

static PyObject *
DiscoDB_values(DiscoDB *self)
{
  return NULL;
}

static PyObject *
DiscoDB_query(register DiscoDB *self, PyObject *query)
{
  return NULL;
}

#pragma mark Serialization / Deserialization Informal Protocol

static PyObject *
DiscoDB_dumps(DiscoDB *self)
{
  return NULL;
}

static PyObject *
DiscoDB_dump(DiscoDB *self, PyObject *file)
{
  return NULL;
}

static PyObject *
DiscoDB_loads(PyObject *bytes)
{
  return NULL;
}

static PyObject *
DiscoDB_load(PyObject *file)
{
  return NULL;
}

#pragma mark Module Initialization

PyMODINIT_FUNC
init_discodb(void)
{
  PyObject *module = Py_InitModule("_discodb", discodb_methods);

  if (PyType_Ready(&DiscoDBType) < 0)
    return;
  Py_INCREF(&DiscoDBType);
  PyModule_AddObject(module, "DiscoDB", (PyObject *)&DiscoDBType);
}

#pragma mark ddb helpers

static struct ddb_cons *
ddb_cons_alloc(void)
{
  struct ddb_cons *cons = ddb_new();
  if (!cons)
    PyErr_NoMemory();
  return cons;
}

static struct ddb_entry *
ddb_entry_alloc(size_t count)
{
  struct ddb_entry *entry = (struct ddb_entry *)calloc(count, sizeof(struct ddb_entry));
  if (!entry)
    PyErr_NoMemory();
  return entry;
}
