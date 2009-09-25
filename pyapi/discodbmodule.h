typedef struct {
  PyObject_VAR_HEAD
  // ddb_t *discodb;
} DiscoDB;

#pragma mark Constructor / Destructor

static void       DiscoDB_dealloc (DiscoDB *);
static PyObject * DiscoDB_new     (PyTypeObject *, PyObject *, PyObject *);

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
static PyObject * DiscoDB_loads   (PyObject *);
static PyObject * DiscoDB_load    (PyObject *);
