"""
:mod:`disco.worker.classic.modutil` -- Parse and find module dependencies
=========================================================================

.. versionadded:: 0.2.3

This module provides helper functions to be used with the ``required_modules``
parameter in :class:`~disco.worker.classic.worker.Worker`.
These functions are needed when
your job functions depend on external Python modules and the default value
for ``required_modules`` does not suffice.

By default, Disco tries to find out which modules are required by job functions
automatically. If the found modules are not included in the Python standard library
or other package that is installed system-wide, it sends them to nodes so they
can be used by the Disco worker process.

Sometimes Disco may fail to detect all required modules. In this case,
you can override the default value either by providing a list of requirements manually,
or by generating the list semi-automatically using the functions in this module.

.. _modspec:

How to specify required modules
-------------------------------

The ``required_modules`` parameter accepts a list of module definitions.
A module definition may be either a module name, e.g. ``"PIL.Image"``,
or a tuple that specifies both the module name and its path,
e.g. ``("mymodule", "lib/mymodule.py")``.
In the former case,
the :class:`disco.worker.classic.worker.Worker` only imports the module,
assuming that it has been previously installed to the node.
In the latter case,
Disco sends the module file to nodes before importing it,
and no pre-installation is required.

For instance, the following is a valid list for
``required_modules``::

        required_modules = ["math", "random", ("mymodule", "lib/mymodule.py")]

This expression imports the standard modules ``math`` and ``random`` and sends
a custom module ``lib/mymodule.py`` to nodes before importing it.

Note that Disco sends only the files that can be found in your `PYTHONPATH`. It
is assumed that files outside ``PYTHONPATH`` belong either to the Python standard
library or to another package that is installed system-wide. Make sure
that all modules that require automatic distribution can be found in your `PYTHONPATH`.

Automatic distribution works only for individual modules and not
for packages nor modules that require a specific directory hierarchy. You need
to install packages and modules with special requirements manually to your nodes.

Typical use cases
-----------------

The following list describes some typical use cases for ``required_modules``.
The list helps you decide when to use the :func:`find_modules` and
:func:`locate_modules` functions.

 - (**default**) If you want to find and send all required modules used by
   your job functions recursively (i.e. also modules that depend on other modules are included),
   you don't need to specify ``required_modules`` at all. This equals to::

        required_modules = modutil.find_modules(job_functions)

   where *job_functions* is a list of all job functions: *map*, *map_init*,
   *combiner* etc.

 - If you want to find and import all required modules, but not send them, or
   you want to disable recursive analysis, use :func:`find_modules`
   explicitly with the ``send_modules`` and ``recursive`` parameters.

 - If you want to send a known set of modules (possible recursively) but you
   don't know their paths, use :func:`locate_modules`.

 - If you want to send a known set of modules. provide a list of *(module name,
   module path)* tuples.

 - If you just want to import specific modules, or sub-modules in a
   pre-installed package (e.g. ``PIL.Image``), provide a list of module names.

Any combinations of the above are allowed. For instance::

        required_modules = find_modules([fun_map]) + [("mymodule", "/tmp/mymodule.py"), "time", "random"]

is a valid expression.

Functions
---------
"""
import re, struct, sys, os, imp, modulefinder
import functools
from os.path import abspath, dirname
from opcode import opname

from disco.error import DiscoError

class ModUtilImportError(DiscoError, ImportError):
    """Error raised when a module can't be found by :mod:`disco.worker.classic.modutil`."""
    def __init__(self, error, function):
        self.error    = error
        self.function = function

    def __str__(self):
        # XXX! Add module name below
        return ("%s: Could not find module defined in %s. Maybe it is a typo. "
                "See documentation of the required_modules parameter for details "
                "on how to include modules." % (self.error, self.function.func_name))

def user_paths():
    return set([os.path.abspath(path)
                for path in os.getenv('PYTHONPATH', '').split(':')] + [''])

def parse_function(function):
    """
    Tries to guess which modules are used by *function*.
    Returns a list of module names.

    This function is used by :func:`find_modules` to parse modules used by a
    function. You can use it to check that all modules used by your functions are
    detected correctly.

    The current heuristic requires that modules are accessed using the dot
    notation directly, e.g. ``random.uniform(1, 10)``. For instance, required
    modules are not detected correctly in the following snippet::

        a = random
        a.uniform(1, 10)

    Also, modules used in generator expressions, like here::

        return ((k, base64.encodestring(v)) for k, v in d.iteritems())

    are not detected correctly.
    """
    if isinstance(function, functools.partial):
        return parse_function(function.func)
    code = function.func_code
    mod = re.compile(r'\x%.2x(..)\x%.2x' % (opname.index('LOAD_GLOBAL'),
                        opname.index('LOAD_ATTR')), re.DOTALL)
    return [code.co_names[struct.unpack('<H', x)[0]] for x in mod.findall(code.co_code)]

def recurse_module(module, path):
    finder = modulefinder.ModuleFinder(path=list(user_paths()))
    finder.run_script(path)
    return dict((name, os.path.realpath(module.__file__)) for name, module in finder.modules.iteritems()
                if name != '__main__' and module.__file__)

def locate_modules(modules, recurse=True, include_sys=False):
    """
    Finds module files corresponding to the module names specified in the list *modules*.

    :param modules: The modules to search for other required modules.

    :param recurse: If ``True``, recursively search for local modules
                    that are used in *modules*.

    A module is local if it can be found in your ``PYTHONPATH``. For modules that
    can be found under system-wide default paths (e.g. ``/usr/lib/python``), just
    the module name is returned without the corresponding path, so system-wide
    modules are not distributed to nodes unnecessarily.

    This function is used by :func:`find_modules` to locate modules used by
    the specified functions.
    """
    LOCALDIRS = user_paths()
    found = {}

    for module in modules:
        file, path, x = imp.find_module(module)
        path = os.path.realpath(path)

        if dirname(path) in LOCALDIRS and os.path.isfile(path):
            found[module] = path
            if recurse:
                found.update(recurse_module(module, path))
        elif include_sys:
            found[module] = None
    return found.items()


def find_modules(functions, send_modules=True, recurse=True, exclude=()):
    """
    Tries to guess and locate modules that are used by *functions*.
    Returns a list of required modules as specified in :ref:`modspec`.

    :param functions: The functions to search for required modules.

    :param send_modules: If ``True``, a ``(module name, module path)`` tuple is
                         returned for each required local module.
                         If ``False``, only the module name is returned and
                         detected modules are not sent to nodes;
                         this implies ``recurse=False``.

    :param recurse: If ``True``, this function includes all modules that
                    are required by *functions* or any other included modules.
                    In other words, it tries to ensure that all module files
                    required by the job are included.
                    If ``False``, only modules that are directly used by
                    *functions* are included.
    """
    modules = set()
    for function in functions:
        fmod = set([m for m in parse_function(function) if m not in exclude])
        if send_modules:
            try:
                m = locate_modules(fmod, recurse, include_sys=True)
            except ImportError, e:
                raise ModUtilImportError(e, function)
            modules.update((k, v) if v else k for k, v in m)
        else:
            modules.update(fmod)
    return list(modules)
