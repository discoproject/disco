# -*- coding: utf-8 -*-
#
# Disco documentation build configuration file, created by
# sphinx-quickstart on Sun Jul  6 17:23:01 2008.
#
# This file is execfile()d with the current directory set to its containing dir.
#
# The contents of this file are pickled, so don't put values in the namespace
# that aren't pickleable (module imports are okay, they're removed automatically).
#
# All configuration values have a default value; values that are commented out
# serve to show the default value.

import sys, os

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.insert(0, os.path.abspath('../bin'))
sys.path.insert(0, os.path.abspath('../lib'))

# Python 3.3 has mock; use it.
try: import mock
except ImportError:
    sys.path.insert(0, os.path.abspath('.'))
    import mock

# General configuration
# ---------------------

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.doctest', 'sphinx.ext.todo', 'sphinx.ext.intersphinx']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['.templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General substitutions.
project = 'Disco'
copyright = '2008-2013, Nokia Corporation. 2008-2014, The Disco Project.'

if on_rtd:
    MOCK_MODULES = ['discodb']
    for mod_name in MOCK_MODULES:
        sys.modules[mod_name] = mock.Mock()
    discodb_docurl = 'http://discodb.readthedocs.org/en/latest'
else:
    discodb_docurl = 'http://discoproject.org/doc/discodb'
# The default replacements for |version| and |release|, also used in various
# other places throughout the built documents.
#
# The short X.Y version.
    version = '%DISCO_VERSION%'
# The full version, including alpha/beta/rc tags.
    release = '%DISCO_RELEASE%'

intersphinx_mapping = {'discodb':  (discodb_docurl, None),
                       'discodex': ('http://discoproject.org/doc/discodex', None)}

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
today_fmt = '%B %d, %Y'

# List of documents that shouldn't be included in the build.
#unused_docs = []

# List of directories, relative to source directories, that shouldn't be searched
# for source files.
#exclude_dirs = []

# If true, '()' will be appended to :func: etc. cross-reference text.
#add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'


# Options for HTML output
# -----------------------

# The style sheet to use for HTML and HTML Help pages. A file of that name
# must exist either in Sphinx' static/ path, or in one of the custom paths
# given in html_static_path.
html_style = 'sphinxdoc.css'

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
#html_title = None

# The name of an image file (within the static path) to place at the top of
# the sidebar.
html_logo = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['.static']

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
#html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
#html_additional_pages = {}

# If false, no module index is generated.
#html_use_modindex = True

# If true, the reST sources are included in the HTML build as _sources/<name>.
#html_copy_source = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#html_use_opensearch = ''

# If nonempty, this is the file name suffix for HTML files (e.g. ".xhtml").
#html_file_suffix = ''

# Output file base name for HTML help builder.
htmlhelp_basename = 'Discodoc'


# Options for LaTeX output
# ------------------------

# The paper size ('letter' or 'a4').
#latex_paper_size = 'letter'

# The font size ('10pt', '11pt' or '12pt').
#latex_font_size = '10pt'

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, document class [howto/manual]).
latex_documents = [
  ('index', 'Disco.tex', 'Disco Documentation', 'Disco Project', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#latex_use_parts = False

# Additional stuff for the LaTeX preamble.
#latex_preamble = ''

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_use_modindex = True

def setup(app):
    import re
    repr_re = re.compile(r'<([\w\s]+) at 0x\w+>')
    def process_signature(app, what, name, obj, opts, signature, return_annotation):
        if signature:
            signature = repr_re.sub(r'\1', signature)
        return signature, return_annotation
    def skip_member(app, what, name, obj, skip, options):
        if skip:
            return True
        if what != 'module' and type(obj) is type:
            return True
    app.connect('autodoc-process-signature', process_signature)
    app.connect('autodoc-skip-member', skip_member)
