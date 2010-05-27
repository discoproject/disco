import os, sys
from django.core.handlers import wsgi

PROJ_ROOT = os.path.dirname(globals()["__file__"])
PROJ_NAME = os.path.basename(PROJ_ROOT)

sys.path.append(os.path.dirname(PROJ_ROOT))

os.environ['DJANGO_SETTINGS_MODULE'] = '%s.settings' % PROJ_NAME
application = wsgi.WSGIHandler()
