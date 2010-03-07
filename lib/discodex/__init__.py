try:
    import json
except ImportError:
    try:
        from django.utils import simplejson as json
    except ImportError:
        from disco.comm import json
