from django.conf.urls.defaults import *

indices        = r'indices/?'
index          = r'%s/(?P<name>[-A-Za-z0-9_@:]+)/?' % (indices)
attribute      = r'%s/(?P<method>\w+)'              % (index)
method         = r'%s(?:/(?P<arg>[^|}\]]*))?'       % (attribute)

dotted_name    = r'\w+(\.\w+)*'
streams        = r'(?P<streams>(\|%s)*)' % (dotted_name)
reduce         = r'(?P<reduce>(}%s)?)'   % (dotted_name)

def pipeline(pattern):
    return r'^%s%s%s$' % (pattern, streams, reduce)

urlpatterns = patterns('discodex.views',
                       url(r'%s$' % indices,  'Indices', name='indices'),
                       url(pipeline(index),   'Indices', name='index'),
                       url(pipeline(method),  'Indices', name='method'),
                       url(r'^/?$',           'Indices', name='home'),
)
