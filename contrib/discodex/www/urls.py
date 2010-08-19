from django.conf.urls.defaults import *

import views

indices        = r'indices/?'
index          = r'%s/(?P<name>[-A-Za-z0-9_@:]+)/?' % (indices)
attribute      = r'%s/(?P<method>\w+)'              % (index)
method         = r'%s(?:/(?P<arg>[^|}\]]*))?'       % (attribute)

dotted_name    = r'\w+(\.\w+)*'
mapfilters     = r'(?P<mapfilters>(\|%s)*)'    % (dotted_name)
reducefilters  = r'(?P<reducefilters>(}%s)*)'  % (dotted_name)
resultsfilters = r'(?P<resultsfilters>(]%s)*)' % (dotted_name)

def pipeline(pattern):
    return r'^%s%s%s%s$' % (pattern, mapfilters, reducefilters, resultsfilters)

urlpatterns = patterns('',
                       url(r'^%s$' % indices, views.indices, name='indices'),
                       url(r'^%s$' % index,   views.indices, name='index'),
                       url(pipeline(method),  views.indices, name='method'),
                       url(r'^/?$',           views.home,    name='home'),
)
