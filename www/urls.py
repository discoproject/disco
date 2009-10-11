from django.conf.urls.defaults import *

import views

indices = r'indices/?'
index   = r'%s/(?P<name>[A-Za-z0-9_@:]+)/?'         % (indices)
keys    = r'%s/(?P<property>keys)/?'    % (index)
values  = r'%s/(?P<property>values)/?'  % (index)
_query  = r'%s/(?P<property>query)/?'   % (index)
query   = r'%s/(?P<query_path>.*)'      % (_query)

urlpatterns = patterns('',
                       url(r'^%s$' % indices, views.indices, name='indices'),
                       url(r'^%s$' % index,   views.indices, name='index'),
                       url(r'^%s$' % keys,    views.indices, name='keys'),
                       url(r'^%s$' % values,  views.indices, name='values'),
                       url(r'^%s$' % query,   views.indices, name='query'),
                       url(r'^/?$',           views.home,    name='home'),
)
