from django.conf.urls.defaults import *

import views

indices        = r'indices/?'
index          = r'%s/(?P<name>[-A-Za-z0-9_@:]+)/?'     % (indices)
target         = r'%s(?:/(?P<target>metadb|datadb)/?)?' % (index)
keys           = r'%s/(?P<property>keys)/?'             % (target)
values         = r'%s/(?P<property>values)/?'           % (target)
items          = r'%s/(?P<property>items)/?'            % (target)
_query         = r'%s/(?P<property>query)/?'            % (target)
query          = r'%s/(?P<query_path>[^|}\]]*)'         % (_query)

name           = r'\w+'
dotted_name    = r'%s(\.%s)*'                  % (name, name)
maybe_curry    = r'%s(:[^|}\]]*)?'             % (dotted_name)
mapfilters     = r'(?P<mapfilters>(\|%s)*)'    % (maybe_curry)
reducefilters  = r'(?P<reducefilters>(}%s)*)'  % (maybe_curry)
resultsfilters = r'(?P<resultsfilters>(]%s)*)' % (maybe_curry)

def pipeline(pattern):
    return r'^%s%s%s%s$' % (pattern, mapfilters, reducefilters, resultsfilters)

urlpatterns = patterns('',
                       url(r'^%s$' % indices, views.indices, name='indices'),
                       url(r'^%s$' % index,   views.indices, name='index'),
                       url(pipeline(keys),    views.indices, name='keys'),
                       url(pipeline(values),  views.indices, name='values'),
                       url(pipeline(items),   views.indices, name='items'),
                       url(pipeline(query),   views.indices, name='query'),
                       url(r'^/?$',           views.home,    name='home'),
)
