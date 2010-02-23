"""
/indices                               -> index collection
        /[index]                       -> index resource
                /keys                  -> index keys as values
                /values                -> index values as values
                /items                 -> index items as key, values
                /query/[query]         -> index values with keys satisfying query as values

TODO:
                /keys/[metakey]        -> index keys with metakey as values
                /keys/[metakey]/values -> values of keys with metakey as k, v

ichunks    -> /indices/[index]
metachunks -> /indices/[metaindex]
                metaindex is normal index, but ichunks are metadb:// not discodb://

/indices/[metaindex]
                        /metadb         -> normal discodb (metadb.metadb)
                        /datadb         -> normal discodb (metadb.datadb)
                        /keys           -> metadb keys as values
                        /values         -> metadb values as key, values
                        /items          -> metadb metakeys, values as key, values
                        /query/[query]  -> metadb values as key, values
                        /query/startingwith:host:/ -> {'host:X': ..., 'host:Y': ...}
"""
from django.conf.urls.defaults import *

import views

indices       = r'indices/?'
index         = r'%s/(?P<name>[A-Za-z0-9_@:]+)/?'      % (indices)
target        = r'%s(?:/(?P<target>metadb|datadb)/?)?' % (index)
keys          = r'%s/(?P<property>keys)/?'             % (target)
values        = r'%s/(?P<property>values)/?'           % (target)
items         = r'%s/(?P<property>items)/?'            % (target)
_query        = r'%s/(?P<property>query)/?'            % (target)
query         = r'%s/(?P<query_path>[^|}]*)'           % (_query)

name          = r'\w+'
dotted_name   = r'%s(\.%s)*'    % (name, name)
maybe_curry   = r'%s(:[^|}]*)?' % (dotted_name)
mapfilters    = r'(?P<mapfilters>(\|%s)*)'   % (maybe_curry)
reducefilters = r'(?P<reducefilters>(}%s)*)' % (maybe_curry)

def pipeline(pattern):
    return r'^%s%s%s$' % (pattern, mapfilters, reducefilters)

urlpatterns = patterns('',
                       url(r'^%s$' % indices, views.indices, name='indices'),
                       url(r'^%s$' % index,   views.indices, name='index'),
                       url(pipeline(keys),    views.indices, name='keys'),
                       url(pipeline(values),  views.indices, name='values'),
                       url(pipeline(items),   views.indices, name='items'),
                       url(pipeline(query),   views.indices, name='query'),
                       url(r'^/?$',           views.home,    name='home'),
)
