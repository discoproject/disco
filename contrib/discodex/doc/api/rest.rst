.. role:: resource(strong)

discodex ReST API
=================

        :resource:`/indices`
                index collection

                :resource:`/[indexname]`
                        index resource

                        :resource:`/keys`
                                index keys as values

                        :resource:`/values`
                                index values as values

                        :resource:`/items`
                                index items as key, values

                        :resource:`/query/[query]`
                                index values with keys satisfying query as values

                :resource:`/[metaindexname]`
                        a metaindex is like a normal index, but ichunks are metadb:// not discodb://
                        there are also 2 additional targets, which have the same API as a normal index

                        :resource:`/metadb`
                                normal discodb (metadb.metadb)

                        :resource:`/datadb`
                                normal discodb (metadb.datadb)

                        :resource:`/keys`
                                metadb keys as values

                        :resource:`/values`
                                metadb values as key, values

                        :resource:`/items`
                                metadb metakeys, values as key, values

                        :resource:`/query/[query]`
                                metadb values as key, values
                                e.g. /query/host:/ -> {'host:X': ..., 'host:Y': ...}


`query` resources always look like their associated `values` resources.

pipelines
---------

/keys
'', k1
'', k2

/values
'', v1
'', v2

/values|kvify|kvswap
v1, ''
v2, ''

/values}count
'', n

/values|kvify|kvswap}count
v1, n1
v2, n2

/indices/[metaindex]/items
metakey, ((key, vs), ...)
