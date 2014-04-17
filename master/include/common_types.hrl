-type host() :: nonempty_string().
-type path() :: nonempty_string().
-type url() :: binary().

% The host portion of a url, if available.
-type url_host() :: host() | none.

-type seq_id() :: non_neg_integer().

-ifdef(namespaced_types).
-type disco_dict(K,V) :: dict:dict(K,V).
-type disco_gbtree(K, V) :: gb_trees:tree(K, V).
-type disco_gbset(K) :: gb_sets:set(K).
-type disco_queue(K) :: queue:queue(K).
-else.
-type disco_dict(_K,_V) :: dict().
-type disco_gbtree(_K,_V) :: gb_tree().
-type disco_gbset(_K) :: gb_set().
-type disco_queue(_K) :: queue().
-endif.
