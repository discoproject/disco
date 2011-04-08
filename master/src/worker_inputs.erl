-module(worker_inputs).
-export([init/1,
         all/1,
         include/2,
         exclude/2,
         fail/2,
         fail/3,
         add/2,
         add/3]).

-record(input, {url :: binary(),
                failed}).

init(Url) when is_binary(Url) ->
    init([Url]);

init(Inputs) when is_list(Inputs) ->
    Items = [init_replicaset(Iid, Urls) || {Iid, Urls} <- disco:enum(Inputs)],
    {gb_trees:from_orddict(lists:flatten(Items)), length(Inputs)}.

init_replicaset(Iid, Url) when is_binary(Url) ->
    init_replicaset(Iid, [Url]);

init_replicaset(Iid, Urls) when is_list(Urls) ->
    [{{Iid, Rid}, #input{url = Url, failed = {0, 0, 0}}}
        || {Rid, Url} <- disco:enum(Urls)].

include(Iids, S) ->
    [{Iid, replicas(Iid, 0, [], S)} || Iid <- Iids].

exclude(Exc, S) ->
    All = gb_sets:from_ordset(all_iids(S)),
    include(gb_sets:to_list(gb_sets:subtract(All, gb_sets:from_list(Exc))), S).

all(S) ->
    include(all_iids(S), S).

replicas(Iid, Rid, Replicas, {T, _MaxIid} = S) ->
    case gb_trees:lookup({Iid, Rid}, T) of
        none ->
            [Repl || {_, Repl} <- lists:sort(Replicas)];
        {value, R} ->
            L = [{R#input.failed, [Rid, R#input.url]}|Replicas],
            replicas(Iid, Rid + 1, L, S)
    end.

fail(Iid, Rids, S) ->
    lists:foldl(fun(Rid, S1) -> fail({Iid, Rid}, S1) end, S, Rids).

fail(Key, {T, MaxIid} = S) ->
    case gb_trees:lookup(Key, T) of
        {value, R} ->
            {gb_trees:update(Key, R#input{failed = now()}, T), MaxIid};
        none ->
            S
    end.

add(Url, {T, MaxIid}) ->
    add(MaxIid, Url, {T, MaxIid + 1}).

add(Iid, _Url, {_T, MaxIid}) when Iid >= MaxIid ->
    invalid_iid;

add(Iid, Url, {T, MaxIid}) ->
    Item = #input{url = Url, failed = {0, 0, 0}},
    Rid = max_rid(Iid, 0, T),
    {{Iid, Rid}, {gb_trees:insert({Iid, Rid}, Item, T), MaxIid}}.

max_rid(Iid, Max, T) ->
    case gb_trees:lookup({Iid, Max}, T) of
        none ->
            Max;
        {value, _} ->
            max_rid(Iid, Max + 1, T)
    end.

all_iids({_T, MaxIid}) ->
    lists:seq(0, MaxIid - 1).
