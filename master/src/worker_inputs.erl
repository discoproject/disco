-module(worker_inputs).
-export([init/1,
         all/1,
         include/2,
         exclude/2,
         fail/3,
         add/2,
         add/3]).

-export_type([state/0, worker_input/0]).

-record(input, {url :: binary(),
                failed :: erlang:timestamp()}).
-type input() :: #input{}.
-type input_id() :: non_neg_integer().
-type replica_id() :: non_neg_integer().
-type replica() :: {{input_id(), replica_id()}, input()}.
-type state() :: {gb_tree(), non_neg_integer()}.

-type labeled_rep() :: [replica_id() | binary(), ...].
-type worker_input() :: {input_id(), [labeled_rep()]}.

-spec init(binary() | [binary()] | [[binary()]]) -> state().
init(Url) when is_binary(Url) ->
    init([Url]);
init(Inputs) when is_list(Inputs) ->
    Items = [init_replicaset(Iid, Urls) || {Iid, Urls} <- disco:enum(Inputs)],
    {gb_trees:from_orddict(lists:flatten(Items)), length(Inputs)}.

-spec init_replicaset(input_id(), binary() | [binary()]) ->
                             [replica()].
init_replicaset(Iid, Url) when is_binary(Url) ->
    init_replicaset(Iid, [Url]);
init_replicaset(Iid, Urls) when is_list(Urls) ->
    [{{Iid, Rid}, #input{url = Url, failed = {0, 0, 0}}}
        || {Rid, Url} <- disco:enum(Urls)].

-spec include([input_id()], state()) -> [worker_input()].
include(Iids, S) ->
    [{Iid, replicas(Iid, 0, [], S)} || Iid <- Iids].

-spec exclude([non_neg_integer()], state()) -> [worker_input()].
exclude(Exc, S) ->
    All = gb_sets:from_ordset(all_iids(S)),
    include(gb_sets:to_list(gb_sets:subtract(All, gb_sets:from_list(Exc))), S).

-spec all(state()) -> [worker_input()].
all(S) ->
    include(all_iids(S), S).

-spec replicas(input_id(), replica_id(), [{erlang:timestamp(), labeled_rep()}],
               state()) -> [labeled_rep()].
replicas(Iid, Rid, Replicas, {T, _MaxIid} = S) ->
    case gb_trees:lookup({Iid, Rid}, T) of
        none ->
            [Repl || {_, Repl} <- lists:sort(Replicas)];
        {value, R} ->
            L = [{R#input.failed, [Rid, R#input.url]}|Replicas],
            replicas(Iid, Rid + 1, L, S)
    end.

-spec fail(input_id(), [replica_id()], state()) -> state().
fail(Iid, Rids, S) ->
    Now = now(),
    lists:foldl(fun(Rid, S1) -> fail_one({Iid, Rid}, Now, S1) end, S, Rids).

fail_one(Key, Now, {T, MaxIid} = S) ->
    case gb_trees:lookup(Key, T) of
        {value, R} ->
            {gb_trees:update(Key, R#input{failed = Now}, T), MaxIid};
        none ->
            S
    end.

-type add_ret() :: 'invalid_iid' | {{input_id(), replica_id()}, state()}.

-spec add(binary(), state()) -> add_ret().
add(Url, {T, MaxIid}) ->
    add(MaxIid, Url, {T, MaxIid + 1}).

-spec add(non_neg_integer(), binary(), state()) -> add_ret().
add(Iid, _Url, {_T, MaxIid}) when Iid >= MaxIid ->
    invalid_iid;
add(Iid, Url, {T, MaxIid}) ->
    Item = #input{url = Url, failed = {0, 0, 0}},
    Rid = max_rid(Iid, 0, T),
    {{Iid, Rid}, {gb_trees:insert({Iid, Rid}, Item, T), MaxIid}}.

-spec max_rid(input_id(), replica_id(), gb_tree()) -> replica_id().
max_rid(Iid, Max, T) ->
    case gb_trees:lookup({Iid, Max}, T) of
        none ->
            Max;
        {value, _} ->
            max_rid(Iid, Max + 1, T)
    end.

-spec all_iids(state()) -> [input_id()].
all_iids({_T, MaxIid}) ->
    lists:seq(0, MaxIid - 1).
