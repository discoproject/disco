-module(worker_inputs).

-export([init/3, all/1, include/2, exclude/2, fail/3, failed_info/2]).
-export_type([state/0, worker_input/0]).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-record(state, {
          % seq_id() -> {input_id(), data_input()}
          inputs     = gb_trees:empty() :: gb_tree(),
          % {seq_id(), rep_id()} -> fail_info()
          input_map  = gb_trees:empty() :: gb_tree(),
          max_seq_id                    :: seq_id()}).
-type state() :: #state{}.

-record(fail_info, {url       :: url(),
                    last_fail :: erlang:timestamp()}).
-type fail_info() :: #fail_info{}.

-type seq_id() :: non_neg_integer().
-type rep_id() :: non_neg_integer().

-type replica() :: [rep_id() | url(), ...].
-type worker_input() :: {seq_id(), label(), [replica()]}.

-spec init([{input_id(), data_input()}], label_grouping(), group()) -> state().
init(Inputs, Grouping, Group) ->
    SeqInputs = disco:enum(Inputs),
    SeqMap = lists:flatten([init_replicas(SeqId, DI, Grouping, Group)
                            || {SeqId, {_Id, DI}} <- SeqInputs]),
    #state{inputs     = gb_trees:from_orddict(SeqInputs),
           input_map  = gb_trees:from_orddict(SeqMap),
           max_seq_id = length(Inputs)}.

-spec init_replicas(seq_id(), data_input(), label_grouping(), group())
                   -> [{{seq_id(), rep_id()}, {all | label(), fail_info()}}].
init_replicas(SeqId, DI, Grouping, Group) ->
    {L, Urls} = pipeline_utils:input_urls(DI, Grouping, Group),
    [{{SeqId, RepId}, {L, #fail_info{url = Url, last_fail = {0, 0, 0}}}}
     || {RepId, Url} <- disco:enum(Urls)].

-spec include([seq_id()], state()) -> [worker_input()].
include(SeqIds, #state{input_map = Map}) ->
    [{SeqId, L, Reps} || SeqId <- SeqIds,
                         {L, Reps} <- [replicas(SeqId, 0, 0, [], Map)]].

-spec exclude([seq_id()], state()) -> [worker_input()].
exclude(Exc, S) ->
    All = gb_sets:from_ordset(all_seq_ids(S)),
    include(gb_sets:to_list(gb_sets:subtract(All, gb_sets:from_list(Exc))), S).

-spec all(state()) -> [worker_input()].
all(S) ->
    include(all_seq_ids(S), S).

-spec replicas(seq_id(), rep_id(), label(), [{erlang:timestamp(), replica()}],
               gb_tree()) -> {all | label(), [replica()]}.
replicas(SeqId, Rid, Label, Replicas, Map) ->
    case gb_trees:lookup({SeqId, Rid}, Map) of
        none ->
            {Label, [Repl || {_, Repl} <- lists:sort(Replicas)]};
        {value, {L, #fail_info{url = Url, last_fail = LastFail}}} ->
            Reps = [{LastFail, [Rid, Url]}|Replicas],
            replicas(SeqId, Rid + 1, L, Reps, Map)
    end.

-spec fail(seq_id(), [rep_id()], state()) -> state().
fail(SeqId, Rids, S) ->
    Now = now(),
    lists:foldl(fun(Rid, S1) -> fail_one({SeqId, Rid}, Now, S1) end, S, Rids).

fail_one(Key, Now, #state{input_map = Map} = S) ->
    case gb_trees:lookup(Key, Map) of
        {value, F} ->
            Map1 = gb_trees:update(Key, F#fail_info{last_fail = Now}, Map),
            S#state{input_map = Map1};
        none ->
            S
    end.

-spec all_seq_ids(state()) -> [seq_id()].
all_seq_ids(#state{max_seq_id = MaxSeqId}) ->
    lists:seq(0, MaxSeqId - 1).

-spec failed_info([seq_id()], state()) -> [{input_id(), [host()]}].
failed_info(SeqIds, #state{inputs = Inputs}) ->
    [make_info(gb_trees:get(SeqId, Inputs))
     || SeqId <- SeqIds, gb_trees:is_defined(SeqId, Inputs)].

make_info({Iid, DI}) -> {Iid, pipeline_utils:locations(DI)}.
