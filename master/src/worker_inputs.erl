-module(worker_inputs).

-export([init/4, all/1, include/2, exclude/2, fail/3,
         is_input_done/1, add_inputs/2]).
-export_type([state/0, worker_input/0]).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-record(fail_info, {url       :: url(),
                    last_fail :: erlang:timestamp()}).
-type fail_info() :: #fail_info{}.

-record(state, {
          inputs     = gb_trees:empty() :: disco_gbtree(seq_id(), {input_id(), data_input()}),
          input_map  = gb_trees:empty() :: disco_gbtree({seq_id(), rep_id()}, fail_info()),
          is_input_done                 :: boolean(),
          stage_grouping                :: label_grouping(),
          stage_group                   :: group(),
          max_seq_id                    :: seq_id()}).
-type state() :: #state{}.

-type rep_id() :: non_neg_integer().

-type replica() :: [rep_id() | url(), ...].
-type worker_input() :: {seq_id(), label(), [replica()]}.

-spec init([{input_id(), data_input()}], label_grouping(), group(), boolean()) -> state().
init(Inputs, Grouping, Group, AllInputs) ->
    SeqInputs = disco:enum(Inputs),
    SeqMap = lists:flatten([init_replicas(SeqId, DI, Grouping, Group)
                            || {SeqId, {_Id, DI}} <- SeqInputs]),
    #state{inputs     = gb_trees:from_orddict(SeqInputs),
           input_map  = gb_trees:from_orddict(SeqMap),
           is_input_done = AllInputs,
           stage_grouping = Grouping,
           stage_group = Group,
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

-spec is_input_done(state()) -> boolean().
is_input_done(#state{is_input_done = Done}) ->
    Done.

-spec exclude([seq_id()], state()) -> [worker_input()].
exclude(Exc, S) ->
    All = gb_sets:from_ordset(all_seq_ids(S)),
    include(gb_sets:to_list(gb_sets:subtract(All, gb_sets:from_list(Exc))), S).

-spec all(state()) -> [worker_input()].
all(S) ->
    include(all_seq_ids(S), S).

-spec replicas(seq_id(), rep_id(), label(), [{erlang:timestamp(), replica()}],
    disco_gbtree({seq_id(), rep_id()}, fail_info())) -> {all | label(), [replica()]}.
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

-spec add_inputs(done | [{input_id(), data_input()}], state()) -> state().
add_inputs(done, S) ->
    S#state{is_input_done = true};
add_inputs([], S) ->
    S;

%TODO This function can be made much faster.
add_inputs([NewInput|Rest], #state{inputs = Inputs,
                                   max_seq_id = MaxSeqId,
                                   stage_grouping = Grouping,
                                   stage_group = Group,
                                   input_map = Map} = S) ->
    Contains = lists:foldl(fun({_, Inp}, T) ->
                               case T of
                                   true -> true;
                                   false ->
                                       case Inp of
                                           NewInput -> true;
                                           _        -> false
                                       end
                               end
                           end, false, gb_trees:to_list(Inputs)),
   case Contains of
       true ->
           add_inputs(Rest, S);
       false ->
        {_Id, DI} = NewInput,
        Map1 = lists:foldl(fun({K, V}, IMap) -> gb_trees:enter(K, V, IMap) end,
                           Map, init_replicas(MaxSeqId, DI, Grouping, Group)),
        add_inputs(Rest,
            S#state{inputs = gb_trees:enter(MaxSeqId, NewInput, Inputs),
                    max_seq_id = MaxSeqId + 1,
                    input_map = Map1})
    end.

-spec all_seq_ids(state()) -> [seq_id()].
all_seq_ids(#state{max_seq_id = MaxSeqId}) ->
    lists:seq(0, MaxSeqId - 1).
