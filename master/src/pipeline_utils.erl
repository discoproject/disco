-module(pipeline_utils).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("job_coordinator.hrl").

-export([stages/1, next_stage/2, group_outputs/2, pick_local_host/1,
         all_deps_finished/3, get_grouping_lists/4]).
-export([locations/1, ranked_locations/1]).
-export([input_urls/3, output_urls/1]).

% Pipeline utilities.

-spec stages(pipeline()) -> [stage_name()].
stages(Pipeline) -> [Stage || {Stage, _G, _} <- Pipeline].

-spec locations(data_input()) -> [host()].
locations({data, {_Label, _Size, Replicas}}) ->
    [H || {_Url, H} <- Replicas, H =/= none];
locations({dir, {Host, _Url, _Labels}}) ->
    [Host].

% We don't currently rank by label in dir inputs.
-spec ranked_locations(data_input()) -> [{data_size(), host()}].
ranked_locations({data, {_Label, Size, Replicas}}) ->
    [{Size, H} || {_Url, H} <- Replicas, H =/= none];
ranked_locations({dir, {Host, _Url, Labels}}) ->
    Size = lists:sum([Sz || {_L, Sz} <- Labels]),
    [{Size, Host}].

% When a task needs to consume node-local data, but a specific
% data-unit has replicas, then we need to pick ad-hoc _a_ node to
% which that data-unit is "local".  This just means that sometimes,
% due to faults, a task could be re-run on another host, in which case
% it would fetch its "local" input from a remote node.
-spec pick_local_host([data_replica()]) -> url_host().
pick_local_host(Replicas) ->
    Reps = [Host || {_Url, Host} <- Replicas, Host =/= none],
    case Reps of
        [] -> none;
        _ -> disco_util:choose_random(Reps)
    end.

-spec next_stage(pipeline(), stage_name()) -> stage() | done.
next_stage([], _S) -> done;
next_stage([{_S, _G, _} = First | _], ?INPUT) -> First;
next_stage([{S, _G, _}], S) -> done;
next_stage([{S, _G, _}, Next|_], S) -> Next;
next_stage([_S|Rest], S) -> next_stage(Rest, S).

-spec all_deps_finished(pipeline(), stage_name(),
                        disco_gbtree(stage_name(), stage_info())) -> boolean().
all_deps_finished([], _S, _) -> true;
all_deps_finished([_|_], ?INPUT, _) -> true;
all_deps_finished([{S, _, _}|_], S, _) -> true;
all_deps_finished([{DepS,_, _}|Rest], S, SI) ->
    StageInfo = jc_utils:stage_info(DepS, SI),
    case StageInfo#stage_info.finished of
        true -> all_deps_finished(Rest, S, SI);
        false -> false
    end.

% Dir files are a scalability mechanism to handle large numbers of
% output labels (and hence output files) from a task.  When output
% data is spread across a large number of labels by a large number of
% stage tasks, usually each labeled file tends to be small.  Handling
% each of (label, filename) as a separate output can take up a large
% amount of memory in the master, and also can cause a huge number of
% http requests to fetch these large number of small files over the
% network.  Instead, the dir file approach puts all the (label, file)
% tuples into a single 'dir' file, and sends the same of this file as
% the output.  In order to perform label-based grouping, the labels in
% the 'dir' file are also sent along with its name in the output spec.
% A subsequent 'shuffle' stage using a group_node grouping normally
% uses these 'dir' file inputs to compress the large number of small
% files into fewer larger files per-label.

% However, the notion of dir files creates a slightly complicates the
% notion of label-based grouping.  For example, when the master
% performs a 'split' grouping, which starts a task per stage input,
% and a dir file is a stage input, it is not clear how many tasks need
% to be started.  In such cases, we use a label as a unit; in the
% 'split' case, it means we start a task per label.

% utility to create a group -> [dir_spec()] mapping for the dir files
% in the specified outputs.
dirdict(Outputs) ->
    Dirs = [{{L, H}, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {dir, {H, _U, Labels}} = D} <- Tout,
               Labels =/= [],
               {L, _Sz} <- Labels],
    lists:foldl(fun([], DirD) ->
                        DirD;
                   ([{G, _}|_] = HD, DirD) ->
                        {_, DirList} = lists:unzip(HD),
                        dict:store(G, DirList, DirD)
                end, dict:new(), disco_util:groupby(1, lists:sort(Dirs))).

-spec unique_labels([{label(), data_size()}]) -> [label()].
unique_labels(LabelSizes) ->
    Labels = [L || {L, _Sz} <- LabelSizes],
    gb_sets:to_list(gb_sets:from_list(Labels)).

-spec group_outputs(label_grouping(), [{task_id(), [task_output()]}])
                   -> [grouped_output()].
group_outputs(_, []) ->
    [];
group_outputs(split, Outputs) ->
    % As explained above, each label from a dir file is a separate
    % 'split' output.
    Dirs = [{{L, H}, [{{Tid, Outid}, D}]}
            || {Tid, Tout} <- Outputs,
               {Outid, {dir, {H, _U, Labels}} = D} <- Tout,
               Labels =/= [],
               L <- unique_labels(Labels)],
    Dats = [{{L, pick_local_host(Reps)}, [{{Tid, Outid}, D}]}
            || {Tid, Tout} <- Outputs,
               {Outid, {data, {L, _S, Reps}} = D} <- Tout,
               Reps =/= []],
    Dirs ++ Dats;

group_outputs(group_node_label, Outputs) ->
    Dats = [{{L, pick_local_host(Reps)}, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {data, {L, _S, Reps}} = D} <- Tout,
               Reps =/= []],
    DatGroups = disco_util:groupby(1, lists:sort(Dats)),
    DirDict = dirdict(Outputs),
    Groups = lists:foldl(fun([], Acc) ->
                                 Acc;
                            ([{G, _D}|_] = DG, Acc) ->
                                 {_GList, DGroup} = lists:unzip(DG),
                                 Group = case dict:find(G, DirDict) of
                                             {ok, Dirs} -> Dirs ++ DGroup;
                                             error -> DGroup
                                         end,
                                 [{G, Group} | Acc]
                         end, [], DatGroups),
    % Add in any dir-groups that were not coalesced with dat-groups.
    GroupNames = lists:usort([G || {G, _Group} <- Groups]),
    DirGroups = [DG || {G, _DG} = DG <- dict:to_list(DirDict),
                       not lists:member(G, GroupNames)],
    DirGroups ++ Groups;

group_outputs(group_label, Outputs) ->
    Dirs = [{L, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {dir, {_H, _U, Labels}} = D} <- Tout,
               Labels =/= [],
               L <- unique_labels(Labels)],
    Dats = [{L, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {data, {L, _S, Reps}} = D} <- Tout,
               Reps =/= []],
    [{{L, none}, Group} % 'none' indicates no host is assigned by grouping.
     || [{L,_D}|_] = LGroup <- disco_util:groupby(1, lists:sort(Dats ++ Dirs)),
        {_L, Group} <- [lists:unzip(LGroup)]];

group_outputs(group_node, Outputs) ->
    Dirs = [{H, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {dir, {H, _U, Labels}} = D} <- Tout,
               Labels =/= []],
    Dats = [{pick_local_host(Reps), {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {data, {_L, _S, Reps}} = D} <- Tout,
               Reps =/= []],
    [{{0, H}, Group}    % '0' indicates no label is assigned by grouping.
     || [{H, _D}|_] = HGroup <- disco_util:groupby(1, lists:sort(Dats ++ Dirs)),
        {_H, Group} <- [lists:unzip(HGroup)]];

group_outputs(group_all, Outputs) ->
    Dirs = [{{Tid, Outid}, D} || {Tid, Tout} <- Outputs,
                                 {Outid, {dir, {_H, _U, Labels}} = D} <- Tout,
                                 Labels =/= []],
    Dats = [{{Tid, Outid}, D} || {Tid, Tout} <- Outputs,
                                 {Outid, {data, {_L, _S, Reps}} = D} <- Tout,
                                 Reps =/= []],
    [{{0, none}, Dirs ++ Dats}].  % '0', 'none' indicate no assigned host or label.

-spec input_urls(data_input(), label_grouping(), group())
                -> {all | label(), [url()]}.
input_urls({data, {L, _Sz, Reps}}, _LG, _G) ->
    labelled_urls([U || {U, _H} <- Reps], L, [{L, 0}]);
input_urls({dir, {_H1, U, LS}}, split, {L, _H2}) ->
    labelled_urls([U], L, LS);
input_urls({dir, {_H1, U, LS}}, group_node_label, {L, _H2}) ->
    labelled_urls([U], L, LS);
input_urls({dir, {_H1, U, LS}}, group_label, {L, _H2}) ->
    labelled_urls([U], L, LS);
input_urls({dir, {_H1, U, _LS}}, group_node, {_L, _H2}) ->
    {all, [U]};
input_urls({dir, {_H1, U, _LS}}, group_all, {_L, _H2}) ->
    {all, [U]}.

-spec labelled_urls([url()], label(), [{label(), data_size()}])
                   -> {label(), [url()]}.
labelled_urls(Urls, Label, LabelSizes) ->
    Labels = [L || {L, _Sz} <- LabelSizes],
    case lists:member(Label, Labels) of
        true  -> {Label, Urls};
        false -> {Label, []}
    end.

-spec output_urls(task_output_type()) -> [url()].
output_urls({data, {_L, _Sz, Reps}}) ->
    [<<U/binary>> || {U, _H} <- Reps];
output_urls({dir, {_H1, U, _LS}}) ->
    [U].

get_groups([], _, NewGroups, OldGroups) ->
    {NewGroups, OldGroups};

get_groups([G|Rest], GroupedBeforeDict, NewGroups, OldGroups) ->
    {GroupId, BeforeGroup} = G,
    case dict:is_key(GroupId, GroupedBeforeDict) of
        false ->
                get_groups(Rest, GroupedBeforeDict, [G|NewGroups], OldGroups);
        true ->
            case dict:fetch(GroupId, GroupedBeforeDict) of
                BeforeGroup ->
                    % no change in this group! do not send any inputs
                    get_groups(Rest, GroupedBeforeDict, NewGroups, OldGroups);
                _ ->
                    % this group has been updated and should be send to the
                    % responsible task.
                    get_groups(Rest, GroupedBeforeDict, NewGroups, [G|OldGroups])
            end
    end.

-spec get_grouping_lists(label_grouping(), [task_output()], task_id(), [task_output()]) ->
    {[grouped_output()], [grouped_output()]}.
get_grouping_lists(Grouping, PrevStageOutputs, TaskId, Outputs) ->
    GroupedBeforeThisTask = group_outputs(Grouping, PrevStageOutputs),
    GroupedBeforeDict = dict:from_list(GroupedBeforeThisTask),
    PrevStageOutputs1 = [{TaskId, Outputs}|PrevStageOutputs],
    GroupedAfterThisTask = group_outputs(Grouping, PrevStageOutputs1),
    get_groups(GroupedAfterThisTask, GroupedBeforeDict, [], []).
