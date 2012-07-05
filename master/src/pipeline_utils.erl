-module(pipeline_utils).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-export([job_from_jobinfo/1, job_schedule_option/1, stages/1]).
-export([next_stage/2, group_outputs/2, pick_local_host/1]).
-export([locations/1, ranked_locations/1]).

% Compatibility utility: construct a pipeline from a job packet of
% Disco 0.4.2 or earlier.

-spec job_from_jobinfo(jobinfo()) -> {[task_output()],
                                      pipeline() | unsupported_job}.
job_from_jobinfo(#jobinfo{inputs = JI, map = M, reduce = R, nr_reduce = NR}) ->
    job_from_jobinfo(JI, M, R, NR).

job_from_jobinfo(JobInputs, Map, Reduce, Nr_reduce) ->
    Inputs = task_inputs(JobInputs),
    Pipeline = pipeline(Map, Reduce, Nr_reduce),
    {Inputs, Pipeline}.

-spec task_inputs([url() | [url()]]) -> [task_output()].
task_inputs(Inputs) ->
    % Currently, we assume that all pipeline inputs are data file
    % inputs; dir-files as job inputs in the job pack are not
    % supported.
    [{N, {data, {0, 0, input_replicas(I)}}} || {N, I} <- disco:enum(Inputs)].

-spec input_replicas([url() | [url()]]) -> [data_replica()].
input_replicas(Input) when is_binary(Input) ->
    % Single replica
    [{Input, disco:preferred_host(Input)}];
input_replicas(Reps) when is_list(Reps) ->
    % Replica set
    [{R, disco:preferred_host(R)} || R <- Reps].

-spec pipeline(boolean(), boolean(), non_neg_integer())
              -> pipeline() | unsupported_job.
pipeline(false, false, _NR) ->
    [];
pipeline(false, true, 1) ->
    [{?REDUCE, join_all}];
pipeline(false, true, _NR) ->
    % This was used to support reduce-only jobs with partitioned
    % inputs from dir:// files. This is now unsupported; instead, the
    % job-submitter will need to prepare an explicit pipeline.
    unsupported_job;
pipeline(true, false, _NR) ->
    [{?MAP, split}, {?MAP_SHUFFLE, join_node}];
pipeline(true, true, 1) ->
    [{?MAP, split}, {?MAP_SHUFFLE, join_node}, {?REDUCE, join_all}];
pipeline(true, true, _NR) ->
    % This was used to support a pre-determined number of partitions
    % in map output, which determined the number of reduces.  However,
    % we now determine the number of reduces dynamically.
    [{?MAP, split}, {?MAP_SHUFFLE, join_node},
     {?REDUCE, join_label}, {?REDUCE_SHUFFLE, join_node}].

-spec job_schedule_option(jobinfo()) -> task_schedule().
job_schedule_option(#jobinfo{force_local = Local, force_remote = Remote}) ->
    job_schedule_option(Local, Remote).
% Prefer Local if both Local and Remote are set.
job_schedule_option(true, _) -> local;
job_schedule_option(_, true) -> remote;
job_schedule_option(_, _) -> none.

-spec stages(pipeline()) -> [stage_name()].
stages(Pipeline) -> [Stage || {Stage, _G} <- Pipeline].

% Other utilities.

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
next_stage([{_S, _G} = First | _], ?INPUT) -> First;
next_stage([{S, _G}], S) -> done;
next_stage([{S, _G}, Next|_], S) -> Next;
next_stage([_S|Rest], S) -> next_stage(Rest, S).

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
% A subsequent 'shuffle' stage using a join_node grouping normally
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
-spec dirdict([{task_id(), [task_output()]}]) -> dict().
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

-spec group_outputs(label_grouping(), [{task_id(), [task_output()]}])
                   -> [grouped_output()].
group_outputs(split, Outputs) ->
    % As explained above, each label from a dir file is a separate
    % 'split' output.
    Dirs = [{{L, H}, [{{Tid, Outid}, D}]}
            || {Tid, Tout} <- Outputs,
               {Outid, {dir, {H, _U, Labels}} = D} <- Tout,
               Labels =/= [],
               {L, _Sz} <- Labels],
    Dats = [{{L, pick_local_host(Reps)}, [{{Tid, Outid}, D}]}
            || {Tid, Tout} <- Outputs,
               {Outid, {data, {L, _S, Reps}} = D} <- Tout,
               Reps =/= []],
    Dirs ++ Dats;

group_outputs(join_node_label, Outputs) ->
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

group_outputs(join_label, Outputs) ->
    Dirs = [{L, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {dir, {_H, _U, Labels}} = D} <- Tout,
               Labels =/= [],
               {L, _Sz} <- Labels],
    Dats = [{L, {{Tid, Outid}, D}}
            || {Tid, Tout} <- Outputs,
               {Outid, {data, {L, _S, Reps}} = D} <- Tout,
               Reps =/= []],
    [{{L, none}, Group} % 'none' indicates no host is assigned by grouping.
     || [{L,_D}|_] = LGroup <- disco_util:groupby(1, lists:sort(Dats ++ Dirs)),
        {_L, Group} <- [lists:unzip(LGroup)]];

group_outputs(join_node, Outputs) ->
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

group_outputs(join_all, Outputs) ->
    Dirs = [{{Tid, Outid}, D} || {Tid, Tout} <- Outputs,
                                 {Outid, {dir, {_H, _U, _Labels}} = D} <- Tout],
    Dats = [{{Tid, Outid}, D} || {Tid, Tout} <- Outputs,
                                 {Outid, {data, {_L, _S, Reps}} = D} <- Tout,
                                 Reps =/= []],
    [{{0, none}, Dirs ++ Dats}].  % '0', 'none' indicate no assigned host or label.
