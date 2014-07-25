-module(jc_utils).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("job_coordinator.hrl").

% This constant control the number of tasks that can be started from the
% following concurrent stages.  The lower this value, the higher the degree of
% concurrency, but the higher the potential for job deadlock.  The constant 1 is
% the minimum value that guarantees there will be no deadlock if the number of
% workers does not decrease.
-define(MIN_RATION_OF_FIRST_ACTIVE_TO_REST, 2).

-export([stage_info_opt/2, stage_info/2, last_stage_task/2,
         update_stage/3, update_stage_tasks/4,
         task_info/2, update_task_info/3, task_spec/2, add_task_spec/3,
         task_inputs/2, task_outputs/2,
         input_info/2, update_input_info/3, add_input/3,
         update_input_failures/2, find_usable_input_hosts/1,
         collect_stagewise/5, wakeup_waiters/3, no_tasks_running/2,
         running_tasks/2, can_run_task/4]).

-type stage_map() :: disco_gbtree(stage_name(), stage_info()).
-type task_map()  :: disco_gbtree(task_id(), task_info()).
-type input_map() :: disco_gbtree(input_id(), data_info()).

% information about a stage which may not have yet run.
-spec stage_info_opt(stage_name(), stage_map()) -> none | stage_info().
stage_info_opt(Stage, SI) ->
    case gb_trees:lookup(Stage, SI) of
        none -> none;
        {value, Info} -> Info
    end.

% information about a stage which the caller guarantees exists.
-spec stage_info(stage_name(), stage_map()) -> stage_info().
stage_info(Stage, SI) ->
    gb_trees:get(Stage, SI).

-spec update_stage(stage_name(), stage_info(), stage_map()) -> stage_map().
update_stage(Stage, Info, SI) ->
    gb_trees:enter(Stage, Info, SI).

-type task_op() :: run | stop | done.
-spec update_stage_tasks(stage_name(), task_id(), task_op(), stage_map())
                -> stage_map().
update_stage_tasks(S, Id, Op, SI) ->
    mod_stage_tasks(S, Id, Op, stage_info(S, SI), SI).
mod_stage_tasks(S, Id, Op, #stage_info{running = R, done = D, n_running = NR} = Info, SI) ->
    {R1, D1, NR1} =
        case Op of
            run  -> {lists:usort([Id | R]), D, NR + 1};
            stop -> {R -- [Id], D, NR - 1};
            done -> {R -- [Id], lists:usort([Id | D]), NR - 1}
        end,
    update_stage(S, Info#stage_info{running = R1, done = D1, n_running = NR1}, SI).

-spec can_run_task(pipeline(), stage_name(), stage_map(), task_schedule()) -> boolean().
can_run_task(P, S, SI, #task_schedule{max_cores = Max}) ->
    can_run_task(P, S, SI, true, 0, Max).
% the fifth argument shows whether all of the dependencies have finished so
% far.  It will determine whether the task can run or not for sequential stages.
% the sixth argument shows the total number of tasks running in the previous
% stages.
can_run_task([_|_], ?INPUT, _, _, _, _) -> true;
can_run_task(_, _, _, _, NR, Max) when NR >= Max -> false;
can_run_task([], _S, _, _, _, _) -> true;
can_run_task([{S, _, _}|_], S, SI, true, DepsNRTasks, Max) ->
    #stage_info{n_running = NR} = jc_utils:stage_info(S, SI),
    NR + DepsNRTasks < Max;
can_run_task([{S, _, false}|_], S, _, false, _, _) -> false;
can_run_task([{S, _, true}|_], S, SI, false, DepsNRTasks, Max) ->
    #stage_info{n_running = NR} = jc_utils:stage_info(S, SI),
    ?MIN_RATION_OF_FIRST_ACTIVE_TO_REST * NR < DepsNRTasks andalso
    NR + DepsNRTasks < Max;
can_run_task([{DepS,_, _}|Rest], S, SI, DepsFinished, DepsNRTasks, Max) ->
    #stage_info{finished = Finished, n_running = NR} = jc_utils:stage_info(DepS, SI),
    case Finished of
        true  -> can_run_task(Rest, S, SI, DepsFinished, DepsNRTasks, Max);
        false -> can_run_task(Rest, S, SI, false, DepsNRTasks + NR, Max)
    end.

-spec no_tasks_running(stage_name(), stage_map()) -> boolean().
no_tasks_running(S, SI) ->
    #stage_info{n_running = NR} = stage_info(S, SI),
    NR == 0.

-spec running_tasks(stage_name(), stage_map()) -> [task_id()].
running_tasks(S, SI) ->
    case stage_info_opt(S, SI) of
        none -> [];
        StageInfo ->
            StageInfo#stage_info.running
    end.

% If this is the last pending task in the stage in order for the stage
% to be done.
-spec last_stage_task(stage_name(), stage_map()) -> boolean().
last_stage_task(Stage, SI) ->
    #stage_info{done = Done, all = All} = stage_info(Stage, SI),
    length(Done) =:= All.

-spec is_running(task_id(), task_map(), stage_map()) -> boolean().
is_running(TaskId, Tasks, SI) ->
    #task_info{spec = #task_spec{taskid = TaskId, stage = Stage}}
        = task_info(TaskId, Tasks),
    #stage_info{done = Done} = stage_info(Stage, SI),
    lists:member(TaskId, Done).

-spec is_waiting(task_id(), task_map()) -> boolean().
is_waiting(TaskId, Tasks) ->
    #task_info{depends = Depends} = task_info(TaskId, Tasks),
    Depends =/= [].

-spec task_info(input | task_id(), task_map()) -> task_info().
task_info(TaskId, Tasks) ->
    gb_trees:get(TaskId, Tasks).

-spec task_spec(input, input_map()) -> input;
            (task_id(), task_map()) -> task_spec().
task_spec(TaskId, Tasks) ->
    (task_info(TaskId, Tasks))#task_info.spec.

-spec task_outputs(input | task_id(), task_map()) -> [task_output()].
task_outputs(TaskId, Tasks) ->
    (task_info(TaskId, Tasks))#task_info.outputs.

% This should only be called for new tasks.
-spec add_task_spec(task_id(), task_spec(), task_map()) -> task_map().
add_task_spec(TaskId, TaskSpec, Tasks) ->
    gb_trees:enter(TaskId, #task_info{spec = TaskSpec}, Tasks).

-spec update_task_info(task_id(), task_info(), task_map()) -> task_map().
update_task_info(TaskId, TInfo, Tasks) ->
    gb_trees:enter(TaskId, TInfo, Tasks).

% Input utilities.
-spec add_input(input_id(), data_info(), input_map()) -> input_map().
add_input(InputId, DInfo, DataMap) ->
    gb_trees:enter(InputId, DInfo, DataMap).

-spec input_info(input_id(), input_map()) -> data_info().
input_info(InputId, DataMap) ->
    gb_trees:get(InputId, DataMap).

-spec update_input_info(input_id(), data_info(), input_map()) -> input_map().
update_input_info(InputId, DInfo, DataMap) ->
    gb_trees:enter(InputId, DInfo, DataMap).

-spec task_inputs([input_id()], input_map()) -> [{input_id(), data_input()}].
task_inputs(InputIds, DataMap) ->
    [{Id, (gb_trees:get(Id, DataMap))#data_info.source} || Id <- InputIds].

% Data utilities.
-spec data_inputs(data_info()) -> [data_input()].
data_inputs(#data_info{source = Source, locations = Locations}) ->
    [Source | gb_trees:values(Locations)].
-spec data_hosts(data_info()) -> [host()].
data_hosts(DInfo) ->
    lists:usort(lists:flatten([pipeline_utils:locations(DI)
                               || DI <- data_inputs(DInfo)])).

-spec update_input_failures([host()], data_info()) -> data_info().
update_input_failures(Hosts, #data_info{failures = Failures} = DInfo) ->
    F = lists:foldl(
          fun(H, Fails) ->
                  case gb_trees:lookup(H, Fails) of
                      none         -> Fails;
                      {value, Cnt} -> gb_trees:enter(H, Cnt + 1, Fails)
                  end
          end, Failures, Hosts),
    DInfo#data_info{failures = F}.

-spec find_usable_input_hosts(data_info()) -> [host()].
find_usable_input_hosts(#data_info{failures = Failures}) ->
    [H || {H, Fails} <- gb_trees:to_list(Failures), Fails < ?MAX_INPUT_FAILURE].

% Input regeneration and task re-run utilities.

% Find the inputs that are local to the specified list of hosts.
-spec local_inputs([input_id()], disco_gbset(host()), input_map()) -> [input_id()].
local_inputs(InputIds, Hosts, DataMap) ->
    local_inputs(InputIds, Hosts, DataMap, []).
local_inputs([], _Hosts, _DataMap, Acc) ->
    lists:usort(Acc);
local_inputs([Id | Rest], Hosts, DataMap, Acc) ->
    % An input is local to a set of hosts iff all its replicas are
    % found only on those hosts.
    Acc1 =
        case lists:all(fun(H) -> gb_sets:is_member(H, Hosts) end,
                       data_hosts(input_info(Id, DataMap)))
        of  true  ->  [Id | Acc];
            false -> Acc
        end,
    local_inputs(Rest, Hosts, DataMap, Acc1).

% The backtracking algorithm tries to figure out what tasks to run in
% order to re-generate a specified intermediate output, given that the
% output is not accessible on the host that it was generated on.

% A precise algorithm to do this would involve the computation of
% three sets:
%
% - Tasks, the set of tasks that need to be re-executed
% - DataInputs, the set of data inputs that need to be re-generated
% - Hosts, the set of hosts on which the Tasks cannot be run
%
% The sets are initialized as follows, given an intermediate input D
% generated by a task T that is inaccessible on host H:
%
% Tasks := {T}, DataInputs := {D}, Hosts := {H}
%
% The sets are then closed under the following relations:
%
% (d) - if any input D' of any task T in Tasks is purely local to
%       Hosts, then D' is in DataInputs
%
% (t) - if T is a task that generated a D in DataInputs, then T is in
%       Tasks
%
% (h) - if H is a blacklisted host for any task T in Tasks, then H is
%       in Hosts
%
% The sets forming the least-fixed-point (LFP) of the above relations
% would provide the Tasks set sought for.
%
% However, the recursion between (d) and (h) makes this difficult to
% compute, and in a dynamic environment with failing hosts, the
% precision with respect to Hosts would not provide much benefit.
%
% Instead, we compute the LFP for (t) and (d), and keep Hosts fixed to
% H along with the list of hosts that are currently down.  This leaves
% the possibility that tasks will be re-executed on blacklisted hosts.

% In the actual implementation, we actually don't need all the tasks
% that need re-running, only the ones that can be re-run immediately
% (i.e. have no dependencies).  Tasks that need re-running but have
% dependencies on other tasks to be re-run, are just added as waiters
% to those tasks.  The tasks that don't have dependencies form a
% frontier that we collect as we traverse backwards, stage by stage,
% through the dependency graph.  As a special case, if a task T is
% already waiting or running, we don't recurse to its input generating
% tasks.

-spec collect_stagewise(task_id(), task_map(), stage_map(), input_map(), disco_gbset(host()))
                    -> {disco_gbset(host()), task_map()}.
collect_stagewise(TaskId, Tasks, SI, DataMap, FHosts) ->
    collect_stagewise(gb_sets:from_list([TaskId]),  % Cur stage
                      gb_sets:empty(),              % Prev stage
                      gb_sets:empty(),              % Frontier
                      Tasks, SI, DataMap, FHosts).

collect_stagewise(CurStage, PrevStage, Frontier, Tasks, SI, DataMap, FHosts) ->
    case {gb_sets:is_empty(CurStage), gb_sets:is_empty(PrevStage)} of
        {true, true} ->
            % We are done.
            {Frontier, Tasks};
        {true, false} ->
            % We are done with the current stage; we recurse, making
            % the previous stage the current one.
            collect_stagewise(PrevStage, gb_sets:empty(), Frontier,
                              Tasks, SI, DataMap, FHosts);
        {false, _} ->
            {TaskId, CurStage1} = gb_sets:take_largest(CurStage),
            case is_running(TaskId, Tasks, SI) orelse is_waiting(TaskId, Tasks)
            of  true ->
                    % Nothing to do, we should have already updated
                    % this task's waiters and depends.
                    collect_stagewise(CurStage1, PrevStage, Frontier,
                                      Tasks, SI, DataMap, FHosts);
                false ->
                    collect_task(TaskId, CurStage1, PrevStage, Frontier,
                                 Tasks, SI, DataMap, FHosts)
            end
    end.

collect_task(TaskId, CurStage, PrevStage, Frontier, Tasks, SI, DataMap, FHosts) ->
    #task_info{spec         = #task_spec{input = Inputs},
               failed_hosts = TFHosts}
        = TInfo = task_info(TaskId, Tasks),
    % Lookup task dependencies.
    LocalInputs = local_inputs(Inputs, FHosts, DataMap),
    GenTaskIds = lists:usort([TId || {TId, _} <- LocalInputs, TId =/= input]),
    % Update task.  Its okay to overwrite depends since it should be
    % empty, or else it would be waiting and we would not be
    % processing it.
    TInfo2 = TInfo#task_info{failed_hosts = gb_sets:union(TFHosts, FHosts),
                             depends      = GenTaskIds},
    Tasks1 = update_task_info(TaskId, TInfo2, Tasks),
    case GenTaskIds of
        [] ->
            % This task is runnable, and hence is on the frontier.
            % Note that we might mark a task as such even though its
            % inputs are pipeline inputs (task_id =:= input), and all
            % of them are on FHosts.  There's not much we can do in
            % this case, since we cannot regenerate pipeline inputs,
            % so we optimistically run the task anyway.
            Frontier1 = gb_sets:add_element(TaskId, Frontier),
            collect_stagewise(CurStage, PrevStage, Frontier1,
                              Tasks1, SI, DataMap, FHosts);
        [_|_] ->
            % These tasks belong to the previous stage, since we
            % currently only have linear pipelines; and this task
            % needs to wait on them (and hence is not in the
            % frontier).
            PrevStage1 = gb_sets:union(PrevStage, gb_sets:from_list(GenTaskIds)),
            GenTasks = [{Id, task_info(Id, Tasks1)} || Id <- GenTaskIds],
            Tasks2 =
                lists:foldl(
                  fun({Id, #task_info{waiters = W} = GT}, Tsks) ->
                          UGT = GT#task_info{waiters = [TaskId|W]},
                          gb_trees:enter(Id, UGT, Tsks)
                  end, Tasks1, GenTasks),
            collect_stagewise(CurStage, PrevStage1, Frontier,
                              Tasks2, SI, DataMap, FHosts)
    end.

% This removes the completed task as a dependency from a set of
% waiters, and returns the set of tasks that are now runnable since
% they have no more dependencies, and the updated task set.
-spec wakeup_waiters(task_id(), [task_id()], task_map())
                    -> {[task_id()], task_map()}.
wakeup_waiters(TaskId, Waiters, Tasks) ->
    lists:foldl(
      fun(WId, {Runnable, Tsks}) ->
              #task_info{depends = Deps} = WInfo = task_info(WId, Tsks),
              UDeps = Deps -- [TaskId],
              Runnable2 = case UDeps of
                              []    -> [WId | Runnable];
                              [_|_] -> Runnable
                          end,
              Tsks2 = gb_trees:enter(WId, WInfo#task_info{depends = UDeps}, Tsks),
              {Runnable2, Tsks2}
      end, {[], Tasks}, Waiters).
