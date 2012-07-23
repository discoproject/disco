
-module(fair_scheduler_job).
-behaviour(gen_server).

-export([start/2, next_task/3, get_stats/2]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-define(SCHEDULE_TIMEOUT, 30000).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("fair_scheduler.hrl").

-type load() :: {non_neg_integer(), {url(), host()}}.

-spec start(jobname(), pid()) -> {ok, pid()} | {error, _}.
start(JobName, JobCoord) ->
    case gen_server:start(fair_scheduler_job, {JobName, JobCoord},
                          disco:debug_flags("fair_scheduler_job-" ++ JobName))
    of  {ok, _Server} = Ret -> Ret;
        Error ->
            % This happens mainly if the job coordinator has
            % already died.
            V = is_process_alive(JobCoord),
            if V ->
                    % If it hasn't, this is a real error.
                    Error;
               true ->
                    % If it's dead, we can just remove a dummy pid
                    % that will become zombie right away.  Scheduler
                    % monitoring will notice that it's dead and react
                    % accordingly.
                    {ok, spawn(fun() -> ok end)}
            end
    end.

-type stats() :: {non_neg_integer(), non_neg_integer()}.
-spec get_stats(pid(), timeout()) -> {ok, stats()}.
get_stats(JobPid, Timeout) ->
    gen_server:call(JobPid, get_stats, Timeout).

-type state() :: {gb_tree(), gb_tree(), [host()]}.

-spec init({jobname(), pid()}) -> gs_init() | {stop, normal}.
init({JobName, JobCoord}) ->
    process_flag(trap_exit, true),
    put(jobname, JobName),
    try
        true = link(JobCoord),
        {ok, {gb_trees:insert(nopref, {0, 0, []}, gb_trees:empty()),
              gb_trees:empty(),
              []}}
    catch K:V ->
            lager:warning("~p: linking to coordinator for job ~p failed: ~p:~p",
                          [?MODULE, JobName, K, V]),
            {stop, normal}
    end.

% MAIN FUNCTION:
% Return a next task to be executed from this Job.
-spec next_task(pid(), [jobinfo()], [host()])
               -> {ok, {host(), task()}} | none.
next_task(Job, Jobs, AvailableNodes) ->
    schedule(schedule_local, Job, Jobs, AvailableNodes).

-spec schedule(schedule_local | schedule_remote, pid(), [jobinfo()], [host()])
              -> {ok, {host(), task()}} | none.
schedule(Mode, Job, Jobs, AvailableNodes) ->
    % First try to find a node-local or remote task to execute.
    try
        case gen_server:call(Job, {Mode, AvailableNodes}, ?SCHEDULE_TIMEOUT) of
            {run, Node, Task} ->
                {ok, {Node, Task}};
            nonodes ->
                none;
            nolocal ->
                % No locals, if empty nodes (i.e. nodes where no job
                % has any tasks assigned) are available, we can assign
                % a task to one of them.
                Empty = all_empty_nodes(Jobs, AvailableNodes),
                schedule(schedule_remote, Job, Jobs, Empty)
        end
    catch K:V ->
            lager:warning("Scheduling error: ~p:~p!", [K, V]),
            gen_server:cast(Job, {die, "Scheduling error (system busy?)"}),
            none
    end.

-spec get_empty_nodes(pid(), [host()], non_neg_integer())
                     -> {ok, [host()]} | {error, term()}.
get_empty_nodes(Job, AvailableNodes, Timeout) ->
    gen_server:call(Job, {get_empty_nodes, AvailableNodes}, Timeout).

% Return an often empty subset of AvailableNodes that don't have any tasks
% assigned to them by any job.
-spec all_empty_nodes([jobinfo()], [host()]) -> [host()].
all_empty_nodes(_, []) -> [];
all_empty_nodes([], AvailableNodes) -> AvailableNodes;
all_empty_nodes([Job|Jobs], AvailableNodes) ->
    try
        {ok, L} = get_empty_nodes(Job, AvailableNodes, 500),
        all_empty_nodes(Jobs, L)
    catch _:_ -> % Job may have died already, don't care
            all_empty_nodes(Jobs, AvailableNodes)
    end.

-type cast_msgs() :: new_task_msg() | update_nodes_msg()
                   | {task_started, host(), pid()}.
-spec handle_cast(cast_msgs(), state()) -> gs_noreply();
                 ({die, string()}, state()) -> gs_stop(normal).

% Assign a new task to this job.
handle_cast({new_task, Task, NodeStats}, {Tasks, Running, Nodes}) ->
    NewTasks = assign_task(Task, NodeStats, Tasks, Nodes),
    {noreply, {NewTasks, Running, Nodes}};

% Cluster topology changed (see below).
handle_cast({update_nodes, NewNodes}, {Tasks, Running, _}) ->
    NewTasks = reassign_tasks(Tasks, NewNodes),
    {noreply, {NewTasks, Running, NewNodes}};

% Add a new task to the list of running tasks (for the fairness fairy).
handle_cast({task_started, Node, Worker}, {Tasks, Running, Nodes}) ->
    erlang:monitor(process, Worker),
    NewRunning = gb_trees:insert(Worker, Node, Running),
    {noreply, {Tasks, NewRunning, Nodes}};

handle_cast({die, Msg}, S) ->
    event_server:event(get(jobname),
        "ERROR: Job killed due to an internal exception: ~s", [Msg], none),
    {stop, normal, S}.

-type schedule_result() :: nolocal | nonodes | {run, host(), task()}.
-spec handle_call(dbg_state_msg(), from(), state()) -> gs_reply(state());
                 (get_stats, from(), state()) -> gs_reply({ok, stats()});
                 ({get_empty_nodes, [host()]}, from(), state()) ->
                         gs_reply({ok, [host()]});
                 ({schedule_local|schedule_remote, [host()]}, from(), state()) ->
                         gs_reply(schedule_result()).

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

% Job stats for the fairness fairy
handle_call(get_stats, _, {Tasks, Running, _} = S) ->
    NumTasks = lists:sum([N || {N, _, _} <- gb_trees:values(Tasks)]),
    {reply, {ok, {NumTasks, gb_trees:size(Running)}}, S};

% Return a subset of AvailableNodes that don't have any tasks assigned
% to them by this job.
handle_call({get_empty_nodes, AvailableNodes}, _, {Tasks, _, _} = S) ->
    case gb_trees:get(nopref, Tasks) of
        {0, _, _} ->
            {reply, {ok, empty_nodes(Tasks, AvailableNodes)}, S};
        _ ->
            {reply, {ok, []}, S}
    end;

% Primary task scheduling policy:
% Try to find
% 1) a local task assigned to one of the AvailableNodes
% 2) any remote task
handle_call({schedule_local, AvailableNodes}, _, {Tasks, Running, Nodes}) ->
    {Reply, UpdatedTasks} = schedule_local(Tasks, AvailableNodes),
    {reply, Reply, {UpdatedTasks, Running, Nodes}};

% Secondary task scheduling policy:
% No local or remote tasks were found. Free nodes are available that have
% no tasks assigned to them by any job. Pick a task from
% If ForceLocal, always fail.
handle_call({schedule_remote, FreeNodes}, _, {Tasks, Running, Nodes}) ->
    LocalNodes = datalocal_nodes(Tasks, gb_trees:keys(Tasks)),
    {Reply, UpdatedTasks} = pop_and_switch_node(Tasks, LocalNodes, FreeNodes),
    {reply, Reply, {UpdatedTasks, Running, Nodes}}.

-spec handle_info(term(), state()) -> gs_noreply() | gs_stop(normal).

% Task done. Remove it from the list of running tasks. (for the fairness fairy)
handle_info({'DOWN', _, _, Worker, _}, {Tasks, Running, Nodes}) ->
    {noreply, {Tasks, gb_trees:delete(Worker, Running), Nodes}};

handle_info({'EXIT', Pid, normal}, S) when Pid == self() ->
    {stop, normal, S};

% Our job coordinator dies, the job is dead, we have no reason to live anymore
handle_info({'EXIT', _, _}, S) ->
    {stop, normal, S};

% handle late replies to "catch gen_server:call"
handle_info({Ref, _Msg}, S) when is_reference(Ref) ->
    {noreply, S}.

-spec schedule_local(gb_tree(), [host()]) -> {schedule_result(), gb_tree()}.
schedule_local(Tasks, AvailableNodes) ->
    % Does the job have any local tasks to be run on AvailableNodes?
    case datalocal_nodes(Tasks, AvailableNodes) of
        % No local tasks assigned to AvailableNodes
        [] ->
            % Does the job have any remote tasks?
            case gb_trees:get(nopref, Tasks) of
                % No remote tasks either. Maybe we can move
                % a local task -> we might receive a
                % schedule_remote() call if free nodes are
                % available.
                {0, _, _} -> {nolocal, Tasks};
                % Remote tasks found. Pick a remote task
                % and run it on a random node that is
                % available.
                _ -> pop_and_switch_node(Tasks, [nopref], AvailableNodes)
            end;
        % Local tasks found. Choose an AvailableNode that has the
        % least number of tasks already running, that is, the first
        % item in the list. Pick the first task from the chosen node.
        [Node|_] ->
            {N, C, [Task|R]} = gb_trees:get(Node, Tasks),
            {{run, Node, Task}, gb_trees:update(Node, {N - 1, C, R}, Tasks)}
    end.

-spec pop_and_switch_node(gb_tree(), [nopref|host()], [host()])
                         -> {schedule_result(), gb_tree()}.
pop_and_switch_node(Tasks, _, []) -> {nonodes, Tasks};
pop_and_switch_node(Tasks, Nodes, AvailableNodes) ->
    case pop_busiest_node(Tasks, Nodes) of
        % No tasks left. However, the last currently running task
        % might fail, so we must stay alive.
        {nonodes, UTasks} -> {nonodes, UTasks};
        % Move Task from its local node to the Target node
        {{run, _, Task}, UTasks} ->
            % Make sure that our choice is ok:
            % All AvailableNodes must not be in taskblacklist,
            % task must not be force_local, nor force_remote if
            % all its inputs are in AvailableNodes.
            case choose_node(Task, AvailableNodes) of
                {ok, Target} ->
                    {{run, Target, Task}, UTasks};
                false ->
                    % The primary choice isn't ok. Find any
                    % other task that is ok.
                    pop_suitable(Tasks, Nodes, AvailableNodes)
            end
    end.

% pop_busiest_node defines the policy for choosing the next task from a chosen
% set of Nodes. Pick the task from the longest list.
-spec pop_busiest_node(gb_tree(), [host()]) -> {schedule_result(), gb_tree()}.
pop_busiest_node(Tasks, []) -> {nonodes, Tasks};
pop_busiest_node(Tasks, Nodes) ->
    {{N, C, [Task|R]}, MaxNode} = lists:max([{gb_trees:get(Node, Tasks), Node}
                                             || Node <- Nodes]),
    {{run, MaxNode, Task}, gb_trees:update(MaxNode, {N - 1, C, R}, Tasks)}.

% Given a task, choose a node from AvailableNodes
% 1) that is not in the taskblack list
% 2) that doesn't contain any input of the task, if force_remote == true
% unless force_local == true. Otherwise return false.
-spec choose_node(task(), [host()]) -> false | {ok, host()}.
choose_node(#task{force_local = true}, _) -> false;
choose_node(Task, AvailableNodes) ->
    case AvailableNodes -- Task#task.taskblack of
        [] -> false;
        NB when Task#task.force_remote ->
            case NB -- [N || {_, N} <- Task#task.input] of
                [] -> false;
                [Node|_] -> {ok, Node}
            end;
        [Node|_] -> {ok, Node}
    end.

% Pop the first task in Nodes that can be run
-spec pop_suitable(gb_tree(), [host()], [host()]) -> {schedule_result(), gb_tree()}.
pop_suitable(Tasks, Nodes, AvailableNodes) ->
    case find_suitable([], none, Nodes, Tasks, AvailableNodes) of
        false -> {nonodes, Tasks};
        {ok, Task, Node, Target} ->
            {N, C, L} = gb_trees:get(Node, Tasks),
            UTasks = gb_trees:update(Node, {N - 1, C, L -- [Task]}, Tasks),
            {{run, Target, Task}, UTasks}
    end.

% Find first task from any node in Nodes that can be run,
% i.e. choose_node() =/= false
-spec find_suitable([task()], none|host(), [host()], gb_tree(), [host()]) ->
    {ok, task(), host(), host()} | false.
find_suitable([], _, [], _, _) -> false;
find_suitable([], _, [Node|R], Tasks, AvailableNodes) ->
    {_, _, L} = gb_trees:get(Node, Tasks),
    find_suitable(L, Node, R, Tasks, AvailableNodes);
find_suitable([T|R], Node, RestNodes, Tasks, AvailableNodes) ->
    case choose_node(T, AvailableNodes) of
        false -> find_suitable(R, Node, RestNodes, Tasks, AvailableNodes);
        {ok, Target} -> {ok, T, Node, Target}
    end.

% return nodes that don't have any local tasks assigned to them
-spec empty_nodes(gb_tree(), [host()]) -> [host()].
empty_nodes(Tasks, AvailableNodes) ->
    filter_nodes(Tasks, AvailableNodes, false).

% return nodes that have at least one local task assigned to them
-spec datalocal_nodes(gb_tree(), [host()]) -> [host()].
datalocal_nodes(Tasks, AvailableNodes) ->
    filter_nodes(Tasks, AvailableNodes, true).

-spec filter_nodes(gb_tree(), [host()], boolean()) -> [host()].
filter_nodes(Tasks, AvailableNodes, Local) ->
    [Node || Node <- AvailableNodes,
             case gb_trees:lookup(Node, Tasks) of
                 none -> false =:= Local;
                 {value, {0, _, _}} -> false =:= Local;
                 _ -> true =:= Local
             end].

-spec on_error(task(), nonempty_string()) -> no_return().
on_error(T, M) ->
    Format = lists:flatten(["ERROR: ~s:~B Task is forced to be ", M,
                            " but there are no nodes where it could be run ",
                            "(inputs:~s)."]),
    Args = [T#task.mode, T#task.taskid,
            [binary_to_list(<<" ", Url/binary>>) || {Url, _} <- T#task.input]],
    event_server:event(get(jobname), Format, Args, none),
    exit(normal).

% Assign a new task to a node which hostname match with the hostname
% of the task's input file, if the node is not in the task's blacklist.
% The task may have several redundant inputs, so we need to find the
% first one that matchs.
-spec assign_task(task(), [load()], gb_tree(), [host()]) -> gb_tree().
assign_task(Task, NodeStats, Tasks, Nodes) ->
    case Nodes -- Task#task.taskblack of
        [] ->
            % If the task has failed already on all the available nodes,
            % we reset the blacklist instead of aborting the job.
            assign_task0(Task#task{taskblack = []}, NodeStats, Tasks, Nodes);
        OkNodes ->
            assign_task0(Task, NodeStats, Tasks, OkNodes)
    end.

-spec assign_task0(task(), [load()], gb_tree(), [host()]) -> gb_tree().
assign_task0(#task{force_remote = true, input = Input} = Task,
             _NodeStats, Tasks, Nodes) ->
    case Nodes -- [N || {_, N} <- Input] of
        [] ->
            on_error(Task, "remote");
        _ ->
            assign_nopref(Task, Tasks, Nodes)
    end;
assign_task0(Task, NodeStats, Tasks, Nodes) ->
    findpref(Task, NodeStats, Tasks, Nodes).

-spec assign_nopref(task(), gb_tree(), [host()]) -> gb_tree().
assign_nopref(#task{force_local = true} = Task, _Tasks, _Nodes) ->
    on_error(Task, "local");

assign_nopref(Task, Tasks, _Nodes) ->
    {N, C, L} = gb_trees:get(nopref, Tasks),
    % Choosing a nopref-replica randomly is clearly a suboptimal policy.
    % We should ignore replicas that have failed in the past.
    {Input, _} = disco_util:choose_random(Task#task.input),
    T = Task#task{chosen_input = Input},
    gb_trees:update(nopref, {N + 1, C + 1, [T|L]}, Tasks).

-spec findpref(task(), [load()], gb_tree(), [host()]) -> gb_tree().
findpref(Task, NodeStats, Tasks, Nodes) ->
    LoadSorted = lists:sort([{taskcount(Node, Tasks), Load, X}
                             || {Load, {_Url, Node} = X}
                                    <- NodeStats, lists:member(Node, Nodes)]),
    case LoadSorted of
        [] ->
            assign_nopref(Task, Tasks, Nodes);
        [{_Load, _Count0, {Url, Node}}|_] ->
            T = Task#task{chosen_input = Url},
            {N, Count, TaskList} = get_default(Node, Tasks, {0, 0, []}),
            gb_trees:enter(Node, {N + 1, Count + 1, [T|TaskList]}, Tasks)
    end.

-spec taskcount(host(), gb_tree()) -> non_neg_integer().
taskcount(Node, Tasks) ->
    {_N, Count, _TaskList} = get_default(Node, Tasks, {0, 0, []}),
    Count.

% Cluster topology changed: New nodes appeared or existing ones were
% deleted or black- / whitelisted. Re-assign tasks.
-spec reassign_tasks(gb_tree(), [host()]) -> gb_tree().
reassign_tasks(Tasks, NewNodes) ->
    {OTasks, NTasks} =
        lists:foldl(
          fun(Node, {OTasks, NTasks}) ->
                  case gb_trees:lookup(Node, OTasks) of
                      {value, TList} ->
                          {gb_trees:delete(Node, OTasks),
                           gb_trees:insert(Node, TList, NTasks)};
                      none ->
                          {OTasks, NTasks}
                  end
          end, {Tasks, gb_trees:empty()}, NewNodes),

    lists:foldl(
      fun(Task, NTasks0) ->
              NodeStats = [{random:uniform(100), Input}
                           || Input <- Task#task.input],
              assign_task(Task, NodeStats, NTasks0, NewNodes)
      end,
      gb_trees:insert(nopref, {0, 0, []}, NTasks),
      lists:flatten([L || {_, _, L} <- gb_trees:values(OTasks)])).

-spec get_default(host(), gb_tree(), T) -> T.
get_default(Key, Tree, Default) ->
    case gb_trees:lookup(Key, Tree) of
        none -> Default;
        {value, Val} -> Val
    end.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
