-module(fair_scheduler_job).
-behaviour(gen_server).

-export([start/2, next_task/3, new_task/3, get_stats/2, update_nodes/2]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("fair_scheduler.hrl").

-spec start(jobname(), pid()) -> {ok, pid()} | {error, _}.
start(JobName, JobCoord) ->
    case gen_server:start(fair_scheduler_job, {JobName, JobCoord},
                          disco:debug_flags("fair_scheduler_job-" ++ JobName))
    of  {ok, _Server} = Ret ->
            Ret;
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

% The main entry point: return a next task to be executed from this
% Job.
-spec next_task(pid(), [pid()], [host()])
               -> {ok, {host(), task()}} | none.
next_task(Job, Jobs, AvailableNodes) ->
    schedule(schedule_local, Job, Jobs, AvailableNodes).

-spec new_task(pid(), task(), loadstats()) -> ok.
new_task(Job, Task, Load) ->
    gen_server:cast(Job, {new_task, Task, Load}).

% Internal API used by scheduler policy.
-type stats() :: {non_neg_integer(), non_neg_integer()}.
-spec get_stats(pid(), timeout()) -> {ok, stats()}.
get_stats(JobPid, Timeout) ->
    gen_server:call(JobPid, get_stats, Timeout).

% Internal API used across different job task schedulers.

-spec get_empty_nodes(pid(), [host()], non_neg_integer())
                     -> {ok, [host()]} | {error, term()}.
get_empty_nodes(Job, AvailableNodes, Timeout) ->
    gen_server:call(Job, {get_empty_nodes, AvailableNodes}, Timeout).

-spec update_nodes(pid(), [node_info()]) -> ok.
update_nodes(Job, NewNodes) ->
    gen_server:cast(Job, {update_nodes, NewNodes}).

% Top-level interaction with the task selection implemention.
-spec schedule(schedule_local | schedule_remote, pid(), [pid()], [host()])
              -> {ok, {host(), task()}} | none.
schedule(Mode, Job, Jobs, AvailableNodes) ->
    % First try to find a node-local or remote task to execute.
    try case gen_server:call(Job, {Mode, AvailableNodes}, infinity) of
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
    catch
        exit:{noproc, V} ->
            lager:info("Job has already finished: ~p", [V]),
            none;
        exit:{normal, V} ->
            lager:info("Job has already exited normally: ~p", [V]),
            none;
        K:V ->
            lager:warning("Scheduling error (~p:~p): ~p",
                          [K, V, erlang:get_stacktrace()]),
            gen_server:cast(Job, {die, "Scheduling error (system busy?)"}),
            none
    end.

%% ===================================================================
%% gen_server callbacks

-type pid_map() :: disco_gbtree(pid(), host()).
% url_host() -> {#queued, #run, [task()]}
-type task_map() :: disco_gbtree(url_host(), {non_neg_integer(),
                                              non_neg_integer(), [task()]}).
-type host_set() :: disco_gbset(url_host()).

-record(state, {job_name    :: jobname(),
                job_coord   :: pid(),
                host_queue = gb_trees:empty() :: task_map(),
                running    = gb_trees:empty() :: pid_map(),
                cluster    = gb_sets:empty() :: host_set()}).
-type state() :: #state{}.

-spec init({jobname(), pid()}) -> gs_init() | {stop, normal}.
init({JobName, JobCoord}) ->
    process_flag(trap_exit, true),
    put(jobname, JobName),
    try  true = link(JobCoord),
         HQ = gb_trees:insert(none, {0, 0, []}, gb_trees:empty()),
         {ok, #state{job_name = JobName, job_coord = JobCoord, host_queue = HQ}}
    catch K:V ->
            lager:warning("~p: linking to coordinator for job ~p failed: ~p:~p",
                          [?MODULE, JobName, K, V]),
            {stop, normal}
    end.

-type cast_msgs() :: new_task_msg() | update_nodes_msg()
                   | {task_started, host(), pid()}.
-spec handle_cast(cast_msgs(), state()) -> gs_noreply();
                 ({die, string()}, state()) -> gs_stop(normal).

% Assign a new task to this job.
handle_cast({new_task, Task, LoadStats},
            #state{host_queue = HQ, cluster = C, job_coord = JC} = S) ->
    NewHQ = assign_task(JC, Task, LoadStats, HQ, C),
    {noreply, S#state{host_queue = NewHQ}};

% Notifications of changes in cluster topology.
handle_cast({update_nodes, NewNodes},
            #state{host_queue = HQ, job_coord = JC} = S) ->
    LoadStats = gb_trees:from_orddict([{H, R} || {H, _, R} <- NewNodes]),
    NewC = gb_sets:from_list([H || {H, _, _} <- NewNodes]),
    NewHQ = reassign_tasks(JC, HQ, LoadStats, NewC),
    {noreply, S#state{host_queue = NewHQ, cluster = NewC}};

% Add a new task to the list of running tasks (for the fairness
% fairy).
handle_cast({task_started, Node, Worker}, #state{running   = Running} = S) ->
    erlang:monitor(process, Worker),
    NewRunning = gb_trees:insert(Worker, Node, Running),
    {noreply, S#state{running = NewRunning}};

handle_cast({die, Msg}, #state{job_name = JobName} = S) ->
    event_server:event(JobName,
                       "ERROR: Job killed due to an internal exception: ~s",
                       [Msg], none),
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
handle_call(get_stats, _, #state{host_queue = HQ, running = Running} = S) ->
    NumTasks = lists:sum([N || {N, _, _} <- gb_trees:values(HQ)]),
    {reply, {ok, {NumTasks, gb_trees:size(Running)}}, S};

% Return a subset of AvailableNodes that don't have any tasks assigned
% to them by this job.
handle_call({get_empty_nodes, AvailableNodes}, _, #state{host_queue = HQ} = S) ->
    case gb_trees:get(none, HQ) of
        {0, _, _} ->
            {reply, {ok, empty_nodes(HQ, AvailableNodes)}, S};
        _ ->
            {reply, {ok, []}, S}
    end;

% Primary task scheduling policy:
% Try to find
% 1) a local task assigned to one of the AvailableNodes
% 2) any remote task
handle_call({schedule_local, AvailableNodes}, _, #state{host_queue = HQ} = S) ->
    {Reply, HQ1} = schedule_local(HQ, AvailableNodes),
    {reply, Reply, S#state{host_queue = HQ1}};

% Secondary task scheduling policy:
% No local or remote tasks were found. Free nodes are available that have
% no tasks assigned to them by any job. Pick a task from
% If ForceLocal, always fail.
handle_call({schedule_remote, FreeNodes}, _, #state{host_queue = HQ} = S) ->
    LocalNodes = datalocal_nodes(HQ, gb_trees:keys(HQ)),
    {Reply, HQ1} = pop_and_switch_node(HQ, LocalNodes, FreeNodes),
    {reply, Reply, S#state{host_queue = HQ1}}.

-spec handle_info(term(), state()) -> gs_noreply() | gs_stop(normal).

% Task done. Remove it from the list of running tasks. (for the fairness fairy)
handle_info({'DOWN', _, _, Worker, _}, #state{running = Running} = S) ->
    {noreply, S#state{running = gb_trees:delete(Worker, Running)}};

handle_info({'EXIT', Pid, normal}, S) when Pid == self() ->
    {stop, normal, S};

% Our job coordinator dies, the job is dead, we have no reason to live anymore
handle_info({'EXIT', _, _}, S) ->
    {stop, normal, S};

% handle late replies to "catch gen_server:call"
handle_info({Ref, _Msg}, S) when is_reference(Ref) ->
    {noreply, S}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


%% ===================================================================
%% Internal utilities.
-spec get_default(node(), disco_gbtree(node(), T), T) -> T.
get_default(Key, Tree, Default) ->
    case gb_trees:lookup(Key, Tree) of
        none -> Default;
        {value, Val} -> Val
    end.

%% ===================================================================
%% Task selection callback implementation for scheduler.

% Return an often empty subset of AvailableNodes that don't have any tasks
% assigned to them by any job.
-spec all_empty_nodes([pid()], [host()]) -> [host()].
all_empty_nodes(_, []) -> [];
all_empty_nodes([], AvailableNodes) -> AvailableNodes;
all_empty_nodes([Job|Jobs], AvailableNodes) ->
    try  {ok, L} = get_empty_nodes(Job, AvailableNodes, 500),
         all_empty_nodes(Jobs, L)
    catch _:_ -> % Job may have died already, don't care
            all_empty_nodes(Jobs, AvailableNodes)
    end.

-spec schedule_local(task_map(), [node()]) -> {schedule_result(), task_map()}.
schedule_local(Tasks, AvailableNodes) ->
    % Does the job have any local tasks to be run on AvailableNodes?
    case datalocal_nodes(Tasks, AvailableNodes) of
        % No local tasks assigned to AvailableNodes
        [] ->
            % Does the job have any remote tasks?
            case gb_trees:get(none, Tasks) of
                % No remote tasks either. Maybe we can move
                % a local task -> we might receive a
                % schedule_remote() call if free nodes are
                % available.
                {0, _, _} -> {nolocal, Tasks};
                % Remote tasks found. Pick a remote task
                % and run it on a random node that is
                % available.
                _ -> pop_and_switch_node(Tasks, [none], AvailableNodes)
            end;
        % Local tasks found. Choose an AvailableNode that has the
        % least number of tasks already running, that is, the first
        % item in the list. Pick the first task from the chosen node.
        [Node|_] ->
            {N, C, [Task|R]} = gb_trees:get(Node, Tasks),
            {{run, Node, Task}, gb_trees:update(Node, {N - 1, C, R}, Tasks)}
    end.

-spec pop_and_switch_node(task_map(), [none|host()], [host()])
                         -> {schedule_result(), task_map()}.
pop_and_switch_node(Tasks, _, []) -> {nonodes, Tasks};
pop_and_switch_node(Tasks, Nodes, AvailableNodes) ->
    case pop_busiest_node(Tasks, Nodes) of
        % No tasks left. However, the last currently running task
        % might fail, so we must stay alive.
        {nonodes, _UTasks} = Ret -> Ret;

        % Move Task from its local node to the Target node
        {{run, _, Task}, UTasks} ->
            % Make sure that our choice is ok.
            case choose_host(Task, AvailableNodes) of
                {ok, Target} -> {{run, Target, Task}, UTasks};

                % The primary choice isn't ok. Find any other task
                % that is ok.
                false        -> pop_suitable(Tasks, Nodes, AvailableNodes)
            end
    end.

% pop_busiest_node defines the policy for choosing the next task from a chosen
% set of Nodes. Pick the task from the longest list.
-spec pop_busiest_node(task_map(), [host()]) -> {schedule_result(), task_map()}.
pop_busiest_node(Tasks, []) -> {nonodes, Tasks};
pop_busiest_node(Tasks, Nodes) ->
    {{N, C, [Task|R]}, MaxNode} = lists:max([{gb_trees:get(Node, Tasks), Node}
                                             || Node <- Nodes]),
    {{run, MaxNode, Task}, gb_trees:update(MaxNode, {N - 1, C, R}, Tasks)}.

% Given a task, choose a node from AvailableHosts that can be used to
% run the task.
-spec choose_host(task(), [host()]) -> false | {ok, host()}.
choose_host({#task_spec{}, #task_run{host = none, failed_hosts = Blacklist}},
            AvailableHosts) ->
    Candidates = gb_sets:subtract(gb_sets:from_list(AvailableHosts), Blacklist),
    case gb_sets:to_list(Candidates) of
        [] -> false;
        [Host|_] -> {ok, Host}
    end;
choose_host({#task_spec{}, #task_run{host = Host}}, AvailableHosts) ->
    case lists:member(Host, AvailableHosts) of
        true -> {ok, Host};
        false -> false
    end.

% Pop the first task in Nodes that can be run
-spec pop_suitable(task_map(), [host()], [host()]) -> {schedule_result(), task_map()}.
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
-spec find_suitable([task()], none|host(), [host()], task_map(), [host()]) ->
    {ok, task(), host(), host()} | false.
find_suitable([], _, [], _, _) -> false;
find_suitable([], _, [Node|R], Tasks, AvailableNodes) ->
    {_, _, L} = gb_trees:get(Node, Tasks),
    find_suitable(L, Node, R, Tasks, AvailableNodes);
find_suitable([T|R], Node, RestNodes, Tasks, AvailableNodes) ->
    case choose_host(T, AvailableNodes) of
        false -> find_suitable(R, Node, RestNodes, Tasks, AvailableNodes);
        {ok, Target} -> {ok, T, Node, Target}
    end.

% return nodes that don't have any local tasks assigned to them
-spec empty_nodes(task_map(), [host()]) -> [host()].
empty_nodes(Tasks, AvailableNodes) ->
    filter_nodes(Tasks, AvailableNodes, false).

% return nodes that have at least one local task assigned to them
-spec datalocal_nodes(task_map(), [host()]) -> [host()].
datalocal_nodes(Tasks, AvailableNodes) ->
    filter_nodes(Tasks, AvailableNodes, true).

-spec filter_nodes(task_map(), [host()], boolean()) -> [host()].
filter_nodes(Tasks, AvailableNodes, Local) ->
    [Node || Node <- AvailableNodes,
             case gb_trees:lookup(Node, Tasks) of
                 none -> false =:= Local;
                 {value, {0, _, _}} -> false =:= Local;
                 _ -> true =:= Local
             end].

%% ===================================================================
%% Host assignment for a new runnable task.

-spec assign_task(pid(), task(), loadstats(), task_map(), host_set()) -> task_map().
assign_task(JC, {#task_spec{taskid = TaskId}, #task_run{host = Host}} = T,
            _LoadStats, HQ, Cluster)
  when Host =/= none ->
    % The job coordinator has already performed a host assignment
    % (usually for a task's first run).
    case gb_sets:is_member(Host, Cluster) of
        true ->
            % The assignment is usable.
            {N, Count, Tasks} = get_default(Host, HQ, {0, 0, []}),
            gb_trees:enter(Host, {N + 1, Count + 1, [T|Tasks]}, HQ);
        false ->
            % Fail the task due to unusable assignment.
            Err = {error, {unknown_host, Host}},
            job_coordinator:task_done(JC, {Err, TaskId, Host}),
            HQ
    end;
assign_task(JC, {TS, #task_run{failed_hosts = Blacklist, input = Inputs} = TR} = T,
            LoadStats, HQ, Cluster) ->
    % Check if we've exhausted all hosts in the cluster.  Note that
    % the job-coordinator also does this test, but since some time may
    % have elapsed since that check, we need to do it here too to
    % avoid race conditions.
    HostCandidates = gb_sets:subtract(Cluster, Blacklist),
    case gb_sets:is_empty(HostCandidates) of
        true ->
            % We have exhausted all cluster hosts.
            case gb_sets:is_empty(Cluster) orelse gb_sets:is_empty(Blacklist) of
                true ->
                    % Mark the task as unassigned.
                    {N, Count, Tasks} = get_default(none, HQ, {0, 0, []}),
                    gb_trees:enter(none, {N + 1, Count + 1, [T|Tasks]}, HQ);
                false ->
                    % Reset blacklist and retry.
                    T1 = {TS, TR#task_run{failed_hosts = gb_sets:empty()}},
                    assign_task(JC, T1, LoadStats, HQ, Cluster)
            end;
        false ->
            % We need to select the best host for the task.
            Host = best_host(HostCandidates, Inputs),
            {N, Count, Tasks} = get_default(Host, HQ, {0, 0, []}),
            gb_trees:enter(Host, {N + 1, Count + 1, [T|Tasks]}, HQ)
    end.

-spec best_host(host_set(), [{input_id(), data_input()}]) -> url_host().
best_host(HostCandidates, Inputs) ->
    ByLocality = lists:flatten([pipeline_utils:ranked_locations(I)
                                || {_Id, I} <- Inputs]),
    % Prefer host with most local input.
    case lists:filter(fun ({_, H}) -> gb_sets:is_member(H, HostCandidates) end,
                      ByLocality)
    of
        [{_S, H}|_] -> H;
        []          -> none
    end.

% The cluster topology might have changed, with new hosts appearing or
% existing ones removed.
-spec reassign_tasks(pid(), task_map(), loadstats(), host_set()) -> task_map().
reassign_tasks(JC, HQ, LoadStats, Cluster) ->
    {OHQ, NHQ} =
        lists:foldl(
          fun(Host, {OHQ, NHQ}) ->
                  case gb_trees:lookup(Host, OHQ) of
                      {value, TList} ->
                          {gb_trees:delete(Host, OHQ),
                           gb_trees:insert(Host, TList, NHQ)};
                      none ->
                          {OHQ, NHQ}
                  end
          end, {HQ, gb_trees:empty()}, gb_sets:to_list(Cluster)),
    lists:foldl(
      fun(T, NHQ0) -> assign_task(JC, T, LoadStats, NHQ0, Cluster) end,
      gb_trees:insert(none, {0, 0, []}, NHQ),
      lists:flatten([T || {_, _, T} <- gb_trees:values(OHQ)])).
