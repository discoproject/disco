
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0, jobhome/1, debug_flags/1, format_time/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-include("task.hrl").
-record(dnode, {name, node_mon, blacklisted, slots, num_running,
        stats_ok, stats_failed, stats_crashed}).

-record(state, {workers, nodes, purged}).

-define(PURGE_TIMEOUT, 86400000). % 24h

start_link() ->
    error_logger:info_report([{"DISCO SERVER STARTS"}]),
    case gen_server:start_link({local, disco_server},
            disco_server, [], debug_flags("disco_server")) of
        {ok, Server} ->
            case catch disco_config:get_config_table() of
                {ok, _Config} ->
                    {ok, Server};
                E ->
                    error_logger:warning_report({"Parsing config failed", E})
            end;
        {error, {already_started, Server}} ->
            {ok, Server}
    end.

stop() ->
    gen_server:call(disco_server, stop).

debug_flags(Server) ->
    case os:getenv("DISCO_DEBUG") of
        "trace" ->
            Root = disco:get_setting("DISCO_MASTER_ROOT"),
            [{debug, [{log_to_file,
                filename:join(Root, Server ++ "_trace.log")}]}];
        _ -> []
    end.

jobhome(JobName) when is_list(JobName) ->
    jobhome(list_to_binary(JobName));
jobhome(JobName) ->
    <<D0:8, _/binary>> = erlang:md5(JobName),
    [D1] = io_lib:format("~.16b", [D0]),
    Prefix = case D1 of [_] -> "0"; _ -> "" end,
    lists:flatten([Prefix, D1, "/", binary_to_list(JobName), "/"]).

format_time(T) ->
    MS = 1000,
    SEC = 1000 * MS,
    MIN = 60 * SEC,
    HOUR = 60 * MIN,
    D = timer:now_diff(now(), T),
    Ms = (D rem SEC) div MS,
    Sec = (D rem MIN) div SEC,
    Min = (D rem HOUR) div MIN,
    Hour = D div HOUR,
    lists:flatten(io_lib:format("~B:~2.10.0B:~2.10.0B.~3.10.0B",
        [Hour, Min, Sec, Ms])).

init(_Args) ->
    process_flag(trap_exit, true),
    {ok, _} = fair_scheduler:start_link(),
    {ok, #state{workers = gb_trees:empty(),
                nodes = gb_trees:empty(),
                purged = gb_trees:empty()}}.

update_nodes(Nodes) ->
    WhiteNodes = [{N#dnode.name, N#dnode.slots} ||
                     #dnode{blacklisted = false} = N <- gb_trees:values(Nodes)],
    DdfsNodes = [{N#dnode.name, not (N#dnode.blacklisted == false)} ||
                    N <- gb_trees:values(Nodes)],
    gen_server:cast(ddfs_master, {update_nodes, DdfsNodes}),
    gen_server:cast(scheduler, {update_nodes, WhiteNodes}),
    gen_server:cast(self(), schedule_next).

handle_cast({update_config_table, Config}, S) ->
    error_logger:info_report([{"Config table update"}]),
    NewNodes = lists:foldl(fun({Node, Slots}, NewNodes) ->
        NewNode = case gb_trees:lookup(Node, S#state.nodes) of
            none ->
                Mon = spawn_link(fun() -> node_mon:spawn_node(Node) end),
                #dnode{name = Node,
                       slots = Slots,
                       node_mon = Mon,
                       blacklisted = false,
                       stats_ok = 0,
                       num_running = 0,
                       stats_failed = 0,
                       stats_crashed = 0};
            {value, N} ->
                #dnode{name = Node,
                       slots = Slots,
                       node_mon = N#dnode.node_mon,
                       blacklisted = N#dnode.blacklisted,
                       stats_ok = N#dnode.stats_ok,
                       num_running = N#dnode.num_running,
                       stats_failed = N#dnode.stats_failed,
                       stats_crashed = N#dnode.stats_crashed}
        end,
        gb_trees:insert(Node, NewNode, NewNodes)
    end, gb_trees:empty(), Config),

    lists:foreach(fun(Node) ->
        case gb_trees:lookup(Node#dnode.name, NewNodes) of
            none ->
                unlink(Node#dnode.node_mon),
                exit(Node#dnode.node_mon, kill);
            _ -> ok
        end
    end, gb_trees:values(S#state.nodes)),
    disco_proxy:update_nodes(gb_trees:keys(NewNodes)),
    update_nodes(NewNodes),
    {noreply, S#state{nodes = NewNodes}};

handle_cast(schedule_next, #state{nodes = Nodes, workers = Workers} = S) ->

    {_, AvailableNodes} = lists:unzip(lists:keysort(1, [{Y, N} ||
        #dnode{slots = X, num_running = Y, name = N, blacklisted = false}
                <- gb_trees:values(Nodes), X > Y])),

    if AvailableNodes =/= [] ->
        case gen_server:call(scheduler, {next_task, AvailableNodes}) of
            {ok, {JobSchedPid, {Node, Task}}} ->
                M = gb_trees:get(Node, Nodes),
                WorkerPid = start_worker(Node, M#dnode.node_mon, Task),
                UWorkers = gb_trees:insert(
                    WorkerPid, {Node, Task}, Workers),
                gen_server:cast(JobSchedPid,
                    {task_started, Node, WorkerPid}),

                UNodes = gb_trees:update(Node,
                    M#dnode{num_running =
                        M#dnode.num_running + 1},
                             Nodes),
                handle_cast(schedule_next,
                    S#state{nodes = UNodes,
                        workers = UWorkers});
            nojobs ->
                {noreply, S}
        end;
    true -> {noreply, S}
    end;

handle_cast({purge_job, JobName}, #state{purged = Purged} = S) ->
    handle_call({clean_job, JobName}, none, S),
    ddfs:delete(ddfs_master, disco:oob_name(JobName)),
    Key = list_to_binary(JobName),
    NPurged =
        case gb_trees:is_defined(Key, Purged) of
            true ->
                Purged;
            false ->
                gb_trees:insert(Key, now(), Purged)
        end,
    {noreply, S#state{purged = NPurged}};

handle_cast({exit_worker, Pid, {Type, _} = Res}, S) ->
    V = gb_trees:lookup(Pid, S#state.workers),
    if V == none -> {noreply, S};
    true ->
        {_, {Node, Task}} = V,
        UWorkers = gb_trees:delete(Pid, S#state.workers),
        Task#task.from ! {Res, Task, Node},
        gen_server:cast(self(), schedule_next),
        update_stats(Node, gb_trees:lookup(Node, S#state.nodes),
            Type, S#state{workers = UWorkers})
    end.

update_stats(_Node, none, _ReplyType, S) -> {noreply, S};
update_stats(Node, {value, N}, ReplyType, S) ->
    M = N#dnode{num_running = N#dnode.num_running - 1},
    M0 = case ReplyType of
        job_ok ->
            M#dnode{stats_ok = M#dnode.stats_ok + 1};
        data_error ->
            M#dnode{stats_failed = M#dnode.stats_failed + 1};
        job_error ->
            M#dnode{stats_crashed = M#dnode.stats_crashed + 1};
        _ ->
            M#dnode{stats_crashed = M#dnode.stats_crashed + 1}
    end,
    {noreply, S#state{nodes = gb_trees:update(Node, M0, S#state.nodes)}}.

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call({new_job, JobName, JobCoord}, _, S) ->
    {reply, catch gen_server:call(scheduler,
        {new_job, JobName, JobCoord}), S};

handle_call({new_task, Task}, _, S) ->
    NodeStats =
        [case gb_trees:lookup(Node, S#state.nodes) of
            none -> {false, Input};
            {value, N} -> {N#dnode.num_running, Input}
         end || {_Url, Node} = Input <- Task#task.input],
    case catch gen_server:call(scheduler, {new_task, Task, NodeStats}) of
        ok ->
            gen_server:cast(self(), schedule_next),
            event_server:event(Task#task.jobname,
                "~s:~B added to waitlist",
                [Task#task.mode, Task#task.taskid], []),
            {reply, ok, S};
        Error ->
            error_logger:warning_report({"Scheduling task failed",
                Task, Error}),
            {reply, failed, S}
    end;

handle_call({get_active, JobName}, _From, #state{workers = Workers} = S) ->
    {Nodes, Tasks} = lists:unzip([{N, M} ||
        {N, #task{mode = M, jobname = X}} <- gb_trees:values(Workers),
            X == JobName]),
    {reply, {ok, {Nodes, Tasks}}, S};

handle_call({get_nodeinfo, all}, _From, S) ->
    Active = [{N, Name} || {N, #task{jobname = Name}}
                <- gb_trees:values(S#state.workers)],
    Available = [{struct, [{node, list_to_binary(N#dnode.name)},
		           {job_ok, N#dnode.stats_ok},
                           {data_error, N#dnode.stats_failed},
                           {error, N#dnode.stats_crashed},
                           {max_workers, N#dnode.slots},
                           {blacklisted, not (N#dnode.blacklisted == false)}]}
                    || N <- gb_trees:values(S#state.nodes)],
    {reply, {ok, {Available, Active}}, S};

handle_call(get_purged, _, #state{purged = Purged} = S) ->
    Now = now(),
    NPurged = gb_trees:from_orddict(
        [{Job, TStamp} || {Job, TStamp} <- gb_trees:to_list(Purged),
            timer:now_diff(Now, TStamp) < ?PURGE_TIMEOUT * 1000]),
    {reply, {ok, gb_trees:keys(NPurged)}, S#state{purged = NPurged}};

handle_call(get_num_cores, _, #state{nodes = Nodes} = S) ->
    NumCores = lists:sum([N#dnode.slots || N <- gb_trees:values(Nodes)]),
    {reply, {ok, NumCores}, S};

handle_call({kill_job, JobName}, _From, S) ->
    event_server:event(JobName, "WARN: Job killed", [], []),
    % Make sure that scheduler don't accept new tasks from this job
    gen_server:cast(scheduler, {job_done, JobName}),
    {reply, ok, S};

handle_call({clean_job, JobName}, From, State) ->
    handle_call({kill_job, JobName}, From, State),
    gen_server:cast(event_server, {clean_job, JobName}),
    {reply, ok, State};

handle_call({blacklist, Node, Token}, _From, #state{nodes = Nodes} = S) ->
    {reply, ok, S#state{nodes =
        toggle_blacklist(Node, Nodes, true, Token)}};

handle_call({whitelist, Node, Token}, _From, #state{nodes = Nodes} = S) ->
    {reply, ok, S#state{nodes =
        toggle_blacklist(Node, Nodes, false, Token)}}.

process_exit(Pid, Msg, Code, S) ->
    process_exit1(gb_trees:lookup(Pid, S#state.workers), Pid, Msg, Code, S).

process_exit1(none, _, _, _, S) -> {noreply, S};
process_exit1({_, {Node, T}}, Pid, Msg, Code, S) ->
    P = io_lib:fwrite("WARN: [~s:~B] ", [T#task.mode, T#task.taskid]),
    event_server:event(Node, T#task.jobname, lists:flatten(P, Msg), [],
            {task_failed, T#task.mode}),
    gen_server:cast(self(), {exit_worker, Pid, {data_error, Code}}),
    {noreply, S}.

handle_info({'EXIT', Pid, normal}, S) ->
    case gb_trees:lookup(Pid, S#state.workers) of
        none -> {noreply, S};
        _ -> error_logger:warning_report(
            {"Task failed to call exit_worker", Pid}),
             process_exit(Pid, "Died unexpectedly without a reason",
            "unexpected", S)
    end;

handle_info({'EXIT', Pid, {worker_dies, {Msg, Args}}}, S) ->
    process_exit(Pid, io_lib:fwrite(Msg, Args), "worker_dies", S);

handle_info({'EXIT', Pid, noconnection}, S) ->
    process_exit(Pid, "Connection lost to the node (network busy?)",
        "noconnection", S);

handle_info({'EXIT', Pid, Reason}, S) when Pid == self() ->
    error_logger:warning_report(["Disco server dies on error!", Reason]),
    {stop, stop_requested, S};

handle_info({'EXIT', Pid, Reason}, S) ->
    process_exit(Pid, io_lib:fwrite("Worked died unexpectedly: ~p",
        [Reason]), "unexpected", S).

toggle_blacklist(Node, Nodes, IsBlacklisted, Token) ->
    UpdatedNodes =
        case gb_trees:lookup(Node, Nodes) of
            % blacklist
            {value, M} when IsBlacklisted == true,
                    M#dnode.blacklisted =/= manual ->
                gb_trees:update(Node,
                    M#dnode{blacklisted = Token}, Nodes);
            % whitelist if token is valid
            {value, M} when Token == any;
                    Token == M#dnode.blacklisted ->
                error_logger:info_report({"Whitelisted", Node}),
                gb_trees:update(Node,
                    M#dnode{blacklisted = false}, Nodes);
            _ -> Nodes
        end,
    update_nodes(UpdatedNodes),
    UpdatedNodes.

start_worker(Node, NodeMon, T) ->
    event_server:event(T#task.jobname, "~s:~B assigned to ~s",
        [T#task.mode, T#task.taskid, Node], []),
    spawn_link(disco_worker, start_link_remote,
        [self(), whereis(event_server), Node, NodeMon, T]).

% callback stubs
terminate(Reason, _State) ->
    error_logger:warning_report({"Disco server dies", Reason}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

