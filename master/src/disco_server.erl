
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0, jobhome/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-include("task.hrl").
-record(dnode, {name, blacklisted, slots, num_running, 
                stats_ok, stats_failed, stats_crashed}).

-define(BLACKLIST_PERIOD, 600000).

start_link() ->
        error_logger:info_report([{"DISCO SERVER STARTS"}]),
        case gen_server:start_link({local, disco_server}, 
                        disco_server, [], []) of
                {ok, Server} ->
                        {ok, _} = disco_config:get_config_table(),
                        {ok, Server};
                {error, {already_started, Server}} ->
                        {ok, Server}
        end.

stop() ->
        gen_server:call(disco_server, stop).

jobhome(JobName) when is_list(JobName) -> jobhome(list_to_binary(JobName));
jobhome(JobName) ->
        <<D0:8, _/binary>> = erlang:md5(JobName),
        [D1] = io_lib:format("~.16b", [D0]),
        Prefix = if length(D1) == 1 -> "0"; true -> "" end,
        lists:flatten([Prefix, D1, "/", binary_to_list(JobName), "/"]).

init(_Args) ->
        process_flag(trap_exit, true),

        % active_workers contains pids of all running
        % disco_worker processes.
        ets:new(active_workers, [named_table, private]),

        {ok, Name} = application:get_env(disco_name),
        register(slave_master, spawn_link(fun() ->
                slave_master(lists:flatten([Name, "_slave"]))
        end)),
        {ok, gb_trees:empty()}.


handle_cast({update_config_table, Config}, {Nodes, _}) ->
        error_logger:info_report([{"Config table update"}]),
        NewNodes = lists:foldl(fun({Node, Slots}, NewNodes) ->
                NewNode = case gb_trees:lookup(Node, Nodes) of
                        none -> 
                                #dnode{name = Node,
                                       slots = Slots,
                                       blacklisted = false,
                                       stats_ok = 0,
                                       num_running = 0,
                                       stats_failed = 0,
                                       stats_crashed = 0};
                        {value, N} ->
                                #dnode{name = Node,
                                       slots = Slots,
                                       blacklisted = N#dnode.blacklisted,
                                       stats_ok = N#dnode.stats_ok,
                                       num_running = N#dnode.num_running,
                                       stats_failed = N#dnode.stats_failed,
                                       stats_crashed = N#dnode.stats_crashed}
                end,
                gb_trees:insert(Node, NewNode, NewNodes)
        end, gb_trees:empty(), Config),

        gen_server:cast(scheduler, {update_nodes, Config}),
        gen_server:cast(self(), schedule_task),
        {noreply, NewNodes};

handle_cast({new_task, Task}, State) ->
        gen_server:cast(scheduler, {new_task, Task}),
        gen_server:cast(self(), schedule_next),
        event_server:event(Task#task.jobname, "~s:~B added to waitlist",
                [Task#task.mode, Task#task.taskid], []),
        {noreply, State};

handle_cast(schedule_next, Nodes) ->
        AvailableNodes = [N#dnode.name || #dnode{slots = S, num_running = N}
                                <- gb_trees:values(Nodes), S > N],
        if AvailableNodes =/= [] ->
                case gen_server:call(scheduler, {next_job, AvailableNodes}) of
                        {ok, {JobSchedPid, {Node, Task}}} -> 
                                
                                WorkerPid = start_worker(Node, Task),
                                gen_server:cast(JobSchedPid,
                                        {task_started, Node, WorkerPid}),

                                M = gb_trees:get(Node, Nodes),
                                UNodes = gb_trees:update(Node, 
                                        M#dnode{num_running = 
                                                M#dnode.num_running + 1},
                                                         Nodes),
                                {noreply, UNodes};
                        nojobs -> 
                                {noreply, Nodes}
                end;
        true -> {noreply, Nodes}
        end;

handle_cast({update_stats, Node, ReplyType}, Nodes) ->
        N = gb_trees:get(Node, Nodes),
        M = N#dnode{num_running = N#dnode.num_running - 1},
        M0 = case ReplyType of
                job_ok ->
                        M#dnode{stats_ok = M#dnode.stats_ok + 1};
                data_error ->
                        M#dnode{stats_failed = M#dnode.stats_failed + 1};
                job_error ->
                        M#dnode{stats_crashed = M#dnode.stats_crashed + 1};
                error ->
                        M#dnode{stats_crashed = M#dnode.stats_crashed + 1}
        end,
        {noreply, gb_trees:update(Node, M0, Nodes)};

handle_cast({exit_worker, Pid, {ReplyType, Msg}}, State) ->
        clean_worker(Pid, ReplyType, Msg),
        {noreply, State}.

handle_call({get_active, JobName}, _From, State) ->
        Tasks = ets:match(active_workers, {'_', {'_', JobName, '_', '$1', '_'}}),
        Nodes = ets:match(active_workers, {'_', {'_', JobName, '$1', '_', '_'}}),
        {reply, {ok, {Nodes, Tasks}}, State};

handle_call({get_nodeinfo, all}, _From, Nodes) ->
        Active = ets:match(active_workers, {'_', {'_', '$2', '$1', '_', '_'}}),
        Available = lists:map(fun(N) ->
                {obj, [{node, list_to_binary(N#dnode.name)},
                       {job_ok, N#dnode.stats_ok},
                       {data_error, N#dnode.stats_failed},
                       {error, N#dnode.stats_crashed}, 
                       {max_workers, N#dnode.slots},
                       {blacklisted, N#dnode.blacklisted}]}
        end, Nodes),
        {reply, {ok, {Available, Active}}, Nodes};

handle_call(get_num_cores, _, Nodes) ->
        NumCores = lists:sum([N#dnode.slots || N <- gb_trees:values(Nodes)]),
        {reply, {ok, NumCores}, Nodes};

handle_call({kill_job, JobName}, _From, State) ->
        event_server:event(JobName, "WARN: Job killed", [], []),
        JobPid = lists:foldl(fun([WorkerPid, JobPid], _) ->
                exit(WorkerPid, kill_worker),
                JobPid
        end, none, ets:match(active_workers,
                {'$1', {'$2', JobName, '_', '_', '_'}})),
        exit(JobPid, kill_worker),
        {reply, ok, State};

handle_call({clean_job, JobName}, From, State) ->
        handle_call({kill_job, JobName}, From, State),
        gen_server:cast(event_server, {clean_job, JobName}),
        {reply, ok, State};

handle_call({purge_job, JobName}, From, Nodes) ->
        % SECURITY NOTE! This function leads to the following command
        % being executed:
        %
        % os:cmd("rm -Rf " ++ filename:join([Root, JobName]))
        %
        % Evidently, if JobName is not checked correctly, this function
        % can be used to remove any directory in the system. This function
        % is totally unsuitable for untrusted environments!
        
        C0 = string:chr(JobName, $.) + string:chr(JobName, $/),
        C1 = string:chr(JobName, $@),
        if C0 =/= 0 orelse C1 == 0 ->
                error_logger:warning_report(
                        {"Tried to purge an invalid job", JobName});
        true ->
                {ok, Root} = application:get_env(disco_root),
                handle_call({clean_job, JobName}, From, Nodes),
                Nodes = [lists:flatten(["dir://", Node, "/", Node, "/",
                                jobhome(JobName), "/null"]) ||
                                        #dnode{name = Node} <- Nodes],
                garbage_collect:remove_job(Nodes),
                garbage_collect:remove_dir(filename:join([Root, jobhome(JobName)]))
        end,
        {reply, ok, Nodes};

handle_call({blacklist, Node}, _From, Nodes) ->
        {reply, ok, toggle_blacklist(Node, Nodes, true)};

handle_call({whitelist, Node}, _From, Nodes) ->
        {reply, ok, toggle_blacklist(Node, Nodes, false)}.

handle_info({'EXIT', Pid, Reason}, State) ->
        if Pid == self() -> 
                error_logger:info_report(["Disco server dies on error!", Reason]),
                {stop, stop_requested, State};
        Reason == normal -> {noreply, State};
        true -> 
                error_logger:info_report(["Worker killed", Pid, Reason]),
                case Reason of
                        {data_error, Input} -> clean_worker(Pid, data_error, 
                                {"Worker failure", Input});
                        kill_worker -> clean_worker(Pid, job_error, "");
                        _ -> clean_worker(Pid, error, Reason)
                end,
                {noreply, State}
        end.

toggle_blacklist(Node, Nodes, IsBlacklisted) ->
        UpdatedNodes = lists:map(fun
                (#dnode{name = Node} = N) ->
                        N#dnode{blacklisted = IsBlacklisted};
                (N) -> N
        end, Nodes),
        Config = [{N#dnode.name, N#dnode.slots} ||
                #dnode{blacklisted = false} = N <- UpdatedNodes],
        gen_server:cast(scheduler, {update_nodes, Config}),
        gen_server:cast(self(), schedule_next),
        UpdatedNodes.

% clean_worker() gets called whenever a disco_worker process dies, either
% normally or abnormally. It removes the worker from the active_workers 
% table and notifies the corresponding job coordinator about the worker's
% status.
clean_worker(Pid, ReplyType, Msg) ->
        {V, Nfo} = case ets:lookup(active_workers, Pid) of
                        [] -> 
                                error_logger:warning_report(
                                        {"clean_worker: unknown pid",
                                                Pid, ReplyType, Msg}),
                                {false, none};
                        R -> {true, R}
                   end,
        if V ->
                [{_, {From, _JobName, Node, _Mode, PartID}}] = Nfo,
                ets:delete(active_workers, Pid),
                From ! {ReplyType, Msg, {Node, PartID}},
                gen_server:cast(self(), {update_stats, Node, ReplyType});
        true -> ok
        end,
        gen_server:cast(self(), schedule_next).


start_worker(Node, T) ->
        event_server:event(T#task.jobname, "~s:~B assigned to ~s",
                [T#task.mode, T#task.taskid, Node], []),
        Pid = spawn_link(disco_worker, start_link_remote, 
                [self(), whereis(event_server), Node, T]),
        ets:insert(active_workers, 
                        {Pid, {T#task.from, 
                               T#task.jobname,
                               Node,
                               T#task.mode,
                               T#task.taskid}}),
        Pid.

% slave:start() contains a race condition, thus it is not safe to call it
% simultaneously in many parallel processes. Instead, we serialize the calls
% through slave_master().
slave_master(SlaveName) ->
        receive
                {start, Pid, Node, Args} ->
                        launch(case application:get_env(disco_slaves_os) of
                                {ok, "osx"} ->
                                        fun() -> 
                                                slave:start(list_to_atom(Node),
                                                SlaveName, Args, self(),
                                                "/usr/libexec/StartupItemContext erl")
                                        end;
                                _ ->
                                        fun() ->
                                                slave:start_link(
                                                        list_to_atom(Node),
                                                        SlaveName, Args)
                                        end
                        end, Pid, Node),
                        slave_master(SlaveName)
        end.

launch(F, Pid, Node) ->
        case catch F() of
                {ok, _} -> 
                        error_logger:info_report({"New slave at ", Node}),
                        Pid ! slave_started;
                {error, {already_running, _}} -> ok;
                {error, timeout} ->
                        Pid ! {slave_failed, lists:flatten(
                                ["Couldn't connect to ", Node, " (timeout). ",
                                "Node blacklisted temporarily."])},
                        spawn_link(fun() -> blacklist_guard(Node) end);
                X ->
                        error_logger:warning_report(
                                {"Couldn't start slave at ", Node, X}),
                        Pid ! {slave_failed, lists:flatten(
                                ["Couldn't connect to ", Node,
                                ". See logs for more information. ",
                                "Node blacklisted temporarily."])},
                        spawn_link(fun() -> blacklist_guard(Node) end)
        end.

blacklist_guard(Node) ->
        error_logger:info_report({"Blacklisting", Node,
                "for", ?BLACKLIST_PERIOD, "ms."}), 
        gen_server:call(disco_server, {blacklist, Node}),
        timer:sleep(?BLACKLIST_PERIOD),
        gen_server:call(disco_server, {whitelist, Node}),
        error_logger:info_report({"Quarantine ended for", Node}).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

