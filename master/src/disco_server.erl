
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0, format_timestamp/1, event/4, event/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-record(job, {jobname, partid, mode, prefnode, input, data, from}).

start_link() ->
        error_logger:info_report([{'DISCO SERVER STARTS'}]),
        case gen_server:start_link({local, disco_server}, 
                        disco_server, [], []) of
                {ok, Server} -> {ok, _} = disco_config:get_config_table(),
                                {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

stop() ->
        gen_server:call(disco_server, stop).

init(_Args) ->
        process_flag(trap_exit, true),

        % active_workers contains Pids of all running
        % disco_worker processes.
        ets:new(active_workers, [named_table, public]),
        
        % job_events records all events related to a job
        ets:new(job_events, [named_table, duplicate_bag]),

        % node_laod records how many disco_workers there are
        % running on a node (could be found in active_workers
        % as well). This table exists mainly for convenience and
        % possibly for performance reasons.
        ets:new(node_load, [named_table]),

        % blacklist contains globally blacklisted nodes 
        ets:new(blacklist, [named_table]),

        % node_stats contains triples {ok_jobs, failed_jobs, crashed_jobs}
        % for each node.
        ets:new(node_stats, [named_table]),
        {ok, []}.

handle_call(get_jobnames, _From, State) ->
        Lst = ets:match(job_events, 
                {'$1', {{'$2', '_'}, '_', ['_', start, '$3'|'_']}}),
        {reply, {ok, Lst}, State};

handle_call({get_jobinfo, JobName}, _From, State) ->
        MapNfo = ets:match(job_events,
                {JobName, {{'_', '$1'}, '_', ['_', map_data|'$2']}}),
        
        Res = ets:match(job_events, {JobName, {'_', '_', ['_', ready|'$1']}}),
        Ready = ets:match(job_events,
                {JobName, {'_', '_', ['_', task_ready|'$1']}}),
        Failed = ets:match(job_events,
                {JobName, {'_', '_', ['_', task_failed|'$1']}}),

        Tasks = ets:match(active_workers, {'_', {'_', JobName, '_', '$1', '_'}}),
        Nodes = ets:match(active_workers, {'_', {'_', JobName, '$1', '_', '_'}}),
        {reply, {ok, {MapNfo, Res, Nodes, Tasks, Ready, Failed}}, State};

handle_call({get_results, JobName}, _From, State) ->
        Pid = lists:flatten(ets:match(job_events,
                {JobName, {'_', '_', ['_', map_data, '$1'|'_']}})),
        Res = lists:map(fun({P, X}) -> [P, list_to_binary(X)] end, 
                lists:flatten(ets:match(job_events, {JobName, 
                        {'_', '_', ['_', ready|'$1']}}))),
        {reply, {ok, Pid, Res}, State};


handle_call({get_nodeinfo, all}, _From, State) ->
        Active = ets:match(active_workers, {'_', {'_', '$2', '$1', '_', '_'}}),
        Available = lists:map(fun({Node, Max}) ->
                [{_, A, B, C}] = ets:lookup(node_stats, Node),
                BL = case ets:lookup(blacklist, Node) of
                        [] -> false;
                        _ -> true
                end,
                {obj, [{node, list_to_binary(Node)},
                       {job_ok, A}, {data_error, B}, {error, C}, 
                       {max_workers, Max}, {blacklisted, BL}]}
        end, ets:tab2list(config_table)),
        {reply, {ok, {Available, Active}}, State};

handle_call({get_nodeinfo, Node}, _From, State) ->
        case ets:lookup(node_stats, Node) of
                [] -> {reply, {ok, []}, State};
                [{_, V}] -> Nfo = ets:match(active_workers, 
                        {'_', {'_', '$1', Node, '_', '_'}}),
                        {reply, {ok, {V, Nfo}}, State}
        end;

handle_call({update_config_table, Config}, _From, WaitQueue) ->
        error_logger:info_report([{'Config table update'}]),
        case ets:info(config_table) of
                undefined -> none;
                _ -> ets:delete(config_table)
        end,
        ets:new(config_table, [named_table, ordered_set]),
        ets:insert(config_table, Config),
        lists:foreach(fun({Node, _}) -> 
                ets:insert_new(node_load, {Node, 0}),
                ets:insert_new(node_stats, {Node, 0, 0, 0})
        end, Config),
        {reply, ok, schedule_waiter(WaitQueue, [])};


% Two ways to start a new job: If the WaitQueue is empty, we may be able
% to start the process right away in try_new_worker(). If not, the job
% is appended to the queue of pending jobs (WaitQueue).
handle_call({new_worker, {JobName, PartID, Mode, PrefNode, Input, Data}},
                {Pid, _}, WaitQueue) when length(WaitQueue) == 0 ->
        
        Req = #job{jobname = JobName, partid = PartID, mode = Mode,
                    prefnode = PrefNode, input = Input, data = Data,
                    from = Pid},

        case try_new_worker(Req) of
                {wait, _} -> event(JobName, "~s:~B added to waitlist",
                                [Mode, PartID], []),
                        {reply, {ok, wait}, WaitQueue ++ [Req]};
                killed -> {reply, {ok, killed}, WaitQueue};
                ok -> {reply, {ok, working}, WaitQueue}
        end;

handle_call({new_worker, {JobName, PartID, Mode, PrefNode, Input, Data}},
                {Pid, _}, WaitQueue) when length(WaitQueue) > 0 ->
        
        Req = #job{jobname = JobName, partid = PartID, mode = Mode,
                    prefnode = PrefNode, input = Input, data = Data,
                    from = Pid},

        event(JobName, "~s:~B added to waitlist", [Mode, PartID], []),
        {reply, {ok, wait}, WaitQueue ++ [Req]};

handle_call({kill_job, JobName}, _From, WaitQueue) ->
        lists:foreach(fun([Pid]) ->
                gen_server:call(Pid, kill_worker)
        end, ets:match(active_workers, {'$1', {'_', JobName, '_', '_', '_'}})),
        {reply, ok, lists:filter(fun(Job) ->
                Job#job.jobname =/= JobName end, WaitQueue)};

handle_call({clean_job, JobName}, From, WaitQueue) ->
        {_, _, NQueue} = handle_call({kill_job, JobName}, From, WaitQueue),
        ets:delete(job_events, JobName),
        {reply, ok, NQueue};

handle_call({blacklist, Node}, _From, State) ->
        event("[master]", "Node ~s blacklisted", [Node], []),
        ets:insert(blacklist, {Node, none}),
        {reply, ok, State};

handle_call({whitelist, Node}, _From, State) ->
        event("[master]", "Node ~s whitelisted", [Node], []),
        ets:delete(blacklist, Node),
        {reply, ok, State};

% There's a small catch with adding a job event: If a job's records have been
% cleaned with clean_job already, we do not want a zombie worker to re-open
% the job's records by adding a new event. Thus only an event with the atom
% start is allowed to initialize records for a new job. Other events are 
% silently ignored if there are no previous records for this job.
handle_call({add_job_event, Host, JobName, [_, start|_] = M}, _From, State) ->
        V = ets:member(job_events, JobName),
        if V -> {reply, job_already_exists, State};
        true -> add_event(Host, JobName, M),
                {reply, ok, State}
        end;

handle_call({add_job_event, Host, JobName, M}, _From, State) ->
        V = ets:member(job_events, JobName),
        if V -> add_event(Host, JobName, M);
        true -> ok end,
        {reply, ok, State};

handle_call({get_job_events, JobName}, _From, State) ->
        case ets:lookup(job_events, JobName) of
                [] -> {reply, {ok, []}, State};
                Events -> {_, EventList} = lists:unzip(Events),
                          {reply, {ok, EventList}, State}
        end;

handle_call({exit_worker, {job_ok, Result}}, {Pid, _}, WaitQueue) ->
        {reply, ok, clean_worker(Pid, job_ok, Result, WaitQueue)};

handle_call({exit_worker, {data_error, Error}}, {Pid, _}, WaitQueue) ->
        {reply, ok, clean_worker(Pid, data_error, Error, WaitQueue)};

handle_call({exit_worker, {job_error, Error}}, {Pid, _}, WaitQueue) ->
        {reply, ok, clean_worker(Pid, job_error, Error, WaitQueue)};

handle_call(Msg, _From, State) ->
        error_logger:info_report(["Invalid call: ", Msg]),
        {reply, error, State}.

handle_info({'EXIT', Pid, Reason}, WaitQueue) ->
        if Pid == self() -> 
                error_logger:info_report(["Disco server dies on error!", Reason]),
                {stop, stop_requested, WaitQueue};
        Reason == normal -> {noreply, WaitQueue};
        true -> {noreply, clean_worker(Pid, error, Reason, WaitQueue)}
        end;

handle_info(Msg, State) ->
        error_logger:info_report(["Unknown message received: ", Msg]),
        {noreply, State}.

% clean_worker() gets called whenever a disco_worker process dies, either
% normally or abnormally. Its main job is to remove the exiting worker
% from the active_workers table and to notify the corresponding job 
% coordinator about the worker's exit status.
clean_worker(Pid, ReplyType, Msg, WaitQueue) ->
        {V, Nfo} = case ets:lookup(active_workers, Pid) of
                        [] -> event("[master]",
                                "WARN: Trying to clean an unknown worker",
                                        [], []),
                              {false, none};
                        R -> {true, R}
                   end,
        if V ->
                [{_, {From, _JobName, Node, _Mode, PartID}}] = Nfo,
                update_stats(Node, ReplyType),
                ets:delete(active_workers, Pid),
                ets:update_counter(node_load, Node, -1),
                From ! {ReplyType, Msg, {Node, PartID}},
                schedule_waiter(WaitQueue, []);
        true -> WaitQueue
        end.

update_stats(Node, job_ok) -> ets:update_counter(node_stats, Node, {2, 1});
update_stats(Node, data_error) -> ets:update_counter(node_stats, Node, {3, 1});
update_stats(Node, job_error) -> ets:update_counter(node_stats, Node, {4, 1});
update_stats(Node, error) -> ets:update_counter(node_stats, Node, {4, 1});
update_stats(_Node, _) -> ok.

% The following functions, schedule_waiter(), node_busy(), choose_node(),
% start_worker() and try_new_worker() handle task scheduling. The basic
% scheme is as follows:
%
% 0) A node becomes available, either due to a task finishing in
%    clean_worker() or a new node or slots being added at 
%    update_config_table().
%
% 1) schedule_waiter() goes through the WaitQueue that includes all pending,
%    not yet running tasks, and tries to get a task running, one by one from
%    the wait queue.
%
% 2) try_new_worker() asks a preferred node from choose_node(). It may report
%    that all the nodes are 100% busy (busy) or that a suitable node could
%    not be found (all_bad). If all goes well, it returns a node name.
%
% 3) If a node name was returned, a new worker is started in start_worker().

schedule_waiter([], Skipped) -> lists:reverse(Skipped);
schedule_waiter([Job|WaitQueue] = Q, Skipped) ->
        case try_new_worker(Job) of
                {wait, busy} -> lists:reverse(Skipped) ++ Q;
                {wait, all_bad} -> 
                        schedule_waiter(WaitQueue, [Job|Skipped]);
                killed -> 
                        schedule_waiter(WaitQueue, Skipped);
                ok -> lists:reverse(Skipped) ++ WaitQueue
        end.

node_busy(_, []) -> true;
node_busy([{_, Load}], [{_, MaxLoad}]) -> Load >= MaxLoad.

choose_node({PrefNode, TaskBlackNodes}) ->
        % Is our preferred choice available?
        PrefBusy = node_busy(ets:lookup(node_load, PrefNode),
                         ets:lookup(config_table, PrefNode)),

        if PrefBusy ->
                % If not, start with all configured nodes..
                AllNodes = ets:tab2list(node_load),

                % ..and choose the ones that are not 100% busy.
                AvailableNodes = lists:filter(fun({Node, _Load} = X) -> 
                        not node_busy([X], ets:lookup(config_table, Node))
                end, AllNodes),

                % From non-busy nodes, remove the ones that have already
                % failed this task (TaskBlackNodes) or that are globally
                % blacklisted (ets-table blacklist).
                BlackNodes = TaskBlackNodes ++ 
                        [X || [X] <- ets:match(blacklist, {'$1', '_'})],
        
                AllowedNodes = lists:filter(fun({Node, _Load}) ->
                        not lists:member(Node, BlackNodes)
                end, AvailableNodes),
                
                if length(AvailableNodes) == 0 -> busy;
                length(AllowedNodes) == 0 -> 
                        {all_bad, length(TaskBlackNodes), length(AllNodes)};
                true -> 
                        % Pick the node with the lowest load.
                        [{Node, _}|_] = lists:keysort(2, AllowedNodes),
                        Node
                end;
        true ->
                % If yes, return the preferred node.
                PrefNode
        end.

start_worker(J, Node) ->
        event(J#job.jobname, "~s:~B assigned to ~s",
                [J#job.mode, J#job.partid, Node], []),
        ets:update_counter(node_load, Node, 1),
        {ok, Pid} = disco_worker:start_link(
                [J#job.from, J#job.jobname, J#job.partid, 
                        J#job.mode, Node, J#job.input, J#job.data]),
        ok = gen_server:call(Pid, start_worker).

try_new_worker(Job) ->
        case choose_node(Job#job.prefnode) of
                busy -> {wait, busy};
                {all_bad, BLen, ALen} when BLen == ALen ->
                        Job#job.from ! {master_error,
                                "Job failed on all available nodes"},
                        killed;
                {all_bad, _, _} -> {wait, all_bad};
                Node -> start_worker(Job, Node)
        end.

% Functions related to event reporting

format_timestamp(Tstamp) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        DateStr ++ TimeStr.

add_event(Host, JobName, [Msg|Params]) ->
        Nu = now(),
        ets:insert(job_events, {JobName, {
                {Nu, list_to_binary(format_timestamp(Nu))},
                list_to_binary(Host), 
                [list_to_binary(Msg)|Params]
        }}).

event(JobName, Format, Args, Params) ->
        event("master", JobName, Format, Args, Params).

event(Host, JobName, Format, Args, Params) ->
        SArgs = lists:map(fun(X) ->
                L = lists:flatlength(io_lib:fwrite("~p", [X])) > 1000,
                if L -> trunc_io:fprint(X, 1000);
                true -> X 
        end end, Args),

        Msg = {add_job_event, Host, JobName,
                [lists:flatten(io_lib:fwrite(Format, SArgs))|Params]},
        Pid = whereis(disco_server),
        if Pid == self() ->
                handle_call(Msg, none, none);
        true ->
                ok = gen_server:call(disco_server, Msg)
        end.

% callback stubs

terminate(_Reason, _State) -> {}.

handle_cast(_Cast, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
