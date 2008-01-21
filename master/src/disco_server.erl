
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
        ets:new(active_workers, [named_table, public]),
        ets:new(job_events, [named_table, duplicate_bag]),
        ets:new(node_load, [named_table]),
        ets:new(blacklist, [named_table]),
        ets:new(node_stats, [named_table]),
        {ok, []}.

handle_call(get_jobnames, _From, State) ->
        Lst = ets:match(job_events, 
                {'$1', {{'$2', '_'}, '_', ['_', start, '$3'|'_']}}),
        {reply, {ok, Lst}, State};

handle_call({get_jobinfo, JobName}, _From, State) ->
        MapNfo = ets:match(job_events,
                {JobName, {{'_', '$1'}, '_', ['_', map_data|'$2']}}),
        %RedNfo = ets:match(job_events,
        %        {JobName, {'$1', '_', ['_', red_data|'$2']}}),
        Res = ets:match(job_events, {JobName, {'_', '_', ['_', ready|'$1']}}),
        Nodes = ets:match(active_workers, {'_', {'_', JobName, '$1', '_'}}),
        {reply, {ok, {MapNfo, Res, Nodes}}, State};

handle_call({get_results, JobName}, _From, State) ->
        Pid = lists:flatten(ets:match(job_events,
                {JobName, {'_', '_', ['_', map_data, '$1'|'_']}})),
        Res = lists:map(fun({P, X}) -> [P, list_to_binary(X)] end, 
                lists:flatten(ets:match(job_events, {JobName, 
                        {'_', '_', ['_', ready|'$1']}}))),
        {reply, {ok, Pid, Res}, State};

        


handle_call({get_nodeinfo, all}, _From, State) ->
        Active = ets:match(active_workers, {'_', {'_', '$2', '$1', '_'}}),
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
                        {'_', {'_', '$1', Node, '_'}}),
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

handle_call({new_worker, {JobName, PartID, Mode, PrefNode, Input, Data}},
                {Pid, _}, WaitQueue) when length(WaitQueue) > 0 ->
        
        Req = #job{jobname = JobName, partid = PartID, mode = Mode,
                    prefnode = PrefNode, input = Input, data = Data,
                    from = Pid},

        event(JobName, "~s:~B added to waitlist", [Mode, PartID], []),
        {reply, {ok, wait}, WaitQueue ++ [Req]};

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

handle_call({kill_job, JobName}, _From, WaitQueue) ->
        lists:foreach(fun([Pid]) ->
                gen_server:call(Pid, kill_worker)
        end, ets:match(active_workers, {'$1', {'_', JobName, '_', '_'}})),
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

clean_worker(Pid, ReplyType, Msg, WaitQueue) ->
        {V, Nfo} = case ets:lookup(active_workers, Pid) of
                        [] -> event("[master]",
                                "WARN: Trying to clean an unknown worker",
                                        [], []),
                              {false, none};
                        R -> {true, R}
                   end,
        if V ->
                [{_, {From, _JobName, Node, PartID}}] = Nfo,
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
        PrefBusy = node_busy(ets:lookup(node_load, PrefNode),
                         ets:lookup(config_table, PrefNode)),
        if PrefBusy ->
                AllNodes = ets:tab2list(node_load),
                AvailableNodes = lists:filter(fun({Node, _Load} = X) -> 
                        not node_busy([X], ets:lookup(config_table, Node))
                end, AllNodes),

                BlackNodes = TaskBlackNodes ++ 
                        lists:flatten(ets:match(blacklist, {'$1', '_'})),

                AllowedNodes = lists:filter(fun({Node, _Load}) ->
                        not lists:member(Node, BlackNodes)
                end, AvailableNodes),

                if length(AvailableNodes) == 0 -> busy;
                length(AllowedNodes) == 0 -> 
                        {all_bad, length(TaskBlackNodes), length(AllNodes)};
                true -> 
                        [{Node, _}|_] = lists:keysort(2, AllowedNodes),
                        Node
                end;
        true ->
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

format_timestamp(Tstamp) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        DateStr ++ TimeStr.

%format_event({Tstamp, Host, [Msg|_]}) ->
%        {obj, [{tstamp, list_to_binary(format_timestamp(Tstamp))},
%               {host, list_to_binary(Host)},
%               {msg, list_to_binary(Msg)}]}.

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
