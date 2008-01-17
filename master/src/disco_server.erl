
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0, format_event/1, event/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

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
        {ok, []}.
 
handle_call({update_config_table, Config}, _From, WaitQueue) ->
        error_logger:info_report([{'Config table update'}]),
        case ets:info(config_table) of
                undefined -> none;
                _ -> ets:delete(config_table)
        end,
        ets:new(config_table, [named_table, ordered_set]),
        ets:insert(config_table, Config),
        lists:foreach(fun({Node, _}) -> 
                ets:insert_new(node_load, {Node, 0})
        end, Config),
        {reply, ok, schedule_waiter(WaitQueue, [])};

handle_call({new_worker, {JobName, PartID, Mode, PrefNode, Input} = Req},
                From, WaitQueue) when length(WaitQueue) > 0 ->
        
        event(JobName, "~s:~w added to waitlist", [Mode, PartID], []),
        {reply, {ok, wait}, WaitQueue ++ {From, Req}};

handle_call({new_worker, {JobName, PartID, Mode, PrefNode, Input} = Req},
                From, WaitQueue) when length(WaitQueue) == 0 ->

        case try_new_worker({From, Req}) of
                {wait, _} -> event(JobName, "~s:~w added to waitlist",
                                [Mode, PartID], []),
                        {reply, {ok, wait}, WaitQueue ++ {From, Req}};
                ok -> {reply, {ok, working}, WaitQueue}
        end;

handle_call({kill_job, JobName}, From, State) ->
        {reply, ok, State};

handle_call({add_job_event, Host, JobName, Event}, From, State) ->
        error_logger:info_report(["<event>", Host, JobName, Event]),
        ets:insert(job_events, {JobName, {now(), Host, Event}}),
        {reply, ok, State};

handle_call({get_job_events, JobName}, From, State) ->
        case ets:lookup(job_events, JobName) of
                [] -> {reply, {ok, []}, State};
                Events -> {_, EventList} = lists:unzip(Events),
                          {reply, {ok, EventList}, State}
        end.

handle_info({'EXIT', Pid, Reason}, WaitQueue) ->
        if Pid == self() -> 
                error_logger:info_report(["Disco server dies on error!", Reason]),
                {stop, stop_requested, WaitQueue};
        true ->
                {Pid, {From, JobName, Node, PartID}} =
                        ets:lookup(active_workers, Pid),
                ets:delete(active_workers, Pid),
                ets:update_counter(node_load, Node, -1),
                Src = {Node, PartID},
                case Reason of
                        {job_ok, Result} -> 
                                Msg = "Worker done",
                                From ! {job_ok, Result, Src};
                        {data_error, Error} ->
                                Msg = ["Data error", Error],
                                From ! {data_error, Error, Src};
                        {job_error, Error} ->
                                Msg = ["Job error", Error],
                                From ! {job_error, Error, Src};
                        Error ->
                                Msg = ["Worker dies on error", Reason],
                                From ! {error, Reason, Src}
                end,
                error_logger:error_report(
                        [Msg, JobName, "node", Node, "pid", Pid]).
               
                {noreply, schedule_waiter(WaitQueue, [])}
        end.

schedule_waiter([], Skipped) -> lists:reverse(Skipped);
schedule_waiter([Waiter|WaitQueue] = Q, Skipped) ->
        case try_new_worker(Waiter) of
                {wait, busy} -> lists:reverse(Skipped) ++ Q;
                {wait, all_bad} -> 
                        schedule_waiter(WaitQueue, [Waiter|Skipped]);
                ok -> lists:reverse(Skipped) ++ WaitQueue
        end.

node_busy(_, []) -> true;
node_busy([{_, Load}], [{_, MaxLoad}]) -> Load >= MaxLoad.

choose_node({PrefNode, BlackNodes}) ->
        PrefBusy = node_busy(ets:lookup(node_load, PrefNode),
                         ets:lookup(config_table, PrefNode))
        if PrefBusy ->
                AvailableNodes = filter(fun({Node, Load} = X) -> 
                        not node_busy(X, ets:lookup(config_table, Node))
                end, ets:tab2list(node_load)),

                AllowedNodes = filter(fun({Node, Load}) ->
                        not lists:member(Node, BlackNodes)
                end, AvailableNodes),

                if length(AvailableNodes) == 0 -> busy;
                length(AllowedNodes) == 0 -> all_bad;
                true -> 
                        [{Node, _}|_] = lists:keysort(2, AllowedNodes),
                        Node
                end
        true ->
                PrefNode
        end.

try_new_worker({From, {JobName, PartID, Mode, PrefNode, Input}})
        case choose_node(PrefNode) of
                busy -> {wait, busy};
                all_bad -> {wait, all_bad};
                Node -> event(JobName, "~s:~w assigned to ~s",
                                [Mode, PartID, Node], []),
                        ets:update_counter(node_load, Node, 1),
                        {ok, _Pid} = disco_worker:start_link(
                                [From, JobName, PartID, Mode, Node, Input]),
                        ok
        end.

format_event({Tstamp, Host, [Msg|_]}) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        [list_to_binary(X) || X <- [DateStr ++ TimeStr, Host, Msg]].

event(JobName, Format, Args, Params) ->
        gen_server:call(disco_server, {add_job_event, "master", JobName,
                [io_lib:fwrite(Format, Args)|Params]),

% callback stubs

terminate(_Reason, _State) -> {}.

handle_cast(_Cast, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
