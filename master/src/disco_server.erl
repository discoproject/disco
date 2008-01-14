
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
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
        {ok, {}}.
 
handle_call({update_config_table, Config}, _From, State) ->
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
        {reply, ok, State};

handle_call({new_worker, 
        {JobName, PartID, Mode, PrefNode, Input}}, From, State) ->
       
       case choose_node(PrefNode) of 
                [] -> {reply, busy, State};
                [Node|_] -> ets:update_counter(node_load, Node, 1),
                            disco_worker:start_link(
                                [From, JobName, PartID, Mode, Node, Input]),
                            {reply, ok, State};
        end,

handle_call({add_job_event, JobName, Event}, From, State) ->
        ets:insert(job_events, {JobName, {now(), Event}}),
        {reply, ok, State}.

handle_call({get_job_events, JobName}, From, State) ->
        case ets:lookup(job_events, JobName) of
                [] -> {reply, {ok, []}, State};
                [{JobName, Events}] -> {reply, {ok, Events}, State}
        end.

handle_info({'EXIT', Pid, Reason}, State) ->
        if Pid == self() -> 
                error_logger:info_report(["Disco server dies on error!", Reason]),
                {stop, stop_requested, State};
        true ->
                {Pid, {JobName, From, Node}} = ets:lookup(active_workers, Pid),
                ets:delete(active_workers, Pid),
                ets:update_counter(node_load, Node, -1),
                case Reason of
                        {job_ok, Result} -> 
                                Msg = "Worker done",
                                From ! {job_ok, Result};
                        {job_error, Error} ->
                                Msg = ["Job error", Error],
                                From ! {job_error, Error};
                        Error ->
                                Msg = ["Worker dies on error", Reason],
                                From ! {error, Reason}
                end,
                error_logger:error_report(
                        [Msg, JobName, "node", Node, "pid", Pid])
        end.

node_busy([], _) -> true;
node_busy([{_, Load}], [{_, MaxLoad}]) -> Load >= MaxLoad.

choose_node(PrefNode) ->
        lists:dropwhile(
                fun({Node, _}) ->
                        node_busy(ets:lookup(node_load, Node),
                                  ets:lookup(config_table, Node))
                end, [{PrefNode, 0}|ets:tab2list(node_load)]).

% callback stubs

terminate(_Reason, _State) -> {}.

handle_cast(_Cast, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
