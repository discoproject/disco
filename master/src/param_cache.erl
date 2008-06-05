-module(param_cache).
-behaviour(gen_server).

-export([start/0, init/1, handle_call/3, handle_cast/2, 
        handle_info/2, terminate/2, code_change/3]).

-define(DISCO_PATH, "/var/disco").

start() ->
        case gen_server:start({local, pcache}, param_cache, [], []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init(_Args) ->
        error_logger:info_report([{"pcache starts at", node()}]),
        ets:new(waiters, [named_table, bag]),
        {ok, []}.

handle_call({get_params, JobName, Parent}, From, State) ->
        {V, F} = case check_file(JobName) of
                {found, FName} -> {false, FName};
                wait -> {true, none};
                new -> spawn_link(fun() ->
                        new_file(JobName, Parent)
                       end), {true, none}
        end,
        if V ->
                ets:insert(waiters, {JobName, {waiter, From}}),
                {noreply, State};
        true ->
                {reply, {ok, F}, State}
        end.

handle_cast({wake_waiters, JobName, Reply}, State) ->
        lists:foreach(fun({_, {_, From}}) ->
                gen_server:reply(From, Reply)
        end, ets:lookup(waiters, JobName)),
        ets:delete(waiters, JobName),
        {noreply, State}.

check_file(JobName) ->
        Dir = filename:join(?DISCO_PATH, JobName),
        M = ets:member(waiters, JobName),
        FName = filename:join(Dir, "params"),
        case file_exists(FName) of
                true -> {found, FName};
                false when M == true -> wait;
                false -> new
        end.

% This is needed since slave IO is directed to the master, thus the 
% standard file modules are unusable.
file_exists(FName) ->
        X = os:cmd("if [ -e " ++ FName ++ " ]; then echo 'x'; fi"),
        X =/= [].

new_file(JobName, Parent) ->
        Parent ! {get_data, self()},
        receive
                {data, Data} -> 
                        Dir = filename:join(?DISCO_PATH, JobName),
                        FName = filename:join(Dir, "params"),
                        os:cmd("mkdir -p " ++ Dir),
                        ok = file:write_file(FName ++ ".partial", Data, [raw]),
                        os:cmd("mv " ++ FName ++ ".partial " ++ FName),
                        Reply = {ok, FName};
                _ -> Reply = {error, invalid_data}
        after 60000 ->
                Reply = {error, data_timeout}
        end,
        gen_server:cast(pcache, {wake_waiters, JobName, Reply}).

% unused

handle_info(_Msg, State) ->
        {noreply, State}. 

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
