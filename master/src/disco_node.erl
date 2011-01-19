-module(disco_node).
-behaviour(gen_server).

-export([home/0, spawn_node/1, start_node/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("disco.hrl").

-record(state, {master :: node()}).

-define(OPTS, [{timeout, 60000}]).

home() ->
    filename:join(disco:get_setting("DISCO_DATA"), disco:host(node())).

spawn_node(Slave) ->
    spawn_link(Slave, ?MODULE, start_node, [node()]).

start_node(Master) ->
    error_logger:info_report({"starting node @", node()}),
    process_flag(trap_exit, true),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Master, ?OPTS) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Error} ->
            exit(Error)
    end,
    receive
        {'EXIT', _From, Reason} ->
            exit(Reason)
    end.

init(Master) ->
    process_flag(trap_exit, true),
    ddfs_node:start(Master),
    temp_gc:start(Master),
    {ok, #state{master = Master}}.

handle_cast({start_worker, Manager, Task}, #state{master = Master} = State) ->
    spawn(fun () ->
                  gen_server:reply(Manager, disco_worker:start_worker(Master, Task))
          end),
    {noreply, State}.

handle_call(Message, From, State) ->
    {stop, {unexpected_call, Message, From}, State}.

handle_info({'EXIT', _From, normal}, State) ->
    {noreply, State};
handle_info({'EXIT', From, {already_started, _Pid}}, State) ->
    error_logger:info_report({"Already started", node(From)}),
    {noreply, State};
handle_info({'EXIT', From, Reason}, State) ->
    error_logger:info_report({"Node failed", node(From), Reason}),
    {stop, Reason, State}.

terminate(_Reason, _State) ->
    {}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
