-module(node_mon).
-behaviour(gen_server).

-export([start_monitor/1, spawn_manager/2, start_manager/2]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(RESTART_DELAY, 15000).

-record(state, {host :: string(),
                slave :: node()}).

start_monitor(Host) ->
    case gen_server:start_link(?MODULE, Host, [{timeout, 1000}]) of
        {ok, Pid} ->
            gen_server:cast(Pid, start_slave),
            Pid;
        {error, Error} ->
            exit({"Failed to start node monitor", Host, Error})
    end.

spawn_manager(Monitor, Task) ->
    spawn_link(?MODULE, start_manager, [Monitor, Task]).

start_manager(Monitor, Task) ->
    process_flag(trap_exit, true),
    case gen_server:call(Monitor, {start_worker, Task}, 60000) of
        {ok, Worker} ->
            link(Worker);
        {error, Error} ->
            exit(Error)
    end,
    receive
        {'EXIT', Reason} ->
            exit(Reason);
        {'EXIT', _From, Reason} ->
            exit(Reason)
    end.

init(Host) ->
    process_flag(trap_exit, true),
    {ok, #state{host = Host}, 1000}.

handle_cast(start_slave, #state{host = Host} = State) ->
    disco_server:connection_status(Host, down),
    Slave = slave_start(Host),
    erlang:monitor_node(Slave, true),
    disco_node:spawn_node(Slave),
    disco_server:connection_status(Host, up),
    {noreply, State#state{slave = Slave}}.

handle_call({start_worker, Task}, From, #state{slave = Slave} = State) ->
    disco_node:start_worker(Slave, From, Task),
    {noreply, State}.

handle_info(nodedown, #state{slave = Slave} = State) ->
    error_logger:info_report({"slave down", Slave}),
    handle_cast(start_slave, State);
handle_info({'EXIT', _From, Reason}, #state{slave = Slave} = State) ->
    error_logger:info_report({"slave died", Slave, Reason}),
    handle_cast(start_slave, State).

terminate(_Reason, _State) ->
    {}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

slave_start(Host) ->
    Name = disco:slave_name(),
    Home = disco:get_setting("DISCO_MASTER_HOME"),
    Args = lists:flatten(["+K true -connect_all false",
                   [disco:format(" -pa ~s/ebin/~s", [Home, Dir])
                    || Dir <- ["", "mochiweb", "ddfs"]],
                   [disco:format(" -env ~s '~s'", [S, disco:get_setting(S)])
                    || S <- disco:settings()]]),
    Erl = disco:get_setting("DISCO_ERLANG"),
    slave_start(Host, Name, Args, Erl).

slave_start(Host, Name, Args, Erl) ->
    error_logger:info_report({"starting slave @", Host}),
    case slave:start(Host, Name, Args, self(), Erl) of
        {ok, Slave} ->
            Slave;
        {error, {already_running, Slave}} ->
            Slave;
        {error, Reason} ->
            error_logger:warning_report({"failed to start slave", Host, Reason}),
            timer:sleep(?RESTART_DELAY),
            slave_start(Host)
    end.
