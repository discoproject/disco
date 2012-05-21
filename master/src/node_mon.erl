-module(node_mon).
-export([start_link/2]).

-include("disco.hrl").

-define(RESTART_DELAY, 15000).
-define(SLAVE_ARGS, "+K true -connect_all false").
-define(RPC_CALL_TIMEOUT, 30000).
-define(RPC_RETRY_TIMEOUT, 120000).

-spec start_link(host_name(), node_spec()) -> pid().
start_link(Host, Spec) ->
    spawn_link(fun() -> spawn_node(Host, Spec) end).

-spec spawn_node(host_name(), node_spec()) -> 'ok'.
spawn_node(Host, Spec) ->
    process_flag(trap_exit, true),
    spawn_node(Host, Spec, is_master(Host)).

-spec spawn_node(host_name(), node_spec(), boolean()) -> 'ok'.
spawn_node(Host, Spec, IsMaster) ->
    case {IsMaster, catch slave_start(Host, Spec)} of
        {true, {ok, Node}} ->
            % start a dummy ddfs_node process for the master, no get or put
            start_ddfs_node(node(), Spec, {false, false}),
            % start ddfs_node for the slave on the master node.
            % put enabled, but no get, which is handled by master
            node_monitor(Host, Node, Spec, {false, true});
        {false, {ok, Node}} ->
            % normal remote ddfs_node, both put and get enabled
            node_monitor(Host, Node, Spec, {true, true});
        {_, {error, {already_running, Node}}} ->
            node_monitor(Host, Node, Spec, {not(IsMaster), true});
        {_, {error, timeout}} ->
            lager:info("Connection timed out to ~p", [Host]),
            disco_server:connection_status(Host, down);
        Error ->
            lager:warning("Spawning node at ~p failed: ~p", [Host, Error]),
            disco_server:connection_status(Host, down)
    end,
    timer:sleep(?RESTART_DELAY).

-spec node_monitor(host_name(), node(), node_spec(), {boolean(), boolean()})
                  -> 'ok'.
node_monitor(Host, Node, Spec, WebConfig) ->
    monitor_node(Node, true),
    start_ddfs_node(Node, Spec, WebConfig),
    start_temp_gc(Host, Node, Spec),
    start_lock_server(Node),
    disco_server:connection_status(Host, up),
    wait(Node),
    disco_server:connection_status(Host, down).

-spec wait(node()) -> 'ok'.
wait(Node) ->
    receive
        {is_ready, Pid} ->
            Pid ! node_ready,
            wait(Node);
        {'EXIT', _, already_started} ->
            lager:info("Node already started at ~p", [Node]),
            wait(Node);
        {'EXIT', _, Reason} ->
            lager:info("Node failed at ~p: ~p", [Node, Reason]);
        {nodedown, _Node} ->
            lager:info("Node ~p down", [Node]);
        E ->
            lager:info("Unexpected message: ~p", [E])
    end.

slave_env() ->
    Home = disco:get_setting("DISCO_MASTER_HOME"),
    lists:flatten([?SLAVE_ARGS,
                   [io_lib:format(" -pa ~s/ebin/~s", [Home, Dir])
                    || Dir <- [""]],
                   [io_lib:format(" -pa ~s/deps/~s/ebin", [Home, Dir])
                    || Dir <- ["mochiweb", "lager"]],
                   [io_lib:format(" -env ~s '~s'", [S, disco:get_setting(S)])
                    || S <- disco:settings()]]).

-spec slave_start(host_name(), node_spec()) -> {'ok', node()} | {'error', _}.
slave_start(Host, #node_spec{host = RealHost, slave_name = SlaveName}) ->
    lager:info("Starting node at ~p (using ~p)", [Host, RealHost]),
    slave:start(RealHost,
                SlaveName,
                slave_env(),
                self(),
                disco:get_setting("DISCO_ERLANG")).

-spec is_master(host_name()) -> boolean().
is_master(Host) ->
    % the underlying tcp connection used by net_adm:names() may hang,
    % so we use a timed rpc.
    case rpc:call(node(), net_adm, names, [Host], ?RPC_CALL_TIMEOUT) of
        {ok, Names} ->
            Master = string:sub_word(atom_to_list(node()), 1, $@),
            lists:keymember(Master, 1, Names);
        {error, address} ->
            % no epmd running, can't be master
            false;
        R ->
            % retry the connection, after a while.
            lager:warning("net_adm:names() failed for ~p: ~p", [Host, R]),
            timer:sleep(?RPC_RETRY_TIMEOUT),
            is_master(Host)
    end.

-spec start_temp_gc(host_name(), node(), node_spec()) -> pid().
start_temp_gc(Host, Node, #node_spec{disco_root = DiscoRoot}) ->
    DataRoot = filename:join(DiscoRoot, Host),
    spawn_link(Node, temp_gc, start_link, [node(), DataRoot]).

-spec start_lock_server(node()) -> pid().
start_lock_server(Node) ->
    spawn_link(Node, fun lock_server:start_link/0).

-spec start_ddfs_node(node(), node_spec(), {boolean(), boolean()}) -> pid().
start_ddfs_node(Node, #node_spec{put_port = PutPort, get_port = GetPort,
                                 ddfs_root = DdfsRoot, disco_root = DiscoRoot},
                {GetEnabled, PutEnabled}) ->
    Args = [{nodename, disco:host(Node)},
            {ddfs_root, DdfsRoot}, {disco_root, DiscoRoot},
            {put_port, PutPort}, {get_port, GetPort},
            {get_enabled, GetEnabled},
            {put_enabled, PutEnabled}],
    spawn_link(Node, ddfs_node, start_link, [Args]).
