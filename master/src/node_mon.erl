-module(node_mon).
-export([start_link/2]).

-include("common_types.hrl").
-include("disco.hrl").

-define(RESTART_DELAY, 15000).
-define(SLAVE_ARGS, "+K true -connect_all false").
-define(RPC_CALL_TIMEOUT, 30000).
-define(RPC_RETRY_TIMEOUT, 120000).

-spec start_link(host(), node_ports()) -> pid().
start_link(Host, Ports) ->
    spawn_link(fun() -> spawn_node(Host, Ports) end).

-spec spawn_node(host(), node_ports(), boolean()) -> ok.
spawn_node(Host, Ports) ->
    process_flag(trap_exit, true),
    spawn_node(Host, Ports, disco:local_cluster()).
spawn_node(Host, Ports, true) ->
    % When simulating a local cluster, the simulated host names will
    % cause the is_master check to fail; by-pass the check and force
    % those nodes to be treated as remote (i.e. with both get/put
    % ports).
    do_spawn_node(Host, "localhost", Ports, false);
spawn_node(Host, Ports, false) ->
    do_spawn_node(Host, Host, Ports, is_master(Host)).

-spec do_spawn_node(host(), host(), node_ports(), boolean()) -> ok.
do_spawn_node(Host, RealHost, Ports, IsMaster) ->
    DiscoRoot = disco:get_setting("DISCO_DATA"),
    case {IsMaster, catch slave_start(Host, RealHost)} of
        {true, {ok, Node}} ->
            % start a dummy ddfs_node process for the master, no get or put
            start_ddfs_node(Host, node(), DiscoRoot, Ports, {false, false}),
            % start ddfs_node for the slave on the master node.
            % put enabled, but no get, which is handled by master
            node_monitor(Host, Node, DiscoRoot, Ports, {false, true});
        {false, {ok, Node}} ->
            % normal remote ddfs_node, both put and get enabled
            node_monitor(Host, Node, DiscoRoot, Ports, {true, true});
        {_, {error, {already_running, Node}}} ->
            node_monitor(Host, Node, DiscoRoot, Ports, {not(IsMaster), true});
        {_, {error, timeout}} ->
            lager:info("Connection timed out to ~p", [Host]),
            disco_server:connection_status(Host, down);
        Error ->
            lager:warning("Spawning node at ~p failed: ~p", [Host, Error]),
            disco_server:connection_status(Host, down)
    end,
    timer:sleep(?RESTART_DELAY).

-spec node_monitor(host(), node(), path(), node_ports(), {boolean(), boolean()})
                  -> ok.
node_monitor(Host, Node, DiscoRoot, Ports, WebConfig) ->
    monitor_node(Node, true),
    start_ddfs_node(Host, Node, DiscoRoot, Ports, WebConfig),
    start_temp_gc(Host, Node, DiscoRoot),
    start_lock_server(Node),
    wait(Host, Node),
    disco_server:connection_status(Host, down).

-spec wait(host(), node()) -> ok.
wait(Host, Node) ->
    receive
        {node_ready, RNode} ->
            lager:info("Node started at ~p (reporting as ~p) on ~p",
                       [Node, RNode, Host]),
            disco_server:connection_status(Host, up),
            wait(Host, Node);
        {is_ready, Pid} ->
            Pid ! node_ready,
            wait(Host, Node);
        {'EXIT', _, already_started} ->
            lager:info("Node already started at ~p on ~p", [Node, Host]),
            wait(Host, Node);
        {'EXIT', _, Reason} ->
            lager:info("Node failed at ~p on ~p: ~p", [Node, Reason, Host]);
        {nodedown, _Node} ->
            lager:info("Node ~p on ~p down", [Node, Host]);
        E ->
            lager:info("Unexpected message to node_mon for ~p: ~p", [Node, E])
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

-spec slave_start(host(), host()) -> {ok, node()} | {error, _}.
slave_start(Host, RealHost) ->
    SlaveName = disco:slave_name(Host),
    lager:info("Starting node ~p on ~p (~p)", [SlaveName, Host, RealHost]),
    slave:start(RealHost,
                SlaveName,
                slave_env(),
                self(),
                disco:get_setting("DISCO_ERLANG")).

-spec is_master(host()) -> boolean().
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

-spec start_temp_gc(host(), node(), path()) -> pid().
start_temp_gc(Host, Node, DiscoRoot) ->
    DataRoot = filename:join(DiscoRoot, Host),
    spawn_link(Node, temp_gc, start_link, [node(), DataRoot]).

-spec start_lock_server(node()) -> pid().
start_lock_server(Node) ->
    spawn_link(Node, fun lock_server:start_link/0).

-spec start_ddfs_node(host(), node(), path(), node_ports(),
                      {boolean(), boolean()}) -> pid().
start_ddfs_node(Host, Node, DiscoRoot,
                #node_ports{put_port = PutPort, get_port = GetPort},
                {GetEnabled, PutEnabled}) ->
    DdfsRoot = disco:ddfs_root(disco:get_setting("DDFS_DATA"), Host),
    Args = [{nodename, Host},
            {disco_root, DiscoRoot}, {ddfs_root, DdfsRoot},
            {get_port, GetPort}, {put_port, PutPort},
            {get_enabled, GetEnabled}, {put_enabled, PutEnabled}],
    spawn_link(Node, ddfs_node, start_link, [Args, self()]).
