-module(node_mon).
-export([start_link/1]).

-define(BLACKLIST_PERIOD, 600000).
-define(RESTART_DELAY, 10000).
-define(SLAVE_ARGS, "+K true -connect_all false").

start_link(Host) ->
    spawn_link(fun() -> spawn_node(Host) end).

-spec spawn_node(nonempty_string()) -> no_return().
spawn_node(Host) ->
    process_flag(trap_exit, true),
    case catch slave_start(Host) of
        {true, {ok, Node}} ->
            % start a dummy ddfs_node process for the master, no get or put
            start_ddfs_node(node(), {false, false}),
            % start ddfs_node for the slave on the master node.
            % put enabled, but no get, which is handled by master
            node_monitor(Node, {false, true});
        {false, {ok, Node}} ->
            % normal remote ddfs_node, both put and get enabled
            node_monitor(Node, {true, true});
        {_, {error, {already_running, Node}}} ->
            % normal remote ddfs_node, both put and get enabled
            node_monitor(Node, {true, true});
        {_, {error, timeout}} ->
            blacklist(Host);
        Error ->
            error_logger:warning_report(
                {"Spawning node @", Host, "failed for unknown reason", Error}),
            blacklist(Host)
    end,
    flush(),
    timer:sleep(?RESTART_DELAY),
    spawn_node(Host).

-spec node_monitor(node(), {bool(), bool()}) -> _.
node_monitor(Node, WebConfig) ->
    monitor_node(Node, true),
    start_ddfs_node(Node, WebConfig),
    start_temp_gc(Node, disco:host(Node)),
    wait(Node).

-spec wait(node()) -> _.
wait(Node) ->
    receive
        {is_ready, Pid} ->
            Pid ! node_ready,
            wait(Node);
        {'EXIT', _, already_started} ->
            error_logger:info_report({"Already started", Node, self()}),
            wait(Node);
        {'EXIT', _, Reason} ->
            error_logger:info_report({"Node failed", Node, Reason});
        {nodedown, _Node} ->
            error_logger:info_report({"Node", Node, "down"});
        E ->
            error_logger:info_report({"Erroneous message (node_mon)", E})
    end.

flush() ->
    receive
        _ -> flush()
    after
        0 -> true
    end.

slave_env() ->
    Home = disco:get_setting("DISCO_MASTER_HOME"),
    lists:flatten([?SLAVE_ARGS,
                   [io_lib:format(" -pa ~s/ebin/~s", [Home, Dir])
                    || Dir <- ["", "mochiweb", "ddfs"]],
                   [io_lib:format(" -env ~s '~s'", [S, disco:get_setting(S)])
                    || S <- disco:settings()]]).

-spec slave_start(nonempty_string()) -> {bool(), {'ok', node()} | {'error', _}}.
slave_start(Host) ->
    error_logger:info_report({"starting node @", Host}),
    {is_master(Host),
     slave:start(Host,
                 disco:node_name(),
                 slave_env(),
                 self(),
                 disco:get_setting("DISCO_ERLANG"))}.

-spec is_master(nonempty_string()) -> bool().
is_master(Host) ->
    case net_adm:names(Host) of
        {ok, Names} ->
            Master = string:sub_word(atom_to_list(node()), 1, $@),
            lists:keymember(Master, 1, Names);
        _ ->
            false
    end.

-spec start_temp_gc(node(), nonempty_string()) -> pid().
start_temp_gc(Node, Host) ->
    DataRoot = disco:get_setting("DISCO_DATA"),
    GCAfter = list_to_integer(disco:get_setting("DISCO_GC_AFTER")),
    spawn_link(Node, temp_gc, start_link,
               [whereis(disco_server),
                whereis(event_server),
                whereis(ddfs_master),
                DataRoot, Host, GCAfter]).

-spec start_ddfs_node(node(), {bool(), bool()}) -> pid().
start_ddfs_node(Node, {GetEnabled, PutEnabled}) ->
    DdfsRoot = disco:get_setting("DDFS_ROOT"),
    DiscoRoot = disco:get_setting("DISCO_DATA"),
    PutMax = list_to_integer(disco:get_setting("DDFS_PUT_MAX")),
    GetMax = list_to_integer(disco:get_setting("DDFS_GET_MAX")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    GetPort = list_to_integer(disco:get_setting("DISCO_PORT")),
    Args = [{nodename, disco:host(Node)},
            {ddfs_root, DdfsRoot}, {disco_root, DiscoRoot},
            {put_max, PutMax}, {get_max, GetMax},
            {put_port, PutPort}, {get_port, GetPort},
            {get_enabled, GetEnabled},
            {put_enabled, PutEnabled}],
    spawn_link(Node, ddfs_node, start_link, [Args]).

blacklist(Host) ->
    error_logger:info_report({"Blacklisting", Host,
                              "for", ?BLACKLIST_PERIOD, "ms."}),
    Token = now(),
    gen_server:call(disco_server, {blacklist, Host, Token}),
    timer:sleep(?BLACKLIST_PERIOD),
    gen_server:call(disco_server, {whitelist, Host, Token}).
