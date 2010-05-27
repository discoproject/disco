-module(node_mon).
-export([spawn_node/1, slave_node/1, slave_node_safe/1]).

-define(BLACKLIST_PERIOD, 600000).
-define(RESTART_DELAY, 10000).
-define(SLAVE_ARGS, "+K true").

slave_node(Node) ->
    list_to_atom(slave_name() ++ "@" ++ Node).

slave_name() ->
    disco:get_setting("DISCO_NAME") ++ "_slave".

slave_node_safe(Node) ->
    case catch list_to_existing_atom(slave_name() ++ "@" ++ Node) of
        {'EXIT', _} -> false;
        X -> X
    end.

spawn_node(Node) ->
    process_flag(trap_exit, true),
    case catch slave_start(Node) of
        {true, {ok, _Node}} ->
            % start a dummy ddfs_node process for the master, no get or put
            start_ddfs_node(node(), {false, false}),
            % start ddfs_node for the slave on the master node.
            % put enabled, but no get, which is handled by master
            node_monitor(Node, {false, true});
        {false, {ok, _SlaveNode}} ->
            % normal remote ddfs_node, both put and get enabled
            node_monitor(Node, {true, true});
        {_, {error, {already_running, _SlaveNode}}} ->
            % normal remote ddfs_node, both put and get enabled
            node_monitor(Node, {true, true});
        {_, {error, timeout}} ->
            blacklist(Node);
        Error ->
            error_logger:warning_report(
                {"Spawning node", Node, "failed for unknown reason", Error}),
            blacklist(Node)
    end,
    timer:sleep(?RESTART_DELAY),
    spawn_node(Node).

node_monitor(Node, WebConfig) ->
    NodeAtom = slave_node(Node),
    monitor_node(NodeAtom, true),
    start_ddfs_node(NodeAtom, WebConfig),
    start_temp_gc(NodeAtom, Node),
    wait(Node).

wait(Node) ->
    receive
        {is_ready, Pid} ->
            Pid ! node_ready,
            wait(Node);
        {'EXIT', _, Reason} ->
            error_logger:info_report({"Node failed", Node, Reason});
        {nodedown, _Node} ->
            error_logger:info_report({"Node", Node, "down"});
        E ->
            error_logger:info_report({"Erroneous message (node_mon)", E})
    end.

slave_env() ->
    Home = disco:get_setting("DISCO_MASTER_HOME"),
    lists:flatten([?SLAVE_ARGS,
                   [io_lib:format(" -pa ~s/ebin/~s", [Home, Dir])
                    || Dir <- ["", "mochiweb", "ddfs"]],
                   [io_lib:format(" -env ~s '~s'", [S, disco:get_setting(S)])
                    || S <- disco:settings()]]).

slave_start(Node) ->
    error_logger:info_report({"starting node", Node}),
    {is_master_node(Node),
        slave:start(list_to_atom(Node), slave_name(), slave_env(), self(),
                disco:get_setting("DISCO_ERLANG"))}.

is_master_node(Node) ->
    case net_adm:names(Node) of
        {ok, Names} ->
            Master = string:sub_word(atom_to_list(node()), 1, $@),
            lists:keymember(Master, 1, Names);
        _ ->
            false
    end.

start_temp_gc(NodeAtom, Node) ->
    DataRoot = disco:get_setting("DISCO_DATA"),
    GCAfter = list_to_integer(disco:get_setting("DISCO_GC_AFTER")),
    spawn_link(NodeAtom, temp_gc, start_link,
        [whereis(disco_server), whereis(event_server), whereis(ddfs_master),
         DataRoot, Node, GCAfter]).

start_ddfs_node(NodeAtom, {GetEnabled, PutEnabled}) ->
    DdfsRoot = disco:get_setting("DDFS_ROOT"),
    DiscoRoot = disco:get_setting("DISCO_DATA"),
    PutMax = list_to_integer(disco:get_setting("DDFS_PUT_MAX")),
    GetMax = list_to_integer(disco:get_setting("DDFS_GET_MAX")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    GetPort = list_to_integer(disco:get_setting("DISCO_PORT")),
    Args = [{nodename, string:sub_word(atom_to_list(NodeAtom), 2, $@)},
            {ddfs_root, DdfsRoot}, {disco_root, DiscoRoot},
            {put_max, PutMax}, {get_max, GetMax},
            {put_port, PutPort}, {get_port, GetPort},
            {get_enabled, GetEnabled},
            {put_enabled, PutEnabled}],
    spawn_link(NodeAtom, ddfs_node, start_link, [Args]).

blacklist(Node) ->
    error_logger:info_report({"Blacklisting", Node,
        "for", ?BLACKLIST_PERIOD, "ms."}),
    Token = now(),
    gen_server:call(disco_server, {blacklist, Node, Token}),
    timer:sleep(?BLACKLIST_PERIOD),
    gen_server:call(disco_server, {whitelist, Node, Token}).






