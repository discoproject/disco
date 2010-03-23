-module(node_mon).
-export([spawn_node/1, slave_node/1, slave_node_safe/1]).

-define(SLAVE_ARGS, "+K true").
-define(RESTART_DELAY, 10000).
-define(BLACKLIST_PERIOD, 600000).

spawn_node(Node) ->
    process_flag(trap_exit, true),
    spawn_link(fun() ->
        case slave_start(Node) of
            {true, {ok, _Node}} ->
                start_ddfs_node(node(), false),
                receive
                    ok -> ok                    
                end;
            {false, {ok, _SlaveNode}} ->
                node_monitor(Node);
            {_, {error, {already_running, _SlaveNode}}} ->
                node_monitor(Node);
            {_, {error, timeout}} ->
                blacklist(Node);
            Error ->
                error_logger:warning_report(
                    {"Spawning node", Node, "failed for unknown reason", Error}),
                blacklist(Node)
        end
    end),
    receive 
        {'EXIT', _Pid, _Reason} -> 
            spawn_node(Node) 
    end.

slave_node_safe(Node) ->
    case catch list_to_existing_atom(slave_name() ++ "@" ++ Node) of
        {'EXIT', _} -> false;
        X -> X
    end.

slave_node(Node) ->
    list_to_atom(slave_name() ++ "@" ++ Node).

slave_name() ->
    disco:get_setting("DISCO_NAME") ++ "_slave".

slave_env() ->
    lists:flatten([?SLAVE_ARGS,
           [io_lib:format(" -pa ~s/ebin/~s", [disco:get_setting("DISCO_MASTER_HOME"), Dir])
            || Dir <- ["", "mochiweb", "ddfs"]],
           [case disco:get_setting(Setting) of
                ""  -> "";
                Val -> io_lib:format(" -env ~s '~s'", [Setting, Val])
            end
            || Setting <- ["DISCO_FLAGS",
                   "DISCO_PORT",
                   "DISCO_PROXY",
                   "DISCO_ROOT",
                   "DDFS_ROOT",
                   "DISCO_DATA",
                   "DISCO_WORKER",
                   "PYTHONPATH"]]]).

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

node_monitor(Node) ->
    process_flag(trap_exit, true),
    NodeAtom = slave_node(Node),
    start_ddfs_node(NodeAtom, true),
    monitor_node(NodeAtom, true),
    receive
        {'EXIT', _, Reason} ->
            error_logger:info_report({"DDFS failed on node", Node, Reason});
        {nodedown, _Node} ->
            error_logger:info_report({"Node", Node, "down"});
        E ->
            error_logger:info_report({"Erroneous message (node_mon)", E})
    end,
    timer:sleep(?RESTART_DELAY).

start_ddfs_node(NodeAtom, GetEnabled) ->
    Enabled = disco:get_setting("DDFS_ENABLED"),
    if Enabled =:= "on" ->
        DdfsRoot = disco:get_setting("DDFS_ROOT"),
        DiscoRoot = disco:get_setting("DISCO_DATA"),
        PutMax = list_to_integer(disco:get_setting("DDFS_PUT_MAX")),
        GetMax = list_to_integer(disco:get_setting("DDFS_GET_MAX")),
        PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
        GetPort = list_to_integer(disco:get_setting("DISCO_PORT")),
        Args = [{ddfs_root, DdfsRoot}, {disco_root, DiscoRoot},
                {put_max, PutMax}, {get_max, GetMax},
                {put_port, PutPort}, {get_port, GetPort},
                {get_enabled, GetEnabled}],
        spawn_link(NodeAtom, ddfs_node, start_link, [Args]);
    true -> ok
    end.

blacklist(Node) ->
    error_logger:info_report({"Blacklisting", Node,
        "for", ?BLACKLIST_PERIOD, "ms."}),
    Token = now(),
    gen_server:call(disco_server, {blacklist, Node, Token}),
    timer:sleep(?BLACKLIST_PERIOD),
    gen_server:call(disco_server, {whitelist, Node, Token}).






