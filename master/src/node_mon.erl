-module(node_mon).
-export([spawn_node/1, slave_node/1]).

-define(SLAVE_ARGS, "+K true").
-define(RESTART_DELAY, 10000).
-define(BLACKLIST_PERIOD, 600000).

spawn_node(Node) ->
    case slave_start(Node) of
        {ok, _SlaveNode} ->
            node_monitor(Node);
        {error, {already_running, _SlaveNode}} ->
            node_monitor(Node);
        {error, timeout} ->
            blacklist(Node);
        Error ->
            error_logger:warning_report(
                {"Spawning node", Node, "failed for unknown reason", Error}),
            blacklist(Node)
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
                   "DISCO_WORKER",
                   "PYTHONPATH"]]]).

slave_start(Node) ->
    error_logger:info_report({"starting node", Node}),
    slave:start(list_to_atom(Node), slave_name(), slave_env(), self(),
        disco:get_setting("DISCO_ERLANG")).

node_monitor(Node) ->
    monitor_node(slave_node(Node), true),
    receive
        {nodedown, _Node} ->
            error_logger:info_report({"Node", Node, "down"}),
            timer:sleep(?RESTART_DELAY),
            spawn_node(Node);
        E ->
            error_logger:info_report({"Erroneous message (node_mon)", E}),
            node_monitor(Node)
    end.

blacklist(Node) ->
    error_logger:info_report({"Blacklisting", Node,
        "for", ?BLACKLIST_PERIOD, "ms."}),
    Token = now(),
    gen_server:call(disco_server, {blacklist, Node, Token}),
    timer:sleep(?BLACKLIST_PERIOD),
    gen_server:call(disco_server, {whitelist, Node, Token}),
    spawn_node(Node).






