-module(disco_proxy).
-export([start/0, update_nodes/1]).

-define(LIGHTY_CHECK_INTERVAL, 30000).
-define(LIGHTY_RESTART_DELAY, 10000).

-define(CONFIG_TEMPLATE,
    "server.modules = (\"mod_proxy\")\n"
    "server.document-root = \"/dev/null\"\n"
    "server.port = ~s\n"
    "server.pid-file = \"~s\"\n"
    "proxy.server = (~s \"\" => ((\"host\" => \"127.0.0.1\", \"port\" => 8989)))\n"
).
-define(HOST_TEMPLATE,
    "\"/proxy/~s/~s/\" => ((\"host\" => \"~b.~b.~b.~b\", \"port\" => ~s)),\n"
).

start() ->
    Proxy = disco:get_setting("DISCO_PROXY_ENABLED"),
    if Proxy =:= "" ->
        error_logger:info_report({"Disco proxy disabled"}),
        ignore;
    true ->
        error_logger:info_report({"Disco proxy enabled"}),
        {ok, spawn_link(fun() ->
            process_flag(trap_exit, true),
            register(disco_proxy, self()),
            proxy_monitor(start_lighty())
        end)}
    end.

update_nodes(Nodes) ->
    Port = disco:get_setting("DISCO_PROXY_PORT"),
    PidFile = disco:get_setting("DISCO_PROXY_PID"),
    Config = disco:get_setting("DISCO_PROXY_CONFIG"),
    Body = io_lib:format(?CONFIG_TEMPLATE, [Port, PidFile,
        lists:flatten([[host_line(Node, "GET"), host_line(Node, "PUT")]
            || Node <- Nodes])]),
    ok = file:write_file(Config, Body),
    case whereis(disco_proxy) of
        undefined -> ok;
        Pid -> exit(Pid, restart)
    end.

host_line(Node, Method) ->
    case inet:getaddr(Node, inet) of
        {ok, Ip} ->
            Port = disco:get_setting(
                    if Method =:= "GET" ->
                        "DISCO_PORT";
                    true ->
                        "DDFS_PUT_PORT"
                    end
            ),
            io_lib:format(?HOST_TEMPLATE, 
                [Node, Method] ++ tuple_to_list(Ip) ++ [Port]);
        _ ->
            ["# ERROR: could not resolve ", Node, "\n"]
    end.
            
proxy_monitor(Pid) ->
    case string:str(os:cmd(["ps -p", Pid]), Pid) of
        0 ->
            error_logger:warning_report({"Lighty died. PID", Pid}),
            sleep(?LIGHTY_RESTART_DELAY),
            exit(lighty_died);
        _ ->
            sleep(?LIGHTY_CHECK_INTERVAL),
            proxy_monitor(Pid)
    end.

start_lighty() ->
    error_logger:info_report({"Starting lighty proxy"}),
    Lighty = disco:get_setting("DISCO_HTTPD"),
    Config = disco:get_setting("DISCO_PROXY_CONFIG"),
    kill_lighty(),
    Out = os:cmd([Lighty, " -f", Config]),
    sleep(5000),
    case get_pid() of
        {ok, Pid} when Pid =/= [] ->
            Pid;
        _ ->
            error_logger:warning_report(
                {"Could not start Lighty proxy (PID file not found or empty)", Out}),
             exit(lighty_init_failed)
    end.

kill_lighty() ->
    case get_pid() of
        {ok, Pid} ->
            error_logger:info_report({"Killing PID", Pid}),
            os:cmd(["kill -9 ", Pid]),
            sleep(1000);
        _ -> ok
    end.

get_pid() ->
    case file:read_file(disco:get_setting("DISCO_PROXY_PID")) of
        {ok, Pid} ->
            {ok, string:strip(binary_to_list(Pid), right, $\n)};
        _ -> 
            false
    end.

sleep(Timeout) ->
    receive
        {'EXIT', _, Reason} ->
            error_logger:info_report({"dying", Reason}),
            kill_lighty(),
            exit(Reason)
    after Timeout ->
        ok
    end.
