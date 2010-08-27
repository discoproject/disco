-module(disco_proxy).
-export([start/0, update_nodes/1]).

-define(PROXY_CHECK_INTERVAL, 30000).
-define(PROXY_RESTART_DELAY, 10000).

-define(LIGHTTPD_CONFIG_TEMPLATE,
    "server.modules = (\"mod_proxy\")\n"
    "server.document-root = \"/dev/null\"\n"
    "server.port = ~s\n"
    "server.pid-file = \"~s\"\n"
    "proxy.server = (~s \"\" => ((\"host\" => \"127.0.0.1\", \"port\" => ~s)))\n"
).
-define(LIGHTTPD_HOST_TEMPLATE,
    "\"/proxy/~s/~s/\" => ((\"host\" => \"~b.~b.~b.~b\", \"port\" => ~s)),\n"
).

-define(VARNISH_CONFIG_TEMPLATE,
    "backend default { .host = \"127.0.0.1\"; .port = \"~s\"; }\n"
    "\n~s\n"
    "sub vcl_recv{\n"
    "~s\n"
    "    return(pipe);\n"
    "}"
).

-define(VARNISH_BACKEND_TEMPLATE,
    "backend ~s { .host = \"~b.~b.~b.~b\"; .port = \"~s\"; }\n"
).

-define(VARNISH_COND_TEMPLATE,
    "    if (req.url ~~ \"^/proxy/~s/~s/\") { set req.backend = ~s; }\n"
).

-define(VARNISH_OK_CHARS,
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
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
            proxy_monitor(start_proxy())
        end)}
    end.

-spec update_nodes([nonempty_string()]) -> 'ok'.
update_nodes(Nodes) ->
    DiscoPort = disco:get_setting("DISCO_PORT"),
    Port = disco:get_setting("DISCO_PROXY_PORT"),
    PidFile = disco:get_setting("DISCO_PROXY_PID"),
    Config = disco:get_setting("DISCO_PROXY_CONFIG"),
    Httpd = disco:get_setting("DISCO_HTTPD"),
    Type = filename:basename(string:sub_word(Httpd, 1, $\ )),
    Body = make_config(Type, Nodes, Port, DiscoPort, PidFile),
    ok = file:write_file(Config, Body),
    case whereis(disco_proxy) of
        undefined -> ok;
        Pid -> exit(Pid, restart)
    end.

proxy_monitor(Pid) ->
    case catch string:str(os:cmd(["ps -p", Pid]), Pid) of
        {'EXIT', {emfile, _}} ->
            error_logger:warning_report({"Out of file descriptors! Sleeping.."}),
            sleep(?PROXY_CHECK_INTERVAL),
            proxy_monitor(Pid);
        {'EXIT', Error} ->
            error_logger:warning_report({"ps failed", Error}),
            exit(ps_failed);
        0 ->
            error_logger:warning_report({"Proxy died. PID", Pid}),
            sleep(?PROXY_RESTART_DELAY),
            exit(proxy_died);
        _ ->
            sleep(?PROXY_CHECK_INTERVAL),
            proxy_monitor(Pid)
    end.

start_proxy() ->
    error_logger:info_report({"Starting proxy"}),
    kill_proxy(),
    Out = os:cmd(disco:get_setting("DISCO_HTTPD")),
    sleep(5000),
    case get_pid() of
        {ok, Pid} when Pid =/= [] ->
            error_logger:info_report({"PID", Pid}),
            Pid;
        _ ->
            error_logger:warning_report(
                {"Could not start proxy (PID file not found or empty)", Out}),
             exit(proxy_init_failed)
    end.

kill_proxy() ->
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
            kill_proxy(),
            exit(Reason)
    after Timeout ->
        ok
    end.

resolve_node(Node, Method) ->
    case inet:getaddr(Node, inet) of
        {ok, Ip} ->
            Port =
                disco:get_setting(
                    if Method =:= "GET" ->
                        "DISCO_PORT";
                    true ->
                        "DDFS_PUT_PORT"
                    end
                ),
            {Ip, Port};
        _ ->
            error_logger:warning_report({"Proxy could not resolve", Node}),
            {{999, 999, 999, 999}, "999"}
    end.

make_config("lighttpd", Nodes, Port, DiscoPort, PidFile) ->
    Line =
        fun(Node, Method) ->
            {Ip, NPort} = resolve_node(Node, Method),
            io_lib:format(?LIGHTTPD_HOST_TEMPLATE,
                  [Node, Method] ++ tuple_to_list(Ip) ++ [NPort])
        end,
    Body = lists:flatten([[Line(N, "GET"), Line(N, "PUT")] || N <- Nodes]),
    io_lib:format(?LIGHTTPD_CONFIG_TEMPLATE, [Port, PidFile, Body, DiscoPort]);

make_config("varnishd", Nodes, _Port, DiscoPort, _PidFile) ->
    BELine =
        fun(Node, Method) ->
            {Ip, NPort} = resolve_node(Node, Method),
            io_lib:format(?VARNISH_BACKEND_TEMPLATE,
                  [varnish_name(Node, Method)] ++ tuple_to_list(Ip) ++ [NPort])
        end,
    CLine =
        fun(Node, Method) ->
            Name = varnish_name(Node, Method),
            io_lib:format(?VARNISH_COND_TEMPLATE, [Node, Method, Name])
        end,
    BE = lists:flatten([[BELine(N, "GET"), BELine(N, "PUT")] || N <- Nodes]),
    Cond = lists:flatten([[CLine(N, "GET"), CLine(N, "PUT")] || N <- Nodes]),
    io_lib:format(?VARNISH_CONFIG_TEMPLATE, [DiscoPort, BE, Cond]);

make_config(Type, _Nodes, _Port, _DiscoPort, _PidFile) ->
    error_logger:warning_report({"Unsupported proxy ", Type}),
    error_logger:warning_report({"DISCO_HTTPD should be either lighttpd or varnishd!"}),
    [].

varnish_name(Node, Method) ->
    varnish_name(Node, [], Method).
varnish_name([], R, Method) ->
    lists:reverse(R) ++ "_" ++ Method;
varnish_name([H|T], R, Method) ->
    case lists:member(H, ?VARNISH_OK_CHARS) of
        true ->
            varnish_name(T, [H|R], Method);
        false ->
            varnish_name(T, [$_|R], Method)
    end.
