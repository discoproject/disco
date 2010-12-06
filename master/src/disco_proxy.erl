-module(disco_proxy).
-behaviour(gen_server).

-export([start/0, stop/0, update_nodes/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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
        case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
            {ok, Server} -> {ok, Server};
            {error, {already_started, Server}} -> {ok, Server}
        end
    end.

stop() ->
    case whereis(?MODULE) of
        undefined -> ok;
        _Pid -> gen_server:call(?MODULE, stop)
    end.

-spec update_nodes([nonempty_string()]) -> 'ok'.
update_nodes(Nodes) ->
    case disco:get_setting("DISCO_PROXY_ENABLED") of
        "" -> ok;
        _ -> gen_server:cast(?MODULE, {update_nodes, Nodes})
    end.

init(_Args) ->
    process_flag(trap_exit, true),
    ProxyMonitor = spawn_link(fun() -> proxy_monitor(start_proxy()) end),
    {ok, ProxyMonitor}.

handle_call(_Req, _From, S) ->
    {noreply, S}.

handle_cast({update_nodes, Nodes}, ProxyMonitor) ->
    do_update_nodes(Nodes),
    exit(ProxyMonitor, config_change),
    Monitor = spawn_link(fun() -> proxy_monitor(start_proxy()) end),
    {noreply, Monitor}.

handle_info({'EXIT', ProxyMonitor, Reason}, ProxyMonitor) ->
    error_logger:warning_report({"Proxy monitor exited", Reason}),
    Monitor = spawn_link(fun() -> proxy_monitor(start_proxy()) end),
    {noreply, Monitor};
handle_info({'EXIT', _Other, config_change}, S) ->
    {noreply, S};
handle_info({'EXIT', Other, Reason}, S) ->
    error_logger:warning_report({"Proxy: received unknown exit:", Other, Reason}),
    {noreply, S}.

terminate(Reason, _State) ->
    error_logger:warning_report({"Disco config dies", Reason}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec do_update_nodes([nonempty_string()]) -> 'ok'.
do_update_nodes(Nodes) ->
    DiscoPort = disco:get_setting("DISCO_PORT"),
    Port = disco:get_setting("DISCO_PROXY_PORT"),
    PidFile = disco:get_setting("DISCO_PROXY_PID"),
    Config = disco:get_setting("DISCO_PROXY_CONFIG"),
    Httpd = disco:get_setting("DISCO_HTTPD"),
    Type = filename:basename(string:sub_word(Httpd, 1, $\ )),
    Body = make_config(Type, Nodes, Port, DiscoPort, PidFile),
    ok = file:write_file(Config, Body).

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
            false
    end.

make_config("lighttpd", Nodes, Port, DiscoPort, PidFile) ->
    Line =
        fun(Node, Method) ->
            case resolve_node(Node, Method) of
                {Ip, NPort} ->
                    io_lib:format(?LIGHTTPD_HOST_TEMPLATE,
                          [Node, Method] ++ tuple_to_list(Ip) ++ [NPort]);
                false ->
                    []
            end
        end,
    Body = lists:flatten([[Line(N, "GET"), Line(N, "PUT")] || N <- Nodes]),
    io_lib:format(?LIGHTTPD_CONFIG_TEMPLATE, [Port, PidFile, Body, DiscoPort]);

make_config("varnishd", Nodes, _Port, DiscoPort, _PidFile) ->
    Line =
        fun(Node, Method) ->
            case resolve_node(Node, Method) of
                {Ip, NPort} ->
                    Name = varnish_name(Node, Method),
                    BELine = io_lib:format(?VARNISH_BACKEND_TEMPLATE,
                                [Name] ++ tuple_to_list(Ip) ++ [NPort]),
                    CLine = io_lib:format(?VARNISH_COND_TEMPLATE,
                                [Node, Method, Name]),
                    {BELine, CLine};
                false ->
                    {[], []}
            end
        end,
    {BEGet, CondGet} = lists:unzip([Line(N, "GET") || N <- Nodes]),
    {BEPut, CondPut} = lists:unzip([Line(N, "PUT") || N <- Nodes]),
    io_lib:format(?VARNISH_CONFIG_TEMPLATE, [
        DiscoPort,
        lists:flatten([BEGet|BEPut]),
        lists:flatten([CondGet|CondPut])]);

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
