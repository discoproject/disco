-module(disco_proxy).
-behaviour(gen_server).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").


-export([start/0, update_nodes/2]).
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

-spec start() -> ignore | {ok, pid()}.
start() ->
    Proxy = disco:get_setting("DISCO_PROXY_ENABLED"),
    LocalCluster = disco:local_cluster(),
    if Proxy =:= "", LocalCluster =:= false ->
            lager:info("Disco proxy disabled"),
            ignore;
       true ->
            lager:info("Disco proxy enabled"),
            case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
            end
    end.

-spec update_nodes([host()], port_map()) -> ok.
update_nodes(Nodes, PortMap) ->
    Proxy = disco:get_setting("DISCO_PROXY_ENABLED"),
    LocalCluster = disco:local_cluster(),
    if Proxy =:= "", LocalCluster =:= false -> ok;
       true -> gen_server:cast(?MODULE, {update_nodes, Nodes, PortMap})
    end.

-type state() :: pid().

-spec init(_) -> gs_init().
init(_Args) ->
    process_flag(trap_exit, true),
    ProxyMonitor = spawn_link(fun() -> proxy_monitor(start_proxy()) end),
    {ok, ProxyMonitor}.

-spec handle_call(term(), from(), state()) -> gs_noreply().
handle_call(_Req, _From, S) ->
    {noreply, S}.

-spec handle_cast({update_nodes, [host()], port_map()}, state())
                 -> gs_noreply().
handle_cast({update_nodes, Nodes, PortMap}, ProxyMonitor) ->
    do_update_nodes(Nodes, PortMap),
    exit(ProxyMonitor, config_change),
    Monitor = spawn_link(fun() -> proxy_monitor(start_proxy()) end),
    {noreply, Monitor}.

-spec handle_info({'EXIT', pid(), term()}, state()) -> gs_noreply().
handle_info({'EXIT', ProxyMonitor, Reason}, ProxyMonitor) ->
    lager:warning("Proxy monitor exited: ~p", [Reason]),
    Monitor = spawn_link(fun() -> proxy_monitor(start_proxy()) end),
    {noreply, Monitor};
handle_info({'EXIT', _Other, config_change}, S) ->
    {noreply, S};
handle_info({'EXIT', Other, Reason}, S) ->
    lager:warning("Proxy: received unknown exit from ~p: ~p", [Other, Reason]),
    {noreply, S}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, _State) ->
    kill_proxy(),
    lager:warning("Disco proxy dies: ~p", [Reason]).

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec do_update_nodes([host()], port_map()) -> ok.
do_update_nodes(Nodes, PortMap) ->
    DiscoPort = disco:get_setting("DISCO_PORT"),
    Port = disco:get_setting("DISCO_PROXY_PORT"),
    PidFile = disco:get_setting("DISCO_PROXY_PID"),
    Config = disco:get_setting("DISCO_PROXY_CONFIG"),
    Httpd = disco:get_setting("DISCO_HTTPD"),
    Type = filename:basename(string:sub_word(Httpd, 1, $\ )),
    Body = make_config(Type, Nodes, Port, DiscoPort, PidFile, PortMap),
    ok = file:write_file(Config, Body).

proxy_monitor(Pid) ->
    try
        case string:str(os:cmd(["ps -p", Pid]), Pid) of
            0 ->
                lager:warning("Proxy at pid ~p died", [Pid]),
                sleep(?PROXY_RESTART_DELAY),
                exit(proxy_died);
            _ ->
                sleep(?PROXY_CHECK_INTERVAL),
                proxy_monitor(Pid)
        end
    catch
        K:E ->
            lager:warning("ps failed: ~p:~p", [K, E]),
            exit(ps_failed)
    end.

start_proxy() ->
    kill_proxy(),
    Out = os:cmd(disco:get_setting("DISCO_HTTPD")),
    sleep(5000),
    case get_pid() of
        {ok, Pid} when Pid =/= [] ->
            lager:info("Starting proxy at pid ~p", [Pid]),
            Pid;
        _ ->
            lager:warning(
              "Could not start proxy ~p (pid file not found or empty)",
              [Out]),
            exit(proxy_init_failed)
    end.

kill_proxy() ->
    case get_pid() of
        {ok, Pid} ->
            lager:info("Killing pid ~p", [Pid]),
            _ = os:cmd(["kill -9 ", Pid]),
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
            lager:info("proxy dying: ~p", [Reason]),
            kill_proxy(),
            exit(Reason)
    after Timeout ->
        ok
    end.

resolve_port(_Host, "GET", none) ->
    disco:get_setting("DISCO_PORT");
resolve_port(_Host, "PUT", none) ->
    disco:get_setting("DDFS_PUT_PORT");
resolve_port(Host, "GET", {_, PortMap}) ->
    {GetPort, _PutPort} = gb_trees:get(Host, PortMap),
    integer_to_list(GetPort);
resolve_port(Host, "PUT", {_, PortMap}) ->
    {_GetPort, PutPort} = gb_trees:get(Host, PortMap),
    integer_to_list(PutPort).

resolve_node(Node, Method, none) ->
    case inet:getaddr(Node, inet) of
        {ok, Ip} ->
            {Ip, resolve_port(Node, Method, none)};
        _ ->
            lager:warning("Proxy could not resolve node ~p", [Node]),
            false
    end;
resolve_node(Node, Method, PortMap) ->
    {{127,0,0,1}, resolve_port(Node, Method, PortMap)}.


make_config("lighttpd", Nodes, Port, DiscoPort, PidFile, PortMap) ->
    Line =
        fun(Node, Method) ->
            case resolve_node(Node, Method, PortMap) of
                {Ip, NPort} ->
                    io_lib:format(?LIGHTTPD_HOST_TEMPLATE,
                          [Node, Method] ++ tuple_to_list(Ip) ++ [NPort]);
                false ->
                    []
            end
        end,
    Body = lists:flatten([[Line(N, "GET"), Line(N, "PUT")] || N <- Nodes]),
    io_lib:format(?LIGHTTPD_CONFIG_TEMPLATE, [Port, PidFile, Body, DiscoPort]);

make_config("varnishd", Nodes, _Port, DiscoPort, _PidFile, PortMap) ->
    Line =
        fun(Node, Method) ->
            case resolve_node(Node, Method, PortMap) of
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

make_config(Type, _Nodes, _Port, _DiscoPort, _PidFile, _PortMap) ->
    lager:warning("Unsupported proxy type ~p", [Type]),
    lager:warning("DISCO_HTTPD should be either lighttpd or varnishd!"),
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
