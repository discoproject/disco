-module(web_server).

-export([start/1]).
-define(HANDLERS, ["ddfs", "disco"]).

-include("config.hrl").

-spec start(string()) -> {ok, pid()} | {error, term()}.
start(Port) ->
    folsom_metrics:new_histogram(?MODULE, exdec, ?FOLSOM_HISTOGRAM_SIZE),
    Conf = [{loop, fun loop/1},
            {name, web_server},
            {max, ?MAX_HTTP_CONNECTIONS},
            {port, Port}],
    case mochiweb_http:start(Conf) of
        {ok, Pid} ->
            lager:info("web server (mochiweb) starts"),
            {ok, Pid};
        {_Error, E} ->
            lager:error("Starting web server failed: ~p", [E]),
            {error, E}
    end.

loop(Req) ->
    loop(Req:get(path), Req).

loop("/proxy/" ++ Path, Req) ->
    % Unwrap '/proxy/<nodename>/<meth>/path' to '/path'.
    {_Node, Rest} = mochiweb_util:path_split(Path),
    {_Meth, RealPath} = mochiweb_util:path_split(Rest),
    loop("/" ++ RealPath, Req);
loop("/" ++ Path = P, Req) ->
    StartTime = now(),
    {Root, _Rest} = mochiweb_util:path_split(Path),
    case lists:member(Root, ?HANDLERS) of
        true ->
            dispatch(Req, list_to_atom(Root ++ "_web"), P);
        false when Path =:= "" ->
            Req:serve_file("index.html", docroot());
        _ ->
            Req:serve_file(Path, docroot())
    end,
    folsom_metrics:notify(?MODULE, timer:now_diff(now(), StartTime));
loop(_, Req) ->
    Req:not_found().

docroot() ->
    disco:get_setting("DISCO_WWW_ROOT").

dispatch(Req, Module, Path) ->
    erlang:put(mochiweb_request_force_close, true),
    try Module:op(Req:get(method), Path, Req)
    catch
        E ->
            lager:error("Request ~p failed: ~p: ~p",
                        [Req:get(path), E, erlang:get_stacktrace()]),
            Req:respond({400, [], [disco:format("~p", [E])]});
        K:V ->
            lager:error("Request ~p failed (~p:~p): ~p",
                        [Req:get(path), K, V, erlang:get_stacktrace()]),
            Req:respond({500, [], ["Internal server error"]})
    end.
