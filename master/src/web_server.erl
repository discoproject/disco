
-module(web_server).

-export([start/1]).
-define(HANDLERS, ["ddfs", "disco"]).

-include("config.hrl").

start(Port) ->
    Conf = [{loop, fun loop/1},
            {name, web_server},
            {max, ?MAX_HTTP_CONNECTIONS},
            {port, Port}],
    lager:info("web_server starts"),
    case catch mochiweb_http:start(Conf) of
        {ok, Pid} ->
            lager:info("mochiweb starts"),
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
    {Root, _Rest} = mochiweb_util:path_split(Path),
    case lists:member(Root, ?HANDLERS) of
        true ->
            dispatch(Req, list_to_atom(Root ++ "_web"), P);
        false when Path =:= "" ->
            Req:serve_file("index.html", docroot());
        _ ->
            Req:serve_file(Path, docroot())
    end;
loop(_, Req) ->
    Req:not_found().

docroot() ->
    disco:get_setting("DISCO_WWW_ROOT").

dispatch(Req, Module, Path) ->
    erlang:put(mochiweb_request_force_close, true),
    case catch Module:op(Req:get(method), Path, Req) of
        {'EXIT', E} ->
            lager:error("Request ~p failed: ~p", [Req:get(path), E]),
            Req:respond({500, [], ["Internal server error"]});
        invalid_utf8 ->
            lager:error("Request ~p failed: ~p", [Req:get(path), invalid_utf8]),
            Req:respond({400, [], ["Invalid UTF8"]});
        _ ->
            ok
    end.
