
-module(web_server).

-export([start/1]).
-define(HANDLERS, ["ddfs", "disco"]).

-include("config.hrl").

start(Port) ->
    Conf = [{loop, fun loop/1},
            {name, web_server},
            {max, ?MAX_HTTP_CONNECTIONS},
            {port, Port}],
    error_logger:info_report({"web_server starts"}),
    case catch mochiweb_http:start(Conf) of
        {ok, Pid} ->
            error_logger:info_report({"mochiweb starts"}),
            {ok, Pid};
        {_Error, E} ->
            error_logger:error_report({"Starting web server failed:", E}),
            {error, E}
    end.

loop(Req) ->
    loop(Req:get(path), Req).

loop("/" ++ Path, Req) ->
    {Root, _Rest} = mochiweb_util:path_split(Path),
    case lists:member(Root, ?HANDLERS) of
        true ->
            dispatch(Req, list_to_atom(Root ++ "_web"));
        false when Path =:= "" ->
            Req:serve_file("index.html", docroot());
        _ ->
            Req:serve_file(Path, docroot())
    end;
loop(_, Req) ->
    Req:not_found().

docroot() ->
    disco:get_setting("DISCO_WWW_ROOT").

dispatch(Req, Module) ->
    erlang:put(mochiweb_request_force_close, true),
    case catch Module:op(Req:get(method), Req:get(path), Req) of
        {'EXIT', E} ->
            error_logger:error_report({"Request failed", Req:get(path), E}),
            Req:respond({500, [], ["Internal server error"]});
        invalid_utf8 ->
            error_logger:error_report({"Request failed", Req:get(path), invalid_utf8}),
            Req:respond({400, [], ["Invalid UTF8"]});
        _ ->
            ok
    end.
