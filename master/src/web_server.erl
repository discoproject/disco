
-module(web_server).

-export([start/1]).
-define(HANDLERS, ["ddfs", "disco"]).

start(Port) ->
    Handler = fun(Req) -> loop(Req:get(path), Req) end,
    Conf = [{name, discoweb}, {loop, Handler}, {port, Port}],
    error_logger:info_report({"MOCHI starts"}),

    case catch mochiweb_http:start(Conf) of
        {'EXIT', E} ->
            failed(E);
        {error, E} ->
            failed(E);
        {ok, Pid} ->
            {ok, Pid}
    end.

failed(E) ->
    error_logger:warning_report({"Starting web server failed:", E}),
    {error, E}.

loop("/" ++ Path, Req) ->
    {ModStr, _} = mochiweb_util:path_split(Path),
    case lists:member(ModStr, ?HANDLERS) of
        true ->
            dispatch(Req, list_to_atom(ModStr ++ "_web"));
        false when Path =:= "" ->
            Req:serve_file("index.html", docroot());
        _ ->
            Req:serve_file(Path, docroot())
    end;

loop(_, Req) ->
    Req:not_found().

docroot() ->
    disco:get_setting("DISCO_MASTER_HOME") ++ "/www/".

dispatch(Req, Mod) ->
    case catch Mod:op(Req:get(method), Req:get(path), Req) of
        {'EXIT', E} ->
            error_logger:warning_report({"Request failed", Req:get(path), E}),
            Req:respond({500, [], ["Internal server error"]});
        _ ->
            ok
    end.
            
    



