
-module(ddfs_get).

-include_lib("kernel/include/file.hrl").

-export([start/2]).

start(MochiConfig, Root) ->
    mochiweb_http:start([{name, ddfs_get},
        {loop, fun(Req) -> loop(Req:get(path), Req, Root) end}
            | MochiConfig]).

loop("/proxy/" ++ Path, Req, Root) ->
    {_Node, Rest} = mochiweb_util:path_split(Path),
    {_Method, RealPath} = mochiweb_util:path_split(Rest),
    loop([$/|RealPath], Req, Root);

loop("/" ++ Path, Req, Root) ->
    % Disable keep-alive
    erlang:put(mochiweb_request_force_close, true),

    case {Req:get(method), string:chr(Path, $.)} of
        {'GET', 0} ->
            case catch gen_server:call(ddfs_node, get_blob) of
                ok ->
                    send_file(Req, filename:join(Root, Path));
                _ -> 
                    Req:respond({503, [],
                        ["Maximum number of downloaders reached. ",
                            "Try again later"]})
            end;
        {'GET', _} ->
            Req:respond({403, [], ["Invalid blob name"]});
        _ ->
            Req:respond({501, [], ["Method not supported"]})
    end. 

send_file(Req, Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{type = regular}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, IO} ->
                    Req:ok({"application/octet-stream", [], {file, IO}}); 
                _ ->
                    Req:respond({500, [], ["Access failed"]})
            end;
        {ok, _} ->
            Req:respond({403, [], ["Forbidden"]});
        {error, enoent} ->
            Req:not_found();
        _ ->
            Req:respond({500, [], ["Access failed"]})
    end.

