
-module(ddfs_get).

-include_lib("kernel/include/file.hrl").

-include("config.hrl").

-export([start/2, serve_ddfs_file/2, serve_disco_file/2]).

start(MochiConfig, Roots) ->
    mochiweb_http:start([{name, ddfs_get},
        {loop, fun(Req) -> loop(Req:get(raw_path), Req, Roots) end}
            | MochiConfig]).

serve_ddfs_file(Path, Req) ->
    DdfsRoot = disco:get_setting("DDFS_ROOT"),
    loop(Path, Req, {DdfsRoot, none}).

serve_disco_file(Path, Req) ->
    DiscoRoot = disco:get_setting("DISCO_DATA"),
    loop(Path, Req, {none, DiscoRoot}).

loop("/proxy/" ++ Path, Req, Roots) ->
    {_Node, Rest} = mochiweb_util:path_split(Path),
    {_Method, RealPath} = mochiweb_util:path_split(Rest),
    loop([$/|RealPath], Req, Roots);

loop("/ddfs/" ++ Path, Req, {DdfsRoot, _DiscoRoot}) ->
    send_file(Req, Path, DdfsRoot);

loop("/disco/" ++ Path, Req, {_DdfsRoot, DiscoRoot}) ->
    send_file(Req, Path, DiscoRoot).

send_file(Req, Path, Root) ->
    % Disable keep-alive
    erlang:put(mochiweb_request_force_close, true),
    case {Req:get(method), mochiweb_util:safe_relative_path(Path)} of
        {'GET', undefined} ->
            Req:not_found();
        {'GET', SafePath} ->
            case catch gen_server:call(ddfs_node, get_blob,
                    ?GET_WAIT_TIMEOUT) of
                ok ->
                    send_file(Req, filename:join(Root, SafePath));
                {'EXIT', {noproc, _}} ->
                    Req:respond({403, [],
                        ["Disco node is not running on this host"]});
                _ ->
                    Req:respond({503, [],
                        ["Maximum number of downloaders reached. ",
                            "Try again later"]})
            end;
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

