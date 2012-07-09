
-module(ddfs_get).

-include_lib("kernel/include/file.hrl").

-include("common_types.hrl").
-include("config.hrl").
-include("ddfs.hrl").

-export([start/2, serve_ddfs_file/3, serve_disco_file/3]).

-spec start(non_neg_integer(), {path(), path()}) -> {ok, pid()} | {error, term()}.
start(Port, Roots) ->
    Ret = mochiweb_http:start([{name, ddfs_get},
                               {max, ?HTTP_MAX_CONNS},
                               {loop, fun(Req) ->
                                              loop(Req:get(raw_path), Req, Roots)
                                      end},
                               {port, Port}]),
    case Ret of
        {ok, _Pid} -> error_logger:info_msg("Started ~p at ~p on port ~p",
                                            [?MODULE, node(), Port]);
        E ->          error_logger:error_msg("~p failed at ~p on port ~p: ~p",
                                             [?MODULE, node(), Port, E])
    end,
    Ret.

-spec serve_ddfs_file(path(), path(), module()) -> _.
serve_ddfs_file(DdfsRoot, Path, Req) ->
    loop(Path, Req, {DdfsRoot, none}).

-spec serve_disco_file(path(), path(), module()) -> _.
serve_disco_file(DiscoRoot, Path, Req) ->
    loop(Path, Req, {none, DiscoRoot}).

-spec loop(path(), module(), {path() | none, path() | none}) -> _.
loop("/proxy/" ++ Path, Req, Roots) ->
    {_Node, Rest} = mochiweb_util:path_split(Path),
    {_Method, RealPath} = mochiweb_util:path_split(Rest),
    loop([$/|RealPath], Req, Roots);

loop("/ddfs/" ++ Path, Req, {DdfsRoot, _DiscoRoot}) ->
    send_file(Req, Path, DdfsRoot);

loop("/disco/" ++ Path, Req, {_DdfsRoot, DiscoRoot}) ->
    send_file(Req, Path, DiscoRoot);

loop(_Path, Req, _Roots) ->
    Req:not_found().

allowed_method('GET') ->
    true;
allowed_method('HEAD') ->
    true;
allowed_method(_) ->
    false.

-spec send_file(module(), path(), path()) -> _.
send_file(Req, Path, Root) ->
    % Disable keep-alive
    erlang:put(mochiweb_request_force_close, true),
    case {allowed_method(Req:get(method)),
          mochiweb_util:safe_relative_path(Path)} of
        {true, undefined} ->
            Req:not_found();
        {true, SafePath} ->
            try case ddfs_node:gate_get_blob() of
                    ok ->
                        send_file(Req, filename:join(Root, SafePath));
                    full ->
                        Req:respond({503, [],
                                     ["Maximum number of downloaders reached. ",
                                      "Try again later"]})
                end
            catch K:V ->
                    error_logger:info_msg("~p: error getting ~p: ~p:~p",
                                          [?MODULE, Path, K, V]),
                    Req:respond({403, [], ["Disco node is busy"]})
            end;
        _ ->
            Req:respond({501, [], ["Method not supported"]})
    end.

-spec send_file(module(), path()) -> _.
send_file(Req, Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{type = regular}} ->
            case file:open(Path, [read, raw, binary]) of
                {ok, IO} ->
                    Req:ok({"application/octet-stream", [], {file, IO}}),
                    file:close(IO);
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
