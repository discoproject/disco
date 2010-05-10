-module(ddfs_gc_node).
-export([gc_node/2]).

-include("config.hrl").

% see ddfs_gc.erl for comments

gc_node(Master, Now) ->
    process_flag(priority, low),
    ets:new(tag, [named_table, set, private]),
    ets:new(blob, [named_table, set, private]),
    {Vols0, Root} = gen_server:call(ddfs_node, get_volumes),
    {_, Vols} = lists:unzip(Vols0),
    traverse(Now, Root, Vols, "blob", blob),
    traverse(Now, Root, Vols, "tag", tag),
    error_logger:info_report({"GC: # blobs", ets:info(blob, size)}),
    error_logger:info_report({"GC: # tags", ets:info(tag, size)}),
    node_server(Root),
    delete_orphaned(Master, Now, Root, "blob", blob, ?ORPHANED_BLOB_EXPIRES),
    delete_orphaned(Master, Now, Root, "tag", tag, ?ORPHANED_TAG_EXPIRES),
    Master ! orphans_done.

node_server(Root) ->
    receive
        {{touch, Ets}, M, Obj} ->
            M ! {Obj, take_key(Obj, Ets)},
            node_server(Root);
        {put_blob, M, Obj, DstUrl} ->
            M ! {Obj, send_blob(Obj, DstUrl, Root)},
            node_server(Root);
        done ->
            ok;
        E ->
            ddfs_gc:abort({"GC: Erroneous message received", E},
                requests_failed)
    end.

take_key(Key, Ets) ->
    ets:update_element(Ets, Key, {3, true}).

send_blob(Obj, DstUrl, Root) ->
    [{_, Vol, _}] = ets:lookup(blob, Obj),
    {ok, Path, _} = ddfs_util:hashdir(Obj, "nonode!", "blob", Root, Vol),
    ddfs_http:http_put(filename:join(Path, binary_to_list(Obj)),
        DstUrl, ?GC_PUT_TIMEOUT).

traverse(Now, Root, Vols, Mode, Ets) ->
    lists:foldl(fun(Vol, _) ->
        ddfs_util:fold_files(filename:join([Root, Vol, Mode]),
            fun(Obj, Dir, _) ->
                handle_file(Obj, Dir, Vol, Ets, Now)
            end,
        nil)
    end, nil, Vols).

%%%
%%% O1) Remove leftover !partial. files
%%%
handle_file("!partial" ++ _ = File, Dir, _, _, Now) ->
    [_, Obj] = string:tokens(File, "."),
    {_, Time} = ddfs_util:unpack_objname(Obj),
    delete_if_expired(filename:join(Dir, File), Now, Time, ?PARTIAL_EXPIRES);

handle_file(Obj, _, Vol, Ets, _) ->
    % We could check a checksum of Obj here
    ets:insert(Ets, {list_to_binary(Obj), Vol, false}).

%%%
%%% O2) Remove orphaned tags
%%% O3) Remove orphaned blobs
%%%
delete_orphaned(Master, Now, Root, Mode, Ets, Expires) ->
    error_logger:info_report(
        {"GC: # orphaned", Mode, ets:info(Ets, size)}),
    lists:foreach(fun([Obj, Vol]) ->
        {_, Time} = ddfs_util:unpack_objname(Obj),
        {ok, Path, _} = ddfs_util:hashdir(Obj, "nonode!", Mode, Root, Vol),
        is_really_orphan(Master, Obj) andalso
            delete_if_expired(filename:join(Path, binary_to_list(Obj)),
                Now, Time, Expires)
    end, ets:match(Ets, {'$1', '$2', false})).

is_really_orphan(Master, Obj) ->
    Master ! {is_orphan, self(), Obj},
    receive
        {Obj, V} -> V;
        E ->
            ddfs_gc:abort({"GC: Erroneous message received", E},
                orphans_failed)
    end.

delete_if_expired(Path, Now, Time, Expires) ->
    Diff = timer:now_diff(Now, Time) / 1000,
    if Diff > Expires ->
        error_logger:info_report({"GC: Deleting expired object", Path}),
        prim_file:delete(Path);
    true ->
        ok
    end.

