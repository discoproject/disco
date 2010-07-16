-module(ddfs_gc_node).
-export([gc_node/2]).

-include("config.hrl").

% see ddfs_gc.erl for comments

-spec gc_node(pid(), timer:timestamp()) -> 'orphans_done'.
gc_node(Master, Now) ->
    process_flag(priority, low),
    ets:new(tag, [named_table, set, private]),
    ets:new(blob, [named_table, set, private]),
    {Vols, Root} = gen_server:call(ddfs_node, get_vols),
    {_, VolNames} = lists:unzip(Vols),
    traverse(Now, Root, VolNames, "blob", blob),
    traverse(Now, Root, VolNames, "tag", tag),
    error_logger:info_report({"GC: # blobs", ets:info(blob, size)}),
    error_logger:info_report({"GC: # tags", ets:info(tag, size)}),
    node_server(Root),
    delete_orphaned(Master, Now, Root, "blob", blob, ?ORPHANED_BLOB_EXPIRES),
    delete_orphaned(Master, Now, Root, "tag", tag, ?ORPHANED_TAG_EXPIRES),
    Master ! orphans_done.

-spec node_server(nonempty_string()) -> 'ok'.
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

-spec take_key(binary(), atom()) -> bool().
take_key(Key, Ets) ->
    ets:update_element(Ets, Key, {3, true}).

-spec send_blob(binary(), nonempty_string(), nonempty_string()) ->
    'ok' | {'error', 'crashed' | 'timeout'}.
send_blob(Obj, DstUrl, Root) ->
    [{_, VolName, _}] = ets:lookup(blob, Obj),
    {ok, Path, _} = ddfs_util:hashdir(Obj, "nonode!", "blob", Root, VolName),
    ddfs_http:http_put(filename:join(Path, binary_to_list(Obj)),
        DstUrl, ?GC_PUT_TIMEOUT).

-spec traverse(timer:timestamp(), nonempty_string(),
    nonempty_string(), nonempty_string(), atom()) -> _.
traverse(Now, Root, VolNames, Mode, Ets) ->
    lists:foldl(fun(VolName, _) ->
                        ddfs_util:fold_files(filename:join([Root, VolName, Mode]),
                                             fun(Obj, Dir, _) ->
                                                     handle_file(Obj, Dir, VolName, Ets, Now)
                                             end,
                                             nil)
                end, nil, VolNames).

%%%
%%% O1) Remove leftover !partial. files
%%%
-spec handle_file(nonempty_string(), nonempty_string(),
    nonempty_string(), atom(), timer:timestamp()) -> _.
handle_file("!partial" ++ _ = File, Dir, _, _, Now) ->
    [_, Obj] = string:tokens(File, "."),
    {_, Time} = ddfs_util:unpack_objname(Obj),
    delete_if_expired(filename:join(Dir, File), Now, Time, ?PARTIAL_EXPIRES);

handle_file(Obj, _, VolName, Ets, _) ->
    % We could check a checksum of Obj here
    ets:insert(Ets, {list_to_binary(Obj), VolName, false}).

%%%
%%% O2) Remove orphaned tags
%%% O3) Remove orphaned blobs
%%%
-spec delete_orphaned(pid(), timer:timestamp(), nonempty_string(),
    nonempty_string(), atom(), non_neg_integer()) -> _.
delete_orphaned(Master, Now, Root, Mode, Ets, Expires) ->
    error_logger:info_report({"GC: # orphaned", Mode, ets:info(Ets, size)}),
    lists:foreach(fun([Obj, VolName]) ->
                          {_, Time} = ddfs_util:unpack_objname(Obj),
                          {ok, Path, _} = ddfs_util:hashdir(Obj, "nonode!", Mode, Root, VolName),
                          is_really_orphan(Master, Obj) andalso
                              delete_if_expired(filename:join(Path, binary_to_list(Obj)),
                                                Now, Time, Expires)
                  end, ets:match(Ets, {'$1', '$2', false})).

-spec is_really_orphan(pid(), binary()) -> bool().
is_really_orphan(Master, Obj) ->
    Master ! {is_orphan, self(), Obj},
    receive
        {Obj, IsOrphan} ->
            IsOrphan;
        E ->
            ddfs_gc:abort({"GC: Erroneous message received", E},
                          orphans_failed)
    end.

-spec delete_if_expired(nonempty_string(), timer:timestamp(),
    timer:timestamp(), non_neg_integer()) -> _.
delete_if_expired(Path, Now, Time, Expires) ->
    Diff = timer:now_diff(Now, Time) / 1000,
    if Diff > Expires ->
        error_logger:info_report({"GC: Deleting expired object", Path}),
        prim_file:delete(Path);
    true ->
        ok
    end.
