-module(ddfs_gc_node).
-export([gc_node/2]).

-include_lib("kernel/include/file.hrl").

-include("config.hrl").

% see ddfs_gc.erl for comments

-type mode() :: nonempty_string().
-type tabname() :: 'tag' | 'blob'.

-spec gc_node(pid(), disco_util:timestamp()) -> 'orphans_done'.
gc_node(Master, Now) ->
    process_flag(priority, low),
    _ = ets:new(tag, [named_table, set, private]),
    _ = ets:new(blob, [named_table, set, private]),
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

-spec take_key(binary(), atom()) -> boolean().
take_key(Key, Ets) ->
    ets:update_element(Ets, Key, {3, true}).

-spec send_blob(binary(), nonempty_string(), nonempty_string()) ->
    'ok' | {'error', 'crashed' | 'timeout'}.
send_blob(Obj, DstUrl, Root) ->
    [{_, VolName, _}] = ets:lookup(blob, Obj),
    {ok, Path, _} = ddfs_util:hashdir(Obj, "nonode!", "blob", Root, VolName),
    ddfs_http:http_put(filename:join(Path, binary_to_list(Obj)),
        DstUrl, ?GC_PUT_TIMEOUT).

-spec traverse(disco_util:timestamp(), nonempty_string(),
               [nonempty_string()], mode(), tabname()) -> _.
traverse(Now, Root, VolNames, Mode, Ets) ->
    lists:foldl(
        fun(VolName, _) ->
            ddfs_util:fold_files(filename:join([Root, VolName, Mode]),
                                 fun(Obj, Dir, _) ->
                                     handle_file(Obj, Dir, VolName, Ets, Now)
                                 end, nil)
        end, nil, VolNames).

%%%
%%% O1) Remove leftover !partial. files
%%%
-spec handle_file(nonempty_string(), nonempty_string(),
    nonempty_string(), tabname(), disco_util:timestamp()) -> _.
handle_file("!trash" ++ _, _, _, _, _) ->
    ok;
handle_file("!partial" ++ _ = File, Dir, _, _, Now) ->
    [_, Obj] = string:tokens(File, "."),
    {_, Time} = ddfs_util:unpack_objname(Obj),
    Diff = timer:now_diff(Now, Time) / 1000,
    Paranoid = disco:has_setting("DDFS_PARANOID_DELETE"),
    delete_if_expired(filename:join(Dir, File), Diff, ?PARTIAL_EXPIRES, Paranoid);
handle_file(Obj, _, VolName, Ets, _) ->
    % We could check a checksum of Obj here
    ets:insert(Ets, {list_to_binary(Obj), VolName, false}).

%%%
%%% O2) Remove orphaned tags
%%% O3) Remove orphaned blobs
%%%
-spec delete_orphaned(pid(), disco_util:timestamp(), nonempty_string(),
                      mode(), tabname(), non_neg_integer()) -> 'ok'.
delete_orphaned(Master, Now, Root, Mode, Ets, Expires) ->
    Paranoid = disco:has_setting("DDFS_PARANOID_DELETE"),
    lists:foreach(
        fun([Obj, VolName]) ->
            {_, Time} = ddfs_util:unpack_objname(Obj),
            {ok, Path, _} =
                ddfs_util:hashdir(Obj, "nonode!", Mode, Root, VolName),
            FullPath = filename:join(Path, binary_to_list(Obj)),
            Diff = timer:now_diff(Now, Time) / 1000,
            is_really_orphan(Master, Obj) andalso
                delete_if_expired(FullPath, Diff, Expires, Paranoid)
        end, ets:match(Ets, {'$1', '$2', false})).

-spec is_really_orphan(pid(), binary()) -> boolean().
is_really_orphan(Master, Obj) ->
    Master ! {is_orphan, self(), Obj},
    receive
        {Obj, IsOrphan} ->
            IsOrphan;
        E ->
            ddfs_gc:abort({"GC: Erroneous message received", E},
                          orphans_failed)
    after 10000 ->
        ddfs_gc:abort({"GC: Master stopped responding"}, orphan_timeout),
        timeout
    end.

-spec delete_if_expired(file:filename(), float(),
                        non_neg_integer(), boolean()) -> 'ok'.
delete_if_expired(Path, Diff, Expires, true) when Diff > Expires ->
    error_logger:info_report({"GC: Deleting expired object (paranoid)", Path}),
    Trash = "!trash." ++ filename:basename(Path),
    Deleted = filename:join(filename:dirname(Path), Trash),
    % Chmod u+w deleted files, so they can removed safely with rm without -f
    _ = prim_file:write_file_info(Path, #file_info{mode = 8#00600}),
    _ = prim_file:rename(Path, Deleted),
    % Sleep here two prevent master being DDOS'ed by info_reports above
    timer:sleep(100);

delete_if_expired(Path, Diff, Expires, _Paranoid) when Diff > Expires ->
    error_logger:info_report({"GC: Deleting expired object", Path}),
    _ = prim_file:delete(Path),
    timer:sleep(100);

delete_if_expired(_Path, _Diff, _Expires, _Paranoid) ->
    ok.

