-module(ddfs_gc_node).
-export([start_gc_node/4]).
-export([gc_node_init/3]).  % WHY IS THIS NEEDED?
-include_lib("kernel/include/file.hrl").

-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_gc.hrl").

% The module contains the node-local portion of the DDFS GC/RR
% algorithm.

-spec start_gc_node(node(), pid(), erlang:timestamp(), phase()) -> pid().
start_gc_node(Node, Master, Now, Phase) ->
    spawn_link(Node, ?MODULE, gc_node_init, [Master, Now, Phase]).

-spec gc_node_init(pid(), erlang:timestamp(), phase()) -> 'ok'.
gc_node_init(Master, Now, Phase) ->
    % All phases of GC/RR require that we build a snapshot of our
    % node-local DDFS content across all volumes.
    process_flag(priority, low),
    {Vols, Root} = ddfs_node:get_vols(),
    {_, VolNames} = lists:unzip(Vols),
    _ = ets:new(tag, [named_table, set, private]),
    _ = ets:new(blob, [named_table, set, private]),
    traverse(Now, Root, VolNames, blob),
    traverse(Now, Root, VolNames, tag),
    error_logger:info_report({"GC: # blobs", ets:info(blob, size)}),
    error_logger:info_report({"GC: # tags", ets:info(tag, size)}),

    % Now, dispatch to the phase that is running on the master.
    gc_node(Master, Now, Root, Phase).

-spec gc_node(pid(), erlang:timestamp(), path(), phase()) -> 'ok'.
gc_node(Master, Now, Root, Phase)
  when Phase =:= start; Phase =:= build_map; Phase =:= map_wait ->
    check_server(Master, Root),
    local_gc(Master, Now, Root),
    replica_server(Master, Root);

gc_node(Master, Now, Root, gc) ->
    local_gc(Master, Now, Root),
    replica_server(Master, Root);

gc_node(Master, _Now, Root, Phase)
  when Phase =:= rr_blobs; Phase =:= rr_blobs_wait; Phase =:= rr_tags ->
    replica_server(Master, Root).

%%
%% Node-local object table construction. (build_map / map_wait / gc)
%%

-spec traverse(erlang:timestamp(), path(), [volume_name()], object_type()) -> 'ok'.
traverse(Now, Root, VolNames, Type) ->
    Mode = case Type of tag -> "tag"; blob -> "blob" end,
    lists:foreach(
      fun(VolName) ->
              DDFSDir = filename:join([Root, VolName, Mode]),
              Handler = fun(Obj, Dir, _Ok) ->
                                handle_file(Obj, Dir, VolName, Type, Now)
                        end,
              ddfs_util:fold_files(DDFSDir, Handler, ok)
      end, VolNames).

-spec handle_file(path(), path(), volume_name(), object_type(), erlang:timestamp())
                 -> 'ok'.
handle_file("!trash" ++ _, _, _, _, _) ->
    ok;
% GC1) Remove leftover !partial. files
handle_file("!partial" ++ _ = File, Dir, _, _, Now) ->
    [_, Obj] = string:tokens(File, "."),
    {_, Time} = ddfs_util:unpack_objname(Obj),
    Diff = timer:now_diff(Now, Time) / 1000,
    Paranoid = disco:has_setting("DDFS_PARANOID_DELETE"),
    delete_if_expired(filename:join(Dir, File), Diff, ?PARTIAL_EXPIRES, Paranoid);
handle_file(Obj, _, VolName, Type, _) ->
    % We could check a checksum of Obj here
    ets:insert(Type, {list_to_binary(Obj), VolName, false}).

%%
%% Serve check requests from master (build_map / map_wait)
%%

check_server(Master, Root) ->
    receive
        {check_blob, ObjName} ->
            LocalObj = {ObjName, node()},
            Master ! {check_blob_result, LocalObj, check_blob(ObjName)},
            check_server(Master, Root);
        start_gc ->
            ok;
        E ->
            ddfs_gc:abort({"GC: Erroneous message received", E},
                          node_request_failed)
    end.

-spec check_blob(object_name()) -> boolean().
check_blob(ObjName) ->
    ets:update_element(blob, ObjName, {3, true}).

% Perform GC
%
% GC2) Remove orphaned tags (old versions and deleted tags)
% GC3) Remove orphaned blobs (blobs not referred by any tag)

local_gc(Master, Now, Root) ->
    delete_orphaned(Master, Now, Root, blob, ?ORPHANED_BLOB_EXPIRES),
    delete_orphaned(Master, Now, Root, tag, ?ORPHANED_TAG_EXPIRES),
    ddfs_gc_main:node_gc_done(Master),
    ok.

-spec delete_orphaned(pid(), erlang:timestamp(), path(), object_type(),
                      non_neg_integer()) -> 'ok'.
delete_orphaned(Master, Now, Root, Type, Expires) ->
    Paranoid = disco:has_setting("DDFS_PARANOID_DELETE"),
    lists:foreach(
      fun([Obj, VolName]) ->
              {_, Time} = ddfs_util:unpack_objname(Obj),
              {ok, Path, _} =
                  ddfs_util:hashdir(Obj, "nonode!", atom_to_list(Type), Root, VolName),
              FullPath = filename:join(Path, binary_to_list(Obj)),
              Diff = timer:now_diff(Now, Time) / 1000,
              case catch ddfs_gc_main:is_orphan(Master, Type, Obj, VolName) of
                  {ok, true} ->
                      delete_if_expired(FullPath, Diff, Expires, Paranoid);
                  _E ->
                      % Do not delete if not orphan, timeout or error.
                      ok
              end
      end, ets:match(Type, {'$1', '$2', false})).

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

%%
%% Re-replication  (rr_blobs / rr_blobs_wait)
%%

-spec replica_server(pid(), path()) -> no_return().
replica_server(Master, Root) ->
    receive
        {put_blob, Replicator, Ref, Blob, PutUrl} ->
            [{_, VolName, _}] = ets:lookup(blob, Blob),
            {ok, Path, _} =
                ddfs_util:hashdir(Blob, "nonode!", "blob", Root, VolName),
            SrcPath = filename:join(Path, binary_to_list(Blob)),
            Replicator ! {Ref, do_put(Blob, SrcPath, PutUrl)},
            replica_server(Master, Root)
    end.

-spec do_put(object_name(), path(), nonempty_string()) ->
                    {'ok', object_name(), [binary(),...]} | {'error', term()}.
do_put(Blob, SrcPath, PutUrl) ->
    case ddfs_http:http_put(SrcPath, PutUrl, ?GC_PUT_TIMEOUT) of
        {ok, Body} ->
            case catch mochijson2:decode(Body) of
                {'EXIT', _, _} ->
                    {error, {server_error, Body}};
                Resp when is_binary(Resp) ->
                    case ddfs_util:parse_url(Resp) of
                        not_ddfs ->
                            {error, {server_error, Body}};
                        _ ->
                            {ok, Blob, [Resp]}
                    end;
                _Err ->
                    {error, {server_error, Body}}
            end;
        {error, E} ->
            {error, E}
    end.
