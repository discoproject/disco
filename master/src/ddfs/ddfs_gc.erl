%
% DDFS garbace collector performs six operations which are 
% interleaved in this module and ddfs_gc_node for efficiency:
%
% O1) Remove leftover !partial. files
% O2) Remove orphaned tags (old versions and deleted tags)
% O3) Remove orphaned blobs (blobs not referred by any tag)
% O4) Re-replicate blobs that don't have enough replicas
% O5) Re-replicate tags that don't have enough replicas
% O6) Deleted old items from the +deleted metatag 
% 
% To aid readability, functions that are mainly responsible for each
% operation are marked with O1)..O5) below and in ddfs_gc_node.
%
% Operations are executed as follows:
%
% MASTER:
% 1. Traverse through all urls in tags (process_tags)
% 2. For each replication set in a tag (check_blobsets)
%       - Check if blob (url) has been already processed (check_status)
%          -> yes, proceed to next blob
%          -> no, ask from the node hosting the blob whether the file exists
%       - If not enough replicas couldn't be verified, rereplicate the blob
%         (rereplicate) [04]
% 3. Once all replication sets have been processed for a tag, rereplicate
%    the tag itself if
%       - Any of the blobs was rereplicated and the urls were updated
%       - Tag doesn't have enough known replicas [05]
% 4. If the number of failed nodes does not exceed DDFS_TAG_MIN_REPLICAS,
%    operations O1, O2, O3 and O6 are safe. Tell nodes that all tags have
%    been processed and proceed to 5. Otherwise exit.
% 5. Start the orphan server that tells nodes if any copies of a tag/blob
%    are available (orphan_server).
% 6. Once the orphan server is finished, run garbage collection on the
%    +deleted tags list (process_deleted) [O6].
%
% NODE:
% 1. Traverse all volumes, create a cache of all existing blobs and tags.
%    Deleted partial files on its way [O1] (traverse).
% 2. Start node_server that tells the master if the file exists based on
%    the cache (node_server).
%       - For each existence request, mark the object in the file cache:
%         Every object touched this way exists in some tag, i.e. is not
%         orphaned.
% 3. Once master notifies that all tags have been processed, process all
%    objects in the cache that were not touched in the previous step. 
%    These objects might be orphans (delete_orphaned)
% 4. For each orphan candidate, ask from the master if the object exists
%    on any node.
%       - If yes, skip the object.
%       - If no and if the object is old enough, delete. [O2] [O3]
% 
% The reason for two-way acknowledgement of orphans (tag -> node, node ->
% master) is the following: If node deleted its potential orphans right
% away, without checking their status on the master, objects would be 
% deleted every time the node's hostname changes and the first tag->node
% check fails. If all hostnames were changed at once, all files would be
% deleted without a warning.

-module(ddfs_gc).
-export([start_gc/0, abort/2]).

-include("config.hrl").

-spec abort(term(), atom()) -> no_return().
abort(Msg, Code) ->
    error_logger:warning_report(Msg),
    error_logger:warning_report({"Garbage collection aborted"}),
    exit(Code).

-spec start_gc() -> no_return().
start_gc() ->
    start_gc(ets:new(deleted_ages, [set, public])).

-spec start_gc(ets:tab()) -> no_return().
start_gc(DeletedAges) ->
    spawn(fun() -> gc_objects(DeletedAges) end),
    timer:sleep(?GC_INTERVAL),
    start_gc(DeletedAges).

-spec gc_objects(ets:tab()) -> 'ok'.
gc_objects(DeletedAges) ->
    % ensures that only a single gc_objects process is running at a time
    register(gc_objects_lock, self()),
    error_logger:info_report({"GC starts"}),
    process_flag(priority, low),

    TagMinK = list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS")),
    put(tagk, list_to_integer(disco:get_setting("DDFS_TAG_REPLICAS"))),
    put(blobk, list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS"))),

    _ = ets:new(gc_nodes, [named_table, set, private]),
    _ = ets:new(obj_cache, [named_table, set, private]),

    {Tags, NumOk, NumFailed} = process_tags(),

    if NumOk > 0, NumFailed < TagMinK ->
        error_logger:info_report({"GC: Only", NumFailed,
            "failed nodes. Deleting is allowed."}),
        _ = [Pid ! done || {_, Pid} <- ets:tab2list(gc_nodes)],
        orphan_server(ets:info(gc_nodes, size)),
        process_deleted(Tags, DeletedAges);
    true ->
        error_logger:info_report({"GC:", NumFailed,
            "failed nodes. Skipping delete."})
    end,
    error_logger:info_report({"GC: Done!"}).

-spec start_gc_nodes([node()]) -> 'ok'.
start_gc_nodes([]) -> ok;
start_gc_nodes([Node|T]) ->
    error_logger:info_report({"GC: Start gc_nodes"}),
    S = self(),
    case net_adm:ping(Node) of
        pong ->
            ets:insert(gc_nodes,
                {Node, spawn_link(Node, ddfs_gc_node, gc_node, [S, now()])});
        pang ->
            error_logger:warning_report({"GC: Node unreachable", Node})
    end,
    start_gc_nodes(T).

-spec process_tags() -> {[binary()], non_neg_integer(), non_neg_integer()}.
process_tags() ->
    error_logger:info_report({"GC: Process tags"}),
    {OkNodes, Failed, Tags} = ddfs_master:get_tags(all),
    start_gc_nodes(OkNodes),
    _ = [process_tag(Tag) || Tag <- Tags],
    {Tags, length(OkNodes), length(Failed)}.

-spec process_tag(binary()) -> 'ok'.
process_tag(Tag) ->
    error_logger:info_report({"process tag", Tag}),
    case catch ddfs_master:tag_operation(gc_get, Tag, 30000) of
        {{missing, deleted}, false} ->
            error_logger:info_report({"deleted", Tag}),
            ok;
        {TagId, TagUrls, TagReplicas} ->
            case catch process_tag(Tag, TagId, TagUrls, TagReplicas) of
                ok ->
                    ok;
        % Note that we *must* die if handling the tag failed. Otherwise all
        % blobs refered by the tag may be incorrectly marked as orphan and
        % deleted.
                {'EXIT', E} ->
                    abort({"GC: Parsing tag", Tag, "failed", E}, parse_failed)
            end;
        E ->
            abort({"GC: Couldn't get data for tag", Tag, E}, tagdata_failed)
    end.

-spec process_tag(binary(), binary(), [[binary()]], [node()]) -> 'ok'.
process_tag(Tag, TagId, TagUrls, TagReplicas) ->
    TagK = get(tagk),

    OkReplicas = [N || {ok, N} <-
        [check_status(tag, {TagId, Node}) || Node <- TagReplicas]],
    case check_blobsets(TagUrls, {ok, []}) of
        {fixed, NewUrls} ->
            % Run call in a separate process so post-timeout replies
            % won't pollute the gc process' inbox.
            _ = spawn(fun() ->
                          Op = {put, urls, NewUrls, internal},
                          ddfs_master:tag_operation(Op, Tag)
                      end),
            ok;
        %%%
        %%% O5) Re-replicate tags that don't have enough replicas
        %%%
        _ when length(OkReplicas) < TagK ->
            error_logger:info_report({"GC: Re-replicating tag", Tag}),
            _ = spawn(fun() ->
                          Op = {put, urls, TagUrls, internal},
                          ddfs_master:tag_operation(Op, Tag)
                      end),
            ok;
        _ ->
            ok
    end,
    ok.

check_blobsets([], {IsFixed, Res}) -> {IsFixed, lists:reverse(Res)};
check_blobsets([[]|T], Res) -> check_blobsets(T, Res);
check_blobsets([Repl|T], {IsFixed, NRepl}) ->
    Status = [{check_status(blob, ddfs_url(U)), U} || U <- Repl],
    case lists:keymember(ignore, 1, Status) of
        true ->
            check_blobsets(T, {IsFixed, [Repl|NRepl]});
        false ->
            NewRepl = check_replicas(Status),
            if NewRepl =:= Repl ->
                check_blobsets(T, {IsFixed, [Repl|NRepl]});
            true ->
                check_blobsets(T, {fixed, [NewRepl|NRepl]})
            end
    end.

check_replicas(Status) ->
    BlobK = get(blobk),
    {_, Repl} = lists:unzip(Status),
    case [{Node, Url} || {{ok, Node}, Url} <- Status] of
        [] ->
            error_logger:warning_report(
                {"GC: !!! All replicas missing !!!!", Repl}),
            Repl;
        [{_, OkUrl}|_] = Ok when length(Ok) < BlobK ->
            {OkNodes, _} = lists:unzip(Ok),
            {Blob, _} = ddfs_url(OkUrl),
            error_logger:info_report({"GC: Blob needs more replicas", OkUrl, "Ok nodes", OkNodes}),
            case rereplicate(Blob, OkNodes) of
                {ok, NewUrl0} ->
                    NewUrl = replace_scheme(OkUrl, NewUrl0),
                    error_logger:info_report({"GC: New replica at", NewUrl}),
                    [NewUrl|Repl];
                _ ->
                    Repl
            end;
        Ok when length(Status) > BlobK ->
            {_, OkUrls} = lists:unzip(Ok),
            OkUrls;
        _ ->
            Repl
    end.

replace_scheme(RightScheme0, WrongScheme0) ->
    RightScheme = binary_to_list(RightScheme0),
    WrongScheme = binary_to_list(WrongScheme0),
    {Scheme, _, _, _, _} = mochiweb_util:urlsplit(RightScheme),
    {_, Host, Path, Query, Fragment} = mochiweb_util:urlsplit(WrongScheme),
    Url = mochiweb_util:urlunsplit({Scheme, Host, Path, Query, Fragment}),
    list_to_binary(Url).

-spec ddfs_url(binary()) -> 'ignore' | {binary(), 'unknown' | node()}.
ddfs_url(<<"tag://", _/binary>>) -> ignore;
ddfs_url(Url) ->
    {_, Host, Path, _, _} = mochiweb_util:urlsplit(binary_to_list(Url)),
    %Host = map_node(Host0),
    BlobName = list_to_binary(filename:basename(Path)),
    case disco:slave_safe(Host) of
        false ->
            error_logger:warning_report({"GC: Unknown host", Host}),
            {BlobName, unknown};
        Node ->
            {BlobName, Node}
    end.

check_status(_, ignore) -> ignore;
check_status(Type, {_, Node} = Key) ->
 %   Node = map_node(Node0),
    case {ets:lookup(gc_nodes, Node), ets:lookup(obj_cache, Key)} of
        % Url checked and valid
        {_, [{_, true}]} ->
            {ok, Node};
        % Url checked and found missing
        {_, [{_, false}]} ->
            {missing, Node};
        % Url not checked, unknown node
        {[], []} ->
            ets:insert(obj_cache, {Key, false}),
            {missing, Node};
        % Url not checked, check status
        {[{_, Pid}], []} ->
            touch(Pid, Type, Key),
            check_status(Type, Key)
    end.

%map_node(unknown) -> unknown;
%
%map_node(Node) when is_list(Node) ->
%    sub(Node, "disco2-", "testdisco-");
%
%map_node(Node) when is_atom(Node) ->
%    [Name, Host] = string:tokens(atom_to_list(Node), "@"),
%    list_to_atom(Name ++ "@" ++ sub(Host, "disco2-", "testdisco-")).
%
%sub(Str, Old, New) ->
%    RegExp = "\\Q" ++ Old ++ "\\E",
%    re:replace(Str, RegExp, New,[ multiline, {return, list}]).

%%%
%%% O4) Re-replicate blobs that don't have enough replicas
%%%
rereplicate(Blob, OkNodes) ->
    case ets:lookup(obj_cache, {Blob, fixed}) of
        [{_, Url}] -> {ok, Url};
        [] ->
            case ddfs_master:new_blob(Blob, 1, OkNodes) of
                {ok, [PutUrl]} ->
                    SrcNode = ddfs_util:choose_random(OkNodes),
                    [{_, Pid}] = ets:lookup(gc_nodes, SrcNode),
                    put_blob(Blob, Pid, PutUrl);
                _ ->
                    error_logger:warning_report(
                        {"GC: No suitabled nodes found for re-replication",
                            Blob, OkNodes})
            end
    end.

put_blob(Blob, Pid, PutUrl) ->
    Pid ! {put_blob, self(), Blob, PutUrl},
    receive
        {Blob, {ok, Body}} ->
            DstUrl = mochijson2:decode(Body),
            ets:insert(obj_cache, {{Blob, fixed}, DstUrl}),
            {ok, DstUrl};
        E ->
            {error, E}
    after 30000 -> timeout
    end.

touch(Pid, Type, {Obj, _} = Key) ->
    Pid ! {{touch, Type}, self(), Obj},
    touch_wait(Key).

touch_wait({Obj, _} = Key) ->
    receive
        {Obj, Reply} ->
            ets:insert(obj_cache, {Key, Reply});
        _ ->
           touch_wait(Key)
    after 5000 ->
        ets:insert(obj_cache, {Key, false})
    end.

orphan_server(NumNodes) ->
    Objs = ets:new(noname, [set, private]),
    ets:foldl(fun({{Obj, _}, _}, _) ->
                  ets:insert(Objs, {Obj, true})
              end, nil, obj_cache),
    ets:delete(obj_cache),
    orphan_server(Objs, NumNodes),
    ets:delete(Objs).

orphan_server(_, 0) -> ok;
orphan_server(Objs, NumNodes) ->
    receive
        {is_orphan, M, Obj} ->
            M ! {Obj, not ets:member(Objs, Obj)},
            orphan_server(Objs, NumNodes);
        orphans_done ->
            orphan_server(Objs, NumNodes - 1);
        _ ->
            orphan_server(Objs, NumNodes)
    after 60000 ->
        error_logger:warning_report({"GC: Orphan server timeouts:",
                                     NumNodes, "unresponsive"}),
        ok
    end.

%%%
%%% O6) Deleted old items from the +deleted metatag 
%%%

% We don't want to accumulate deleted tags in the +deleted list
% infinitely. The downside of removing a tag from the list too
% early is that there might be a node still hosting a version of
% the tag file, which we just haven't seen yet. If this node reappears
% and the tag has been already removed from +deleted, the tag
% will come back from dead. 

% To prevent this from happening, we wait until
% all known entries of the tag have been garbage collected and
% the ?DELETED_TAG_EXPIRES quarantine period has passed. We assume
% that the quarantine is long enough so that all temporarily 
% unavailable nodes have time to resurrect during that time, i.e.
% no nodes can re-appear after being gone for ?DELETED_TAG_EXPIRES
% milliseconds.
%
% The Ages table persists the time of death for each deleted tag.
%
-spec process_deleted([binary()], ets:tab()) -> _.
process_deleted(Tags, Ages) ->
    error_logger:info_report({"GC: Process deleted"}),
    Now = now(),

    % Let's start with the current list of deleted tags
    {ok, Deleted} = ddfs_master:tag_operation(get_tagnames,
                                              <<"+deleted">>,
                                              ?NODEOP_TIMEOUT),

    % Update the time of death for newly deleted tags
    gb_sets:fold(fun(Tag, none) ->
                     ets:insert_new(Ages, {Tag, Now}), none
                 end, none, Deleted),
    % Remove those tags from the candidate set which still have
    % active copies around.
    DelSet = gb_sets:subtract(Deleted, gb_sets:from_ordset(Tags)),

    lists:foreach(fun({Tag, Age}) ->
        Diff = timer:now_diff(Now, Age) / 1000,
        case gb_sets:is_member(Tag, DelSet) of
            false ->
                % Copies of tag still alive, remove from Ages
                ets:delete(Ages, Tag);
            true when Diff > ?DELETED_TAG_EXPIRES ->
                % Tag ready to be removed from +deleted
                error_logger:info_report({"REMOVE DELETED", Tag}),
                ddfs_master:tag_operation({delete_tagname, Tag},
                                          <<"+deleted">>,
                                          ?TAG_UPDATE_TIMEOUT);
            true ->
                ok
                % Tag hasn't been dead long enough to
                % be removed from +deleted
        end
    end, ets:tab2list(Ages)).
