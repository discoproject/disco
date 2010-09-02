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
    error_logger:info_report({"GC starts"}),
    % ensures that only a single gc_objects process is running at a time
    register(gc_objects_lock, self()),
    process_flag(priority, low),

    TagMinK = list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS")),
    put(tagk, list_to_integer(disco:get_setting("DDFS_TAG_REPLICAS"))),
    put(blobk, list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS"))),

    ets:new(gc_nodes, [named_table, set, private]),
    ets:new(obj_cache, [named_table, set, private]),

    {Tags, NumOk, NumFailed} = process_tags(),

    if NumOk > 0, NumFailed < TagMinK ->
        error_logger:info_report({"GC: Only", NumFailed,
            "failed nodes. Deleting is allowed."}),
        [Pid ! done || {_, Pid} <- ets:tab2list(gc_nodes)],
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
    {OkNodes, Failed, Tags} = gen_server:call(ddfs_master, {get_tags, all}),
    start_gc_nodes(OkNodes),
    process_tags(Tags),
    {Tags, length(OkNodes), length(Failed)}.

-spec process_tags([binary()]) -> 'ok'.
process_tags([]) -> ok;
process_tags([Tag|T]) ->
    error_logger:info_report({"process tag", Tag}),
    case gen_server:call(ddfs_master, {tag, gc_get, Tag}) of
        {deleted, false} ->
            process_tags(T);
        {E, false} ->
            abort({"GC: Couldn't get data for tag", Tag, E}, tag_failed);
        {TagData, TagReplicas} ->
            case catch process_tag(Tag, TagData, TagReplicas) of
                ok ->
                    process_tags(T);
                {'EXIT', E} -> 
                    abort({"GC: Parsing tag", Tag, "failed", E}, json_failed)
            end
    end.

-spec process_tag(binary(), binary(), [node()]) -> 'ok'.
process_tag(Tag, TagData, TagReplicas) ->
    TagK = get(tagk),
    {struct, Json} = mochijson2:decode(TagData),
    {value, {_, Urls}} = lists:keysearch(<<"urls">>, 1, Json),
    {value, {_, TagObj}} = lists:keysearch(<<"id">>, 1, Json),
    
    OkReplicas = [N || {ok, N} <-
        [check_status(tag, {TagObj, Node}) || Node <- TagReplicas]],

    case check_blobsets(Urls, {ok, []}) of
        {fixed, NewUrls} ->
            % Run call in a separate process so post-timeout replies
            % won't pollute the gc process' inbox.
            spawn(fun() -> gen_server:call(ddfs_master,
                    {tag, {put, NewUrls}, Tag}) end);
        %%%
        %%% O5) Re-replicate tags that don't have enough replicas
        %%%
        {ok, _} when length(OkReplicas) < TagK ->
            error_logger:info_report({"GC: Re-replicating tag", Tag}),
            spawn(fun() -> gen_server:call(ddfs_master,
                {tag, {put, Urls}, Tag}) end);
        _ -> 
            ok
    end, 
    ok.

check_blobsets([], Res) -> Res;
check_blobsets([[]|T], Res) -> check_blobsets(T, Res);
check_blobsets([Repl|T], {IsFixed, NRepl}) ->
    BlobK = get(blobk),
    Status = [check_status(blob, ddfs_url(U)) || U <- Repl],
    Ignore = [ok || ignore <- Status],
    case {Ignore, [Node || {ok, Node} <- Status]} of
        {[], []} ->
            error_logger:warning_report(
                {"GC: !!! All replicas missing !!!!", Repl}),
            check_blobsets(T, {Status, [Repl|NRepl]});
        {[], Ok} when length(Ok) < BlobK ->
            [OrigUrl|_] = Repl,
            {Blob, _} = ddfs_url(OrigUrl),
            error_logger:info_report({"GC: Blob needs more replicas", Blob}),
            case rereplicate(Blob, Ok) of
                {ok, NewUrl0} ->
                    NewUrl = replace_scheme(OrigUrl, NewUrl0),
                    error_logger:info_report({"GC: New replica at", NewUrl}),
                    check_blobsets(T,
                        {fixed, [[NewUrl|Repl -- [NewUrl]]|NRepl]});
                _ ->
                    check_blobsets(T, {IsFixed, [Repl|NRepl]})
            end;
        _ ->
            check_blobsets(T, {IsFixed, [Repl|NRepl]})
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
    BlobName = list_to_binary(filename:basename(Path)),
    case disco:node_safe(Host) of
        false ->
            error_logger:warning_report({"GC: Unknown host", Host}),
            {BlobName, unknown};
        Node ->
            {BlobName, Node}
    end.

check_status(_, ignore) -> ignore;
check_status(Type, {_, Node} = Key) ->
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

%%%
%%% O4) Re-replicate blobs that don't have enough replicas
%%%
rereplicate(Blob, OkNodes) ->
    case ets:lookup(obj_cache, {Blob, fixed}) of
        [{_, Url}] -> {ok, Url};
        [] ->
            case gen_server:call(ddfs_master, {new_blob, Blob, 1, OkNodes}) of
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
    receive
        {Obj, Reply} ->
            ets:insert(obj_cache, {Key, Reply});
        E ->
            abort({"GC: Erroneous message received", E}, touch_failed)
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
    TagSet = gb_sets:from_ordset([<<"tag://", T/binary>> || T <- Tags]),

    % Let's start with the current list of deleted tags
    {ok, Deleted} = gen_server:call(ddfs_master,
        {tag, get_deleted, <<"+deleted">>}, ?NODEOP_TIMEOUT),

    % Update the time of death for newly deleted tags
    gb_sets:fold(fun(Tag, none) ->
        ets:insert_new(Ages, {Tag, Now}), none
    end, none, Deleted),
    % Remove those tags from the candidate set which still have
    % active copies around.
    DelSet = gb_sets:subtract(Deleted, TagSet),
    error_logger:info_report({"DELE", DelSet}),

    lists:foreach(fun({Tag, Age}) ->
        Diff = timer:now_diff(Now, Age) / 1000,
        error_logger:info_report({"TAG", Tag, "Diff", Diff}),
        case gb_sets:is_member(Tag, DelSet) of
            false ->
                error_logger:info_report({"TAG", Tag, "alive"}),
                % Copies of tag still alive, remove from Ages
                ets:delete(Ages, Tag);
            true when Diff > ?DELETED_TAG_EXPIRES ->
                % Tag ready to be removed from +deleted
                error_logger:info_report({"REMOVE DELETED", Tag}),
                gen_server:call(ddfs_master, {tag, {remove_deleted, Tag},
                    <<"+deleted">>}, ?TAG_UPDATE_TIMEOUT);
            true ->
                error_logger:info_report({"TAG", Tag, "too young"}),
                ok
                % Tag hasn't been dead long enough to
                % be removed from +deleted
        end
    end, ets:tab2list(Ages)).
