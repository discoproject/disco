% Both garbage collection (GC) and re-replication (RR) require working
% from a consistent snapshot of DDFS.  Since computing this snapshot
% is expensive in both time and memory, once computed, the snapshot is
% used for both purposes, and GC and RR are implemented together.  GC
% is performed before RR, so that GC can free up disk space that can
% be used by RR.
%
% GC performs the following operations:
%
% GC1) Remove leftover !partial. files
% GC2) Remove orphaned tags (old versions and deleted tags)
% GC3) Remove orphaned blobs (blobs not referred by any tag)
% GC4) Delete old deleted tags from the +deleted metatag
%
% while RR does the following:
%
% RR1) Re-replicate blobs that don't have enough replicas
% RR2) Update tags that contain blobs that were re-replicated, and/or
%      re-replicate tags that don't have enough replicas.
%
% GC and RR are performed in the following phases:
%
% - Startup and initialization  (phase = start)
%
%     The various master and node processes are started up, and a
%     current listing of tags in the system is retrieved from
%     ddfs_master.  Each node, meanwhile, traverses all its volumes
%     and creates a cache of all existing blobs and tags it finds,
%     deleting any partial files along the way (GC1).
%
% - Build snapshot              (phase = build_map/map_wait)
%
%     Two maps (gc_tag_map, gc_blob_map) of the current tags and blobs
%     in use (or referenced) in DDFS is built up.  These maps are
%     built on the master, which iterates over the current list of
%     tags in DDFS, and checks whether each blob referenced by a tag
%     is present on the node that is supposed to host it.  The node
%     keeps track of (touches) these in-use blobs.
%
%     To handle transient node disconnections, the master keeps state
%     so that it can resume this process when a node connection comes
%     back.  In this case, the master re-sends any in-use requests to
%     which it was expecting responses from the node.  On the node,
%     its list of in-use tags and blobs will be incomplete after a
%     re-connect, since the master only re-sends pending requests, not
%     all requests.  To handle this (and other cases, described
%     below), before deleting an object, a node needs to check with
%     the master whether it is in use.
%
%     If more than DDFS_GC_MAX_NODE_FAILURES node connections are
%     lost, then the GC/RR process is aborted.
%
%     GC cannot proceed safely unless this snapshot is built, since
%     otherwise it might delete data that is still in-use.  Hence, GC
%     aborts if the snapshot cannot be built.  For simplicity, we also
%     do not proceed with RR in this case.
%
%     Once the gc maps are built, the master notifies the nodes, which
%     enter the GC phase.
%
% - GC                          (phase = gc)
%
%     Each node now processes the untouched blobs and tags in its
%     cache, which are now potential orphans.  However, before
%     deleting any (GC2 and GC3), each node now checks with the master
%     whether each untouched tag or blob in its cache is still in use
%     at the master.  Also, to handle recent objects added to DDFS
%     after the snapshot was built, which would otherwise erroneously
%     be considered as orphans, it only deletes objects that are older
%     than ORPHANED_{BLOB,TAG}_EXPIRES.
%
%     The deletion of each potential orphan object is preceded by a
%     two-way check (tag -> node, node -> master).  As described
%     above, the node needs to re-do the check in the case it was
%     restarted during GC, since it will then have an incomplete
%     knowledge of in-use objects.
%
%     Another reason is the following: if node deleted its potential
%     orphans right away, without checking their status on the master,
%     objects would be deleted every time the node's hostname changes
%     and the first tag->node check fails.  If all hostnames were
%     changed at once, all files would be deleted without a warning.
%
%     Another reason is that during RR, tags need to be updated after
%     any RR of an in-use blob.  However, if the master goes down
%     after a blob RR, but before the tag is updated, then the blob RR
%     is lost.  Having the node check with the master before deleting
%     a blob allows the recovery of such lost blob replicas.
%
%     Once a node is done processing its potential orphans, it informs
%     the master.  Once all nodes are done with GC, the master then
%     deletes old items from the +deleted metatag (GC4), and proceeds
%     to RR.
%
%     If a node connection is reestablished during this phase, the
%     node rebuilds its cache, and treats each object as a potential
%     orphan.  This will make GC take longer, but is still safe.
%
%     If more than DDFS_GC_MAX_NODE_FAILURES node connections are
%     lost, then the GC/RR process is aborted.
%
% - RR                          (phase = rr_blobs/rr_blobs_wait/rr_tags)
%
%     Re-replication is done in two parts.  In the first part
%     (rr_blobs, rr_blobs_wait), the master first re-replicates each
%     blob that has fewer replicas than DDFS_BLOB_REPLICAS (RR1).
%     Then, in the second part (rr_tags), it processes tags that need
%     to be updated.
%
%     Tags may need to be updated (RR2) for one or more reasons: (i)
%     it has fewer than DDFS_TAG_MIN_REPLICAS, (ii) it contains blobs
%     that have been re-replicated, (iii) lost blob replicas have been
%     recovered for blobs in the tag.
%
%     Tag updates are performed by sending an update message to the
%     ddfs_tag process responsible for the tag.  This is done because
%     the tag may have been updated by the user after the snapshot was
%     built, in which case the update needs to be merged with the
%     current contents of the tag.
%
%     RR continues as long as possible, regardless of the loss of node
%     connections, since it is an inherently safe process.  However,
%     tag RR may fail due to too_many_failed_nodes if too many node
%     connections are lost.
%
%     RR completes once all blobs and tags are processed.
%
%
% DDFS node removal
%
% For a DDFS node to be safely removed from the cluster, the following
% conditions have to be satisfied:
%
% (1) no new data or metadata, or replicas of existing data or
%     metadata, should be written to the node while the removal is in
%     progress
%
% (2) the data and metadata already on the node needs to be replicated
%     to the other nodes in the cluster, so that blob and tag replica
%     quotas can be met without counting the replicas hosted on the
%     node
%
% (3) all references to blobs on the node should be removed from their
%     containing tags
%
% The third step is safety-critical: for instance, it should not
% result in a reference to the last available blob being removed, in
% case the other replicas of the blob are on nodes that are currently
% down.
%
% Implementation:
%
% A node pending removal is put on a 'blacklist'.  This blacklist is
% removed from the set of the writable DDFS nodes by ddfs_master when
% new blobs or tags are created, or when existing ones are
% re-replicated (1).

% Any blob or tag replicas found on a blacklisted node are not counted
% towards satisfying their replica quotas (2), and blob and tag
% replication is initiated if those quotas are not met (in phase
% rr_blobs and rr_tags).  Once enough backup blob replicas are ensured
% to be available, a 'filter' update message is sent to the tag (in
% phase rr_tags) to remove any references to blob replicas hosted on
% the blacklisted node (3).  The actual removal is performed by the
% corresponding ddfs_tag.
%
% The blacklisted node can be removed by the admin from the config
% after ensuring that no tag contains references to data hosted on it.
% After a node is put on the blacklist, there might need to be several
% runs of GC/RR before the node removal is safe.
%
% The blob reference removal requires an invariant: that any operations
% on a tag do not modify the replica set for a blob in the tag, other
% than perhaps removing the replica set completely.  This is because
% the safety computation is not atomic with the reference removal.  If
% the replica set is modified (e.g. by removing some replicas from the
% set that were relied on by the safety check) after the safety check
% but before the reference removal, the removal becomes unsafe.
%
% This invariant is ensured by comparing the tag id used for the
% safety check, with the tag id at the time of the filter operation.
% If the two differ, the filter operation is not performed.


-module(ddfs_gc_main).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([is_orphan/4, node_gc_done/1]).

-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_tag.hrl").
-include("ddfs_gc.hrl").

-define(NODE_RETRY_WAIT, 30000).

-type rr_next() :: {object_type(), object_name()}.
-record(state, {
          % dynamic state
          deleted_ages :: ets:tab(),
          phase    = start            :: phase(),
          gc_peers = gb_trees:empty() :: gb_tree(),

          last_response_time = now()  :: erlang:timestamp(),
          progress_timer              :: 'undefined' | timer:tref(),

          num_pending_reqs  = 0               :: non_neg_integer(),       % build_map/map_wait
          pending_nodes     = gb_sets:empty() :: gb_set(),                % gc
          rr_next           = undefined       :: 'undefined' | rr_next(), % rr_blobs
          rr_pid            = undefined       :: 'undefined' | pid(),     % rr_blobs

          % static state
          tags              = []      :: [object_name()],
          root              = ""      :: string(),
          blacklist         = []      :: [node()],

          tagmink :: non_neg_integer(),
          tagk    :: non_neg_integer(),
          blobk   :: non_neg_integer()}).
-type state() :: #state{}.

-type object_location() :: {node(), volume_name()}.

%% ===================================================================
%% launch entry point

-spec start_link(string(), ets:tab()) -> {ok, pid()} | {error, term()}.
start_link(Root, DeletedAges) ->
    case gen_server:start_link(?MODULE, {Root, DeletedAges}, []) of
        {ok, Pid} ->
            {ok, Pid};
        E ->
            E
    end.

-spec is_orphan(pid(), object_type(), object_name(), volume_name())
               -> {ok, boolean()}.
is_orphan(Master, Type, ObjName, Vol) ->
    gen_server:call(Master, {is_orphan, Type, ObjName, node(), Vol}).

-spec node_gc_done(pid()) -> 'ok'.
node_gc_done(Master) ->
    gen_server:cast(Master, {gc_done, node()}).

-spec add_replicas(pid(), object_name(), [url()]) -> 'ok'.
add_replicas(Master, BlobName, NewUrls) ->
    gen_server:cast(Master, {add_replicas, BlobName, NewUrls}).


%% ===================================================================
%% gen_server callbacks

init({Root, DeletedAges}) ->
    % Ensure only one gc process is running at a time.  We don't use a
    % named process to implement uniqueness so that our message queue
    % isn't corrupted by stray delayed messages.
    register(gc_lock, self()),
    State = #state{deleted_ages = DeletedAges,
                   root = Root,
                   tagmink = list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS")),
                   tagk = list_to_integer(disco:get_setting("DDFS_TAG_REPLICAS")),
                   blobk = list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS"))},

    % In-use blobs and tags are tracked differently.  Blobs are
    % immutable objects, and need to be explicitly re-replicated.
    % Tags are mutable objects, whose incarnation ids are updated on
    % every mutation; they are also implicitly re-replicated, since the
    % storage of every mutation creates the appropriate number of
    % replicas.
    %
    % Since blobs need to be re-replicated explicitly, we track all
    % their current locations.  Since tags are mutable, we don't track
    % their locations, but track their current incarnation: all older
    % incarnations will be garbage.  During the rr_tags phase, the
    % number of tag locations will be used to decide whether they need
    % re-replication.

    % gc_blob_map: {Key :: {object_name(), node()},
    %               State :: 'pending' | 'missing' | 'true' | 'false'}
    % gc_tag_map:  {Key :: tagname(),
    %               Id  :: erlang:timestamp()}
    _ = ets:new(gc_blob_map, [named_table, set, private]),
    _ = ets:new(gc_tag_map, [named_table, set, private]),

    process_flag(trap_exit, true),
    gen_server:cast(self(), start),
    {ok, State}.


handle_call({is_orphan, Type, ObjName, Node, Vol}, _, S) ->
    S1 = S#state{last_response_time = now()},
    {reply, check_is_orphan(S, Type, ObjName, Node, Vol), S1};

handle_call(dbg_get_state, _, S) ->
    {reply, S, S}.


handle_cast(start, #state{phase = start, tagmink = TagMinK} = S) ->
    error_logger:info_report({"GC: initializing"}),
    {OkNodes, Failed, Tags} = ddfs_master:get_tags(all),
    {ok, Blacklist} = ddfs_master:get_gc_blacklist(),
    {NumOk, NumFailed} = {length(OkNodes), length(Failed)},
    if NumOk > 0, NumFailed < TagMinK ->
            error_logger:info_report({"GC: building map, with", NumFailed,
                                      "failed nodes"}),
            Phase = build_map,
            Peers = start_gc_peers(OkNodes, self(), now(), Phase),
            % We iterate over the tags by messaging ourselves, so that
            % we keep processing our message queue, which would
            % otherwise fill up when we process very large numbers of
            % blobs and tags, causing potential for deadlock.  This
            % way, we can also react to node disconnects during the
            % tag processing.
            gen_server:cast(self(), {build_map, Tags}),
            {noreply, S#state{phase = Phase,
                              gc_peers = Peers,
                              tags = Tags,
                              blacklist = Blacklist}};
       true ->
            error_logger:error_report({"GC: stopping, too many failed nodes",
                                       NumFailed, TagMinK, NumOk}),
            {stop, normal, S}
    end;

handle_cast({retry_node, Node}, #state{phase = Phase} = S) ->
    error_logger:info_report({"GC: restarting connection to", Node, Phase}),
    Pid = ddfs_gc_node:start_gc_node(Node, self(), now(), Phase),
    Peers = update_peer(S#state.gc_peers, Node, Pid),
    case Phase of
        P when P =:= build_map; P =:= map_wait ->
            Pending = resend_pending(Node, Pid),
            % Assert an invariant.
            true = (Pending =< S#state.num_pending_reqs),
            ok;
        _ ->
            ok
    end,
    {noreply, S#state{gc_peers = Peers}};

handle_cast({build_map, [T|Tags]}, #state{phase = build_map} = S) ->
    case catch check_tag(T, S, ?MAX_TAG_OP_RETRIES) of
        {ok, Sent} ->
            gen_server:cast(self(), {build_map, Tags}),
            Pending = S#state.num_pending_reqs + Sent,
            {noreply, S#state{num_pending_reqs = Pending}};
        E ->
            % We failed to handle this tag; we cannot safely proceed
            % since otherwise we might erroneously consider its blobs
            % as orphans and delete them.
            error_logger:error_report({"GC: stopping, unable to get tag",
                                       T, E}),
            {stop, normal, S}
    end;
handle_cast({build_map, []}, #state{phase = build_map} = S) ->
    S1 = case num_pending_objects() of
             0 ->
                 % We have no more responses to wait for, we can enter
                 % the GC phase directly.
                 start_gc_phase(S#state{num_pending_reqs = 0});
             Pending ->
                 % We've sent out all our status requests, start a
                 % progress tracker to avoid stalling indefinitely.
                 %
                 % We currently use a timer that tracks gc protocol
                 % messages.  This will not catch spurious progress
                 % caused by protocol messages from repeated
                 % disconnects/reconnects of the same node.  A better
                 % way to ensure progress is for the timer to ensure
                 % that pending_msgs/pending_nodes decreases at each
                 % wakeup.  However, ensuring pending_nodes decreases
                 % requires estimating how long the gc phase at a node
                 % would last.
                 %
                 % Initialize last_response_time, which will now be
                 % updated whenever we get a gc protocol message.
                 error_logger:info_report({"GC: all build_map requests sent,",
                                           "entering map_wait"}),
                 {ok, ProgressTimer} =
                     timer:send_after(?GC_PROGRESS_INTERVAL, check_progress),
                 S#state{phase = map_wait,
                         last_response_time = now(),
                         progress_timer = ProgressTimer,
                         num_pending_reqs = Pending}
         end,
    {noreply, S1};

handle_cast({gc_done, Node}, #state{phase = gc, pending_nodes = Pending} = S) ->
    NewPending = gb_sets:delete(Node, Pending),
    S1 = case gb_sets:size(NewPending) of
             0 ->
                 % This was the last node we were waiting for to
                 % finish GC.  Update the deleted tag.
                 process_deleted(S#state.tags, S#state.deleted_ages),

                 error_logger:info_report({"GC: entering rr_blobs phase"}),
                 % We start the first phase of the RR phase.
                 RRPid = start_replicator(self()),
                 Start = ets:first(gc_blobs),
                 rereplicate_blob(S, Start),
                 S#state{phase = rr_blobs, rr_pid = RRPid};
             _ ->
                 S
         end,
    {noreply, S1#state{pending_nodes = NewPending,
                       last_response_time = now()}};

handle_cast({rr_blob, '$end_of_table'},
            #state{phase = rr_blobs, rr_pid = RR} = S) ->
    % We are done with sending replication requests; we now wait for
    % the replicator to terminate.
    error_logger:info_report({"GC: done sending blob replication requests",
                              "entering rr_blobs_wait"}),
    stop_replicator(RR),
    {noreply, S#state{phase = rr_blobs_wait}};
handle_cast({rr_blob, Next}, #state{phase = rr_blobs} = S) ->
    rereplicate_blob(S, Next),
    {noreply, S};
handle_cast({add_replicas, BlobName, NewUrls}, #state{phase = Phase} = S)
  when Phase =:= rr_blobs; Phase =:= rr_blobs_wait ->
    update_replicas(S, BlobName, NewUrls),
    {noreply, S};
handle_cast({add_replicas, _BlobName, _NewUrls} = M, #state{phase = Phase} = S) ->
    error_logger:info_report({"GC: ignoring late response", M, Phase}),
    {noreply, S};

handle_cast({rr_tags, [T|Tags]}, #state{phase = rr_tags} = S) ->
    update_tag(S, T, ?MAX_TAG_OP_RETRIES),
    gen_server:cast(self(), {rr_tags, Tags}),
    {noreply, S};
handle_cast({rr_tags, []}, #state{phase = rr_tags} = S) ->
    % We are done with the RR phase, and hence with GC!
    error_logger:info_report({"GC: tag update/replication done, done with GC!"}),
    node_broadcast(S#state.gc_peers, end_rr),
    {stop, normal, S}.

handle_info({check_blob_result, LocalObj, Status}, #state{phase = Phase} = S)
  when Phase =:= build_map, is_boolean(Status);
       Phase =:= map_wait, is_boolean(Status) ->
    Checked = check_blob_result(LocalObj, Status),
    Pending = S#state.num_pending_reqs - Checked,
    % Assert an invariant.
    true = (Pending >= 0),
    S1 = case Pending of
             0 when Phase =:= map_wait ->
                 % That was the last result we were waiting for; we
                 % can now enter the GC phase.
                 start_gc_phase(S);
             _ ->
                 S#state{num_pending_reqs = Pending,
                         last_response_time = now()}
         end,
    {noreply, S1};

handle_info(check_progress, #state{phase = Phase} = S)
  when Phase =:= build_map; Phase =:= map_wait; Phase =:= gc ->
    Since = timer:now_diff(now(), S#state.last_response_time),
    case Since < ?GC_PROGRESS_INTERVAL of
        true ->
            % We have been making forward progress, restart the
            % progress timer.
            {ok, T} = timer:send_after(?GC_PROGRESS_INTERVAL, check_progress),
            {noreply, S#state{progress_timer = T}};
        false ->
            % We haven't made progress. Stop GC.
            error_logger:error_report({"GC: progress timeout in phase",
                                       S#state.phase}),
            {stop, normal, S}
    end;
handle_info(check_progress, S) ->
    % We don't need this timer in the RR phases.
    {noreply, S#state{progress_timer = undefined}};

handle_info({'EXIT', Pid, Reason}, S) when Pid == self() ->
    error_logger:warning_report({"GC: dying on error!", Reason}),
    {stop, stop_requested, S};

handle_info({'EXIT', Pid, normal}, #state{phase = rr_blobs_wait, rr_pid = RR} = S)
  when Pid == RR  ->
    % The RR process has finished normally, and we can proceed to the
    % second phase of RR: start updating the tags.
    error_logger:info_report({"GC: done with blob replication, entering rr_tags"}),
    gen_server:cast(self(), {rr_tags, S#state.tags}),
    {noreply, S#state{rr_pid = undefined, phase = rr_tags}};
handle_info({'EXIT', Pid, Reason}, #state{phase = Phase, rr_pid = RR} = S)
  when Pid == RR  ->
    % Unexpected exit of RR process; exit.
    error_logger:error_report({"GC: unexpected exit of replicator",
                               Reason, Phase}),
    {stop, normal, S};

handle_info({'EXIT', Pid, Reason}, #state{phase = Phase} = S) ->
    case find_node(S#state.gc_peers, Pid) of
        undefined ->
            {noreply, S};
        {Node, Failures} when Failures > ?MAX_GC_NODE_FAILURES ->
            error_logger:error_report({"GC: too many failures",
                                       Node, Failures, Phase}),

            {stop, {too_many_failures, Node}, S};
        {Node, Failures} ->
            error_logger:warning_report({"GC: Node disconnected",
                                         Node, Failures, Reason, Phase}),
            schedule_retry(Node),
            {noreply, S}
    end;

% handle late replies to "catch gen_server:call" (via ddfs_master).
handle_info({Ref, _Msg}, S) when is_reference(Ref) ->
    {noreply, S}.

%% ===================================================================
%% gen_server callback stubs

terminate(normal, _S) ->
    ok;
terminate(Reason, #state{phase = Phase} = _S) ->
    error_logger:warning_report({"GC: dying", Reason, Phase}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% peer connection management and messaging protocol

-spec schedule_retry(node()) -> 'ok'.
schedule_retry(Node) ->
    Self = self(),
    _ = spawn(fun() ->
                      timer:sleep(?NODE_RETRY_WAIT),
                      gen_server:cast(Self, {retry_node, Node})
              end),
    ok.

-spec start_gc_peers([node()], pid(), erlang:timestamp(), phase()) -> gb_tree().
start_gc_peers(Nodes, Self, Now, Phase) ->
    lists:foldl(
      fun(N, Peers) ->
              Pid = ddfs_gc_node:start_gc_node(N, Self, Now, Phase),
              gb_trees:insert(N, {Pid, 0}, Peers)
      end, gb_trees:empty(), Nodes).

-spec find_peer(gb_tree(), node()) -> pid() | 'undefined'.
find_peer(Peers, Node) ->
    case gb_trees:lookup(Node, Peers) of
        none -> undefined;
        {value, {Pid, _}} -> Pid
    end.

-spec find_node(gb_tree(), pid()) -> {node(), non_neg_integer()} | 'undefined'.
find_node(Peers, Pid) ->
    Iter = gb_trees:iterator(Peers),
    Looper = fun(I, Loop) ->
                   case gb_trees:next(I) of
                       none -> undefined;
                       {Node, {Pid, Failures}, _} -> {Node, Failures};
                       {_, _, Next} -> Loop(Next, Loop)
                   end
           end,
    Looper(Iter, Looper).

-spec update_peer(gb_tree(), node(), pid()) -> gb_tree().
update_peer(Peers, Node, Pid) ->
    {_OldPid, Failures} = gb_trees:get(Node, Peers),
    gb_trees:enter(Node, {Pid, Failures+1}, Peers).

-spec resend_pending(node(), pid()) -> non_neg_integer().
resend_pending(Node, Pid) ->
    Objects = lists:flatten(ets:lookup(gc_blob_map, {{'$1', Node}, pending})),
    lists:foreach(fun([ObjName]) ->
                          node_send(Pid, {check_blob, ObjName})
                  end, Objects),
    length(Objects).


-spec node_broadcast(gb_tree(), protocol_msg()) -> 'ok'.
node_broadcast(Peers, Msg) ->
    lists:foreach(fun({Pid, _}) ->
                          node_send(Pid, Msg)
                  end, gb_trees:values(Peers)).

-spec node_send(pid(), protocol_msg()) -> 'ok'.
node_send(Pid, Msg) ->
    Pid ! Msg,
    ok.


%% ===================================================================
%% build_map and map_wait phases

-spec check_tag(tagname(), state(), non_neg_integer()) ->
                       {'ok', non_neg_integer()} | {'error', term()}.
check_tag(Tag, S, Retries) ->
    case catch ddfs_master:tag_operation(gc_get, Tag, ?GET_TAG_TIMEOUT) of
        {{missing, deleted}, false} ->
            error_logger:info_report({"deleted", Tag}),
            {ok,  0};
        {'EXIT', {timeout, _}} when Retries =/= 0 ->
            check_tag(Tag, S, Retries - 1);
        {TagId, TagUrls, TagReplicas} ->
            {ok, check_tag(S, Tag, TagId, TagUrls, TagReplicas)};
        E ->
            E
    end.

-spec check_tag(state(), tagname(), tagid(), [[url()]], [node()])
               -> non_neg_integer().
check_tag(S, Tag, TagId, TagUrls, TagReplicas) ->
    record_tag(S, Tag, TagId, TagReplicas),
    lists:foldl(fun(BlobSet, Sent) -> check_blobset(S, BlobSet, Sent) end,
                0, TagUrls).

-spec record_tag(state(), object_name(), tagid(), [node()]) -> 'ok'.
record_tag(_S, Tag, TagId, _TagReplicas) ->
    % Assert that TagId embeds the specified Tag name.
    {Tag, Tstamp} = ddfs_util:unpack_objname(TagId),
    % This should be the first and only entry for this tag.
    true = ets:insert_new(gc_tag_map, {Tag, Tstamp}),
    ok.

-spec check_blobset(state(), [url()], non_neg_integer())
                   -> non_neg_integer().
check_blobset(S, [Blob|BlobSet], Pending) ->
    Sent = check_blob_status(S, ddfs_url(Blob)),
    check_blobset(S, BlobSet, Pending + Sent);
check_blobset(_S, [], Pending) -> Pending.

-spec ddfs_url(url()) -> {'ignore', 'unknown'} | local_object().
ddfs_url(<<"tag://", _/binary>>) -> {ignore, unknown};
ddfs_url(Url) ->
    case ddfs_util:parse_url(Url) of
        not_ddfs ->
            {ignore, unknown};
        {Host, _Vol, _Type, _Hash, BlobName} ->
            case disco:slave_safe(Host) of
                false ->
                    error_logger:warning_report({"GC: Unknown host", Host}),
                    {BlobName, unknown};
                Node ->
                    {BlobName, Node}
            end
    end.

-spec check_blob_status(state(), {'ignore', 'unknown'} | local_object())
                       -> non_neg_integer().
check_blob_status(_S, {ignore, _}) -> 0;
check_blob_status(S, {ObjName, Node} = Key) ->
    case {find_peer(S#state.gc_peers, Node), ets:lookup(gc_blob_map, Key)} of
        {_, [{_, _}]} ->    % Previously seen object
            0;
        {undefined, []} ->  % Unknown node, new object
            ets:insert(gc_blob_map, {Key, missing}),
            0;
        {Pid, []} ->        % Known node, new object
            % Mark the object as pending, and send a status request.
            ets:insert(gc_blob_map, {Key, pending}),
            node_send(Pid, {check_blob, ObjName}),
            1
    end.

-spec num_pending_objects() -> non_neg_integer().
num_pending_objects() ->
    ets:select_count(gc_blob_map, [{{'_', pending}, [], [true]}]).

-spec check_blob_result(local_object(), boolean()) -> non_neg_integer().
check_blob_result(LocalObj, Status) ->
    case ets:lookup_element(gc_blob_map, LocalObj, 2) of
        pending ->
            ets:update_element(gc_blob_map, LocalObj, {2, Status}),
            1;
        _ ->
            0
    end.


%% ===================================================================
%% gc phase and replica recovery

-spec start_gc_phase(state()) -> state().
start_gc_phase(S) ->
    % We are done with building the in-use map.  Now, we need to
    % collect the nodes that host a replica of each known in-use blob,
    % and also add the nodes that host any recovered replicas.  We
    % store this in a new ETS, gc_blobs, which will store for each
    % blob, (a) the known locations, (b) any recovered locations,
    % and (c) for each blob, any new replicated locations.

    % gc_blobs: {Key :: object_name(),
    %            Present :: [node()],
    %            Recovered :: [object_location()],
    %            Update :: rep_update()}
    _ = ets:new(gc_blobs, [named_table, set, private]),

    _ = ets:foldl(
          % There is no clause for the 'pending' status, so that we
          % can assert if we start gc with any still-pending entries.
          fun({{BlobName, Node}, true}, _) ->
                  case ets:lookup(gc_blobs, BlobName) of
                      [] ->
                          Entry = {BlobName, [Node], [], noupdate},
                          ets:insert(gc_blobs, Entry);
                      [{_, Present, _, _}] ->
                          Acc = [Node | Present],
                          ets:update_element(gc_blobs, BlobName, {2, Acc})
                  end;
             ({{BlobName, _Node}, Status}, _)
                when Status =:= missing; Status =:= false ->
                  % Create an entry for missing blobs.  This allows
                  % us to recover them from other nodes if present
                  % (e.g. after hostname changes).
                  case ets:lookup(gc_blobs, BlobName) of
                      [] ->
                          Entry = {BlobName, [], [], noupdate},
                          ets:insert(gc_blobs, Entry);
                      [{_, _, _, _}] ->
                          true
                  end
          end, true, gc_blob_map),
    ets:delete(gc_blob_map),

    error_logger:info_report({"GC: entering gc phase"}),
    node_broadcast(S#state.gc_peers, start_gc),
    % Update the last_response_time to indicate forward progress.
    S#state{num_pending_reqs  = 0,
            pending_nodes = gb_sets:from_list(gb_trees:keys(S#state.gc_peers)),
            phase = gc,
            last_response_time = now()}.

-spec check_is_orphan(state(), object_type(), object_name(), node(), volume_name())
                     -> {ok, boolean()}.
check_is_orphan(_S, tag, Tag, _Node, _Vol) ->
    {TagName, Tstamp} = ddfs_util:unpack_objname(Tag),
    case ets:lookup(gc_tag_map, TagName) of
        [] ->
            % This tag was not present in our snapshot, but could have
            % been newly created.  Mark it as an orphan, but the node
            % will not delete it if it is recent.
            {ok, true};
        [{_, GcTstamp}] when Tstamp < GcTstamp ->
            % This is an older incarnation of the tag, hence an orphan.
            {ok, true};
        [{_, _GcTstamp}] ->
            % This is a current or newer incarnation.
            {ok, false}
    end;
check_is_orphan(S, blob, BlobName, Node, Vol) ->
    MaxReps = S#state.blobk + ?NUM_EXTRA_REPLICAS,
    % The gc mark/in-use protocol is resumable, but the node loses its
    % in-use knowledge when it goes down.  On reconnect, it might
    % perform orphan-checks on blobs that were already marked by the
    % master in a previous session with that node.  Similarly, we
    % might re-recover blobs again after a reconnect.
    case ets:lookup(gc_blobs, BlobName) of
        [] ->
            % This blob was not present in our snapshot, but could
            % have been newly created.  Mark it as an orphan, but the
            % node will not delete it if it is recent.
            {ok, true};
        [{_, Present, Recovered, _}] ->
            case {lists:member(Node, Present),
                  lists:member({Node, Vol}, Recovered)} of
                {true, _} ->
                    % Re-check of an already marked blob.
                    {ok, false};
                {_, true} ->
                    % Re-check of an already recovered blob.
                    {ok, false};
                % Use a fast path for the normal case when there is no
                % blacklist.
                {false, false}
                  when S#state.blacklist =:= [],
                       length(Present) + length(Recovered) > MaxReps ->
                    % This is a newly recovered replica, but we have
                    % more than enough replicas, so we can afford
                    % marking this as an orphan.
                    {ok, true};
                {false, false}
                  when S#state.blacklist =:= [] ->
                    % This is a usable, newly-recovered, lost replica;
                    % record the volume for later use.
                    NewRecovered = [{Node, Vol} | Recovered],
                    ets:update_element(gc_blobs, BlobName, {3, NewRecovered}),
                    {ok, false};
                {false, false} ->
                    {RepNodes, _RepVols} = lists:unzip(Recovered),
                    Blacklist = S#state.blacklist,
                    Usable = find_usable(Blacklist, lists:usort(RepNodes ++ Present)),
                    case length(Usable) > MaxReps of
                        true ->
                            {ok, true};
                        false ->
                            % Note that Node could belong to the blacklist; we
                            % still record the replica so that we can use it for
                            % re-replication if needed.
                            NewRecovered = [{Node, Vol} | Recovered],
                            ets:update_element(gc_blobs, BlobName, {3, NewRecovered}),
                            {ok, false}
                    end
            end
    end.

% GC4) Delete old deleted tags from the +deleted metatag
%
% We don't want to accumulate deleted tags in the +deleted list
% infinitely.  The downside of removing a tag from the list too early
% is that there might be a node still hosting a version of the tag
% file, which we just haven't seen yet.  If this node reappears and
% the tag has been already removed from +deleted, the tag will come
% back from dead.
%
% To prevent this from happening, we wait until all known entries of
% the tag have been garbage collected and the ?DELETED_TAG_EXPIRES
% quarantine period has passed.  We assume that the quarantine is long
% enough so that all temporarily unavailable nodes have time to
% resurrect during that time, i.e.  no nodes can re-appear after being
% gone for ?DELETED_TAG_EXPIRES milliseconds.
%
% The Ages table persists the time of death for each deleted tag.

-spec process_deleted([object_name()], ets:tab()) -> 'ok'.
process_deleted(Tags, Ages) ->
    error_logger:info_report({"GC: Pruning +deleted"}),
    Now = now(),

    % Let's start with the current list of deleted tags
    {ok, Deleted} = ddfs_master:tag_operation(get_tagnames,
                                              <<"+deleted">>,
                                              ?NODEOP_TIMEOUT),

    % Update the time of death for newly deleted tags
    gb_sets:fold(fun(Tag, none) ->
                         ets:insert_new(Ages, {Tag, Now}), none
                 end, none, Deleted),
    % Remove those tags from the candidate set which still have active
    % copies around.
    DelSet = gb_sets:subtract(Deleted, gb_sets:from_ordset(Tags)),

    lists:foreach(
      fun({Tag, Age}) ->
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
                      % Tag hasn't been dead long enough to be removed from
                      % +deleted
                      ok
              end
      end, ets:tab2list(Ages)).

%% ===================================================================
%% blacklist utilities

% This is more dialyzer friendly than an inline call.
-spec find_usable([node()], [node()]) -> [node()].
find_usable(BL, Nodes) ->
    [N || N <- Nodes, not lists:member(N, BL)].

-spec find_unusable([node()], [node()]) -> [node()].
find_unusable(BL, Nodes) ->
    [N || N <- Nodes, lists:member(N, BL)].

%% ===================================================================
%% blob rereplication

-type rep_result() :: 'noupdate' | {'update', [url()]}.

% Rereplicate at most one blob, and then return.
-spec rereplicate_blob(state(), rr_next() | '$end_of_table') -> 'ok'.
rereplicate_blob(_S, '$end_of_table' = End) ->
    gen_server:cast(self(), {rr_blob, End});
rereplicate_blob(S, BlobName) ->
    [{_, Present, Recovered, _}] = ets:lookup(gc_blobs, BlobName),
    FinalReps = rereplicate_blob(S, BlobName, Present, Recovered, S#state.blobk),
    ets:update_element(gc_blobs, BlobName, {4, FinalReps}),
    Next = ets:next(gc_blobs, BlobName),
    gen_server:cast(self(), {rr_blob, Next}).

% RR1) Re-replicate blobs that don't have enough replicas

-spec rereplicate_blob(state(), object_name(), [node()],
                       [object_location()], non_neg_integer())
                      -> rep_result().
rereplicate_blob(S, BlobName, Present, Recovered, Blobk) ->
    BL = S#state.blacklist,
    SafePresent = find_usable(BL, Present),
    SafeRecovered = [{N, V} || {N, V} <- Recovered, not lists:member(N, BL)],
    case {length(SafePresent), length(SafeRecovered)} of
        {NumPresent, NumRecovered}
          when NumRecovered =:= 0, NumPresent >= Blobk ->
            % No need for replication or blob update.
            noupdate;
        {NumPresent, NumRecovered}
          when NumRecovered > 0, NumPresent + NumRecovered >= Blobk ->
            % No need for new replication; containing tags need updating to
            % recover lost blob replicas.
            {update, []};
        {0, 0}
          when length(Present) =:= 0, length(Recovered) =:= 0 ->
            % We have no good copies from which to generate new replicas;
            % we have no option but to live with the current information.
            error_logger:warning_report({"GC: all replicas missing!!!", BlobName}),
            noupdate;
        {_NumPresent, NumRecovered} ->
            % Extra replicas are needed; we generate one new replica at a
            % time, in a single-shot way. We use any available replicas as
            % sources, including those from blacklisted nodes.
            {RepNodes, _RepVols} = lists:unzip(Recovered),
            OkNodes = RepNodes ++ Present,
            case {try_put_blob(S, BlobName, OkNodes, BL), NumRecovered} of
                {{error, _E}, 0} ->
                    noupdate;
                {{error, _E}, _} ->
                    % We should record the usable recovered replicas.
                    {update, []};
                {pending, _} ->
                    % Mark the blob as updatable (see update_replicas/3).
                    {update, []}
            end
    end.

-spec try_put_blob(state(), object_name(), [node(),...], [node()]) ->
                          'pending' | {'error', term()}.
try_put_blob(#state{rr_pid = RR} = S, BlobName, OkNodes, BL) ->
    case ddfs_master:new_blob(BlobName, 1, OkNodes ++ BL) of
        {ok, [PutUrl]} ->
            Srcs = [{find_peer(S#state.gc_peers, N), N} || N <- OkNodes],
            {SrcPeer, SrcNode} = ddfs_util:choose_random(Srcs),
            RR ! {put_blob, BlobName, SrcPeer, SrcNode, PutUrl},
            pending;
        E ->
            {error, E}
    end.

% gen_server update handler.
-spec update_replicas(state(), object_name(), [url()]) -> 'ok'.
update_replicas(_S, BlobName, NewUrls) ->
    % An update can only arrive for a blob that was marked for replication
    % (see rereplicate_blob/5).
    [{_, _P, _R, {update, Urls}}] = ets:lookup(gc_blobs, BlobName),
    Update = {update, NewUrls ++ Urls},
    _ = ets:update_element(gc_blobs, BlobName, {4, Update}),
    ok.


% Coordinate blob transfers in a separate process, which can wait for
% the result of the attempted replication, and reports the result back
% to the main gen_server.  If a peer goes down during the transfer, it
% is not retried, but a timeout error is reported instead.

-spec start_replicator(pid()) -> pid().
start_replicator(Master) ->
    spawn_link(fun() -> replicator(Master, 0) end).

-spec replicator(pid(), non_neg_integer()) -> no_return().
replicator(Master, Ref) ->
    receive
        {put_blob, BlobName, SrcPeer, SrcNode, PutUrl} ->
            do_put_blob(Master, Ref, BlobName, SrcPeer, SrcNode, PutUrl),
            replicator(Master, Ref + 1);
        rr_end ->
            ok
    end.

-spec stop_replicator(pid()) -> 'ok'.
stop_replicator(RR) ->
    RR ! rr_end,
    ok.

-spec do_put_blob(pid(), non_neg_integer(), object_name(),
                  pid(), node(), binary()) -> 'ok'.
do_put_blob(Master, Ref, BlobName, SrcPeer, SrcNode, PutUrl) ->
    SrcPeer ! {put_blob, self(), Ref, BlobName, PutUrl},
    wait_put_blob(Master, Ref, SrcNode).

-spec wait_put_blob(pid(), non_neg_integer(), node()) -> 'ok'.
wait_put_blob(Master, Ref, SrcNode) ->
    receive
        {Ref, {ok, BlobName, NewUrls}} ->
            add_replicas(Master, BlobName, NewUrls);
        {Ref, _E} ->
            ok;
        {_OldRef, {ok, BlobName, NewUrls}} ->
            % Delayed response.
            add_replicas(Master, BlobName, NewUrls),
            wait_put_blob(Master, Ref, SrcNode);
        {_OldRef, _OldResult} ->
            wait_put_blob(Master, Ref, SrcNode)
    after ?GC_PUT_TIMEOUT ->
            ok
    end.


%% ===================================================================
%% tag updates

-spec update_tag(state(), object_name(), non_neg_integer()) -> 'ok'.
update_tag(_S, _T, 0) ->
    ok;
update_tag(S, T, Retries) ->
    case catch ddfs_master:tag_operation(gc_get, T, ?GET_TAG_TIMEOUT) of
        {{missing, deleted}, false} ->
            ok;
        {'EXIT', {timeout, _}} ->
            update_tag(S, T, Retries - 1);
        {TagId, TagUrls, TagReplicas} ->
            update_tag_body(S, T, TagId, TagUrls, TagReplicas);
        _E ->
            ok
    end.

% RR2) Update tags that contain blobs that were re-replicated, and/or
%      re-replicate tags that don't have enough replicas.

-spec update_tag_body(state(), tagname(), tagid(), [[url()]], [node()])
                     -> 'ok'.
update_tag_body(S, Tag, Id, TagUrls, TagReplicas) ->
    % Collect the blobs that need updating, and compute their new
    % replica locations.
    Updates = collect_updates(S, TagUrls),
    UsableTagReplicas = find_usable(S#state.blacklist, TagReplicas),
    case {Updates, length(UsableTagReplicas)} of
        {[], NumTagReps} when NumTagReps >= S#state.tagk ->
            % There are no blob updates, and there are the requisite
            % number of tag replicas; tag doesn't need update.
            ok;
        _ ->
            % In all other cases, send the tag an update.
            Msg = {gc_rr_update, Updates, S#state.blacklist, Id},
            ddfs_master:tag_notify(Msg, Tag)
    end.

-spec collect_updates(state(), [[url()]]) -> [blob_update()].
collect_updates(S, BlobSets) ->
    collect_updates(S, BlobSets, []).
collect_updates(_S, [], Updates) ->
    Updates;
collect_updates(S, [[]|Rest], Updates) ->
    collect_updates(S, Rest, Updates);
collect_updates(S, [BlobSet|Rest], Updates) ->
    {[BlobName|_], Nodes} = lists:unzip([ddfs_url(Url) || Url <- BlobSet]),
    case BlobName of
        ignore ->
            collect_updates(S, Rest, Updates);
        _ ->
            case ets:lookup(gc_blobs, BlobName) of
                [] ->
                    % New blob added after GC/RR started.
                    collect_updates(S, Rest, Updates);
                [{_, P, _R, noupdate}] ->
                    % Check whether blacklist filtering is needed,
                    % provided we have enough replicas present.  Since
                    % this is marked noupdate, we have no usable
                    % recovered replicas.
                    SafePresent = find_usable(S#state.blacklist, P),
                    ToFilter = find_unusable(S#state.blacklist, Nodes),
                    case ((ToFilter =/= [])
                          andalso (length(SafePresent) >= S#state.blobk)) of
                        true ->
                            Update = {BlobName, filter},
                            collect_updates(S, Rest, [Update | Updates]);
                        false ->
                            % Blacklist filtering is not needed, or
                            % it's not yet safe.
                            collect_updates(S, Rest, Updates)
                    end;
                [{_, _P, R, {update, NewUrls}}] ->
                    add_blob_update(S, BlobName, R, NewUrls, Updates)
            end
    end.

-spec add_blob_update(state(), object_name(), [object_location()],
                      [url()], [blob_update()]) -> [blob_update()].
add_blob_update(S, BlobName, Recovered, NewUrls, AccUpdates) ->
    SafeRecovered = [{N, V} || {N, V} <- Recovered,
                               not lists:member(N, S#state.blacklist)],
    RecUrls = lists:map(
                fun({N, V}) ->
                        {ok, _Local, Url} = ddfs_util:hashdir(BlobName,
                                                              disco:host(N),
                                                              "blob",
                                                              S#state.root,
                                                              V),
                        Url
                end, SafeRecovered),
    AddedUrls = lists:usort(RecUrls ++ NewUrls),
    case AddedUrls of
        [] -> AccUpdates;
        _  -> [{BlobName, AddedUrls} | AccUpdates]
    end.
