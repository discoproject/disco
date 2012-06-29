% Both garbage collection (GC) and re-replication (RR) require working
% from a consistent snapshot of DDFS.  Since computing this snapshot
% is expensive in both time and memory, once computed, the snapshot is
% used for both purposes, and GC and RR are implemented together.  GC
% is performed before RR, so that GC can free up disk space that can
% be used by RR.
%
% GC performs the following operations:
%
% GC1) Remove leftover !partial. files (from failed PUT operations)
% GC2) Remove orphaned tags (old versions and deleted tags)
% GC3) Remove orphaned blobs (blobs not referred by any tag)
% GC4) Recover lost replicas for non-orphaned blobs (from lost tag updates)
% GC5) Delete old deleted tags from the +deleted metatag
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
%     a blob allows the recovery of such lost blob replicas (GC4).
%
%     Once a node is done processing its potential orphans, it informs
%     the master.  Once all nodes are done with GC, the master then
%     deletes old tags from the +deleted metatag (GC5), and proceeds
%     to RR.
%
%     If a node connection is reestablished during this phase, the
%     node rebuilds its cache, and treats each object as a potential
%     orphan.  This will make GC take longer, but is still safe.
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
%
% Any blob or tag replicas found on a blacklisted node are not counted
% towards satisfying their replica quotas (2), and blob and tag
% replication is initiated if those quotas are not met (in phase
% rr_blobs and rr_tags).  Once enough backup blob replicas are ensured
% to be available, a 'filter' update message is sent to the tag (in
% phase rr_tags) to remove any references to blob replicas hosted on
% the blacklisted node (3).  The actual removal is performed by the
% corresponding ddfs_tag.
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
%
% In the rr_tags phase, as the tags are scanned for updates, we also
% track whether there exist references to blob replicas on gc
% blacklisted nodes, and whether sufficient tag replicas exist on
% non-blacklisted nodes.  At the end of the phase, this helps to
% compute the set of blacklisted nodes that can be safely removed from
% DDFS.

-module(ddfs_gc_main).
-behaviour(gen_server).

-export([start_link/2, gc_status/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).
-export([is_orphan/4, node_gc_done/2]).

-include("common_types.hrl").
-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_tag.hrl").
-include("ddfs_gc.hrl").
-include("gs_util.hrl").

-define(NODE_RETRY_WAIT, 30000).

-type rr_next() :: object_name().
-record(state, {
          % dynamic state
          deleted_ages                :: ets:tab(),
          phase    = start            :: phase(),
          gc_peers = gb_trees:empty() :: gb_tree(),

          last_response_time = now()          :: erlang:timestamp(),
          progress_timer    = undefined       :: 'undefined' | timer:tref(),
          gc_stats          = init_gc_stats() :: gc_run_stats(),

          num_pending_reqs  = 0               :: non_neg_integer(),       % build_map/map_wait
          pending_nodes     = gb_sets:empty() :: gb_set(),                % gc
          rr_reqs           = 0               :: non_neg_integer(),       % rr_blobs
          rr_pid            = undefined       :: 'undefined' | pid(),     % rr_blobs
          safe_blacklist    = gb_sets:empty() :: gb_set(),                % rr_tags

          % static state
          tags              = []      :: [object_name()],
          root                        :: string(),
          blacklist         = []      :: [node()],

          tagk    :: non_neg_integer(),
          blobk   :: non_neg_integer()}).
-type state() :: #state{}.

-type object_location() :: {node(), volume_name()}.

%% ===================================================================
%% external API

-spec start_link(string(), ets:tab()) -> {ok, pid()} | {error, term()}.
start_link(Root, DeletedAges) ->
    case gen_server:start_link(?MODULE, {Root, DeletedAges}, []) of
        {ok, Pid} -> {ok, Pid};
        E         -> E
    end.

-spec gc_status(pid(), pid()) -> ok.
gc_status(Master, From) ->
    gen_server:cast(Master, {gc_status, From}).

%% ===================================================================
%% internal API

-spec is_orphan(pid(), object_type(), object_name(), volume_name())
               -> {ok, boolean()}.
is_orphan(Master, Type, ObjName, Vol) ->
    gen_server:call(Master, {is_orphan, Type, ObjName, node(), Vol}).

-spec node_gc_done(pid(), gc_run_stats()) -> ok.
node_gc_done(Master, GCStats) ->
    gen_server:cast(Master, {gc_done, node(), GCStats}).

-spec add_replicas(pid(), object_name(), [url()]) -> ok.
add_replicas(Master, BlobName, NewUrls) ->
    gen_server:cast(Master, {add_replicas, BlobName, NewUrls}).


%% ===================================================================
%% gen_server callbacks
-spec init({string(), ets:tab()}) -> gs_init().
init({Root, DeletedAges}) ->
    % Ensure only one gc process is running at a time.  We don't use a
    % named process to implement uniqueness so that our message queue
    % isn't corrupted by stray delayed messages.
    register(gc_lock, self()),
    State = #state{deleted_ages = DeletedAges,
                   root = Root,
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
    %               State :: 'pending' | 'missing' | check_blob_result()}
    % gc_tag_map:  {Key :: tagname(),
    %               Id  :: erlang:timestamp()}
    _ = ets:new(gc_blob_map, [named_table, set, private]),
    _ = ets:new(gc_tag_map, [named_table, set, private]),

    process_flag(trap_exit, true),
    gen_server:cast(self(), start),
    {ok, State}.

-type is_orphan_msg() :: {is_orphan, object_type(), object_name(),
                          node(), volume_name()}.
-spec handle_call(is_orphan_msg(), from(), state()) -> gs_reply(boolean());
                 (dbg_state_msg(), from(), state()) -> gs_reply(state()).
handle_call({is_orphan, Type, ObjName, Node, Vol}, _, S) ->
    S1 = S#state{last_response_time = now()},
    {reply, check_is_orphan(S, Type, ObjName, Node, Vol), S1};

handle_call(dbg_get_state, _, S) ->
    {reply, S, S}.

-type gc_status_msg()     :: {gc_status, pid()}.
-type retry_node_msg()    :: {retry_node, node()}.
-type build_map_msg()     ::  {build_map, [tagname()]}.
-type gc_done_msg()       :: {gc_done, node(), gc_run_stats()}.
-type rr_blob_msg()       :: {rr_blob, term()}.
-type add_replicas_msg()  :: {add_replicas, object_name(), [url()]}.
-type rr_tags_msg()       :: {rr_tags, [tagname()], non_neg_integer()}.

-spec handle_cast(gc_status_msg() | retry_node_msg() | build_map_msg()
                  | gc_done_msg() | rr_blob_msg()    | add_replicas_msg()
                  | rr_tags_msg(),
                  state()) -> gs_noreply() | gs_stop(stop_requested | shutdown).
handle_cast({gc_status, From}, #state{phase = P} = S) when is_pid(From) ->
    From ! {ok, P},
    {noreply, S};

handle_cast(start, #state{phase = start} = S) ->
    lager:info("GC: initializing"),
    {ok, Blacklist} = ddfs_master:gc_blacklist(),
    case get_all_tags() of
        {ok, Tags, OkNodes} ->
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
        E ->
            lager:error("GC: unable to start: ~p", [E]),
            cleanup_for_exit(S),
            {stop, shutdown, S}
    end;

handle_cast({retry_node, Node},
            #state{phase = Phase, gc_peers = GCPeers,
                   num_pending_reqs = NumPendingReqs} = S) ->
    lager:info("GC: retrying connection to ~p (in phase ~p)", [Node, Phase]),
    Pid = ddfs_gc_node:start_gc_node(Node, self(), now(), Phase),
    Peers = update_peer(GCPeers, Node, Pid),
    case Phase of
        P when P =:= build_map; P =:= map_wait ->
            Pending = resend_pending(Node, Pid),
            % Assert an invariant.
            true = (Pending =< NumPendingReqs),
            ok;
        _ ->
            ok
    end,
    {noreply, S#state{gc_peers = Peers}};

handle_cast({build_map, [T|Tags]},
            #state{phase = build_map, num_pending_reqs = PendingReqs} = S) ->
    Check = try check_tag(T, S, ?MAX_TAG_OP_RETRIES)
            catch K:V -> {error, {K,V}}
            end,
    case Check of
        {ok, Sent} ->
            gen_server:cast(self(), {build_map, Tags}),
            Pending = PendingReqs + Sent,
            {noreply, S#state{num_pending_reqs = Pending}};
        E ->
            % We failed to handle this tag; we cannot safely proceed
            % since otherwise we might erroneously consider its blobs
            % as orphans and delete them.
            lager:error("GC: stopping, unable to get tag ~p: ~p", [T, E]),
            cleanup_for_exit(S),
            {stop, shutdown, S}
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
                 lager:info("GC: all build_map requests sent, entering map_wait"),
                 {ok, ProgressTimer} =
                     timer:send_after(?GC_PROGRESS_INTERVAL, check_progress),
                 S#state{phase = map_wait,
                         last_response_time = now(),
                         progress_timer = ProgressTimer,
                         num_pending_reqs = Pending}
         end,
    {noreply, S1};

handle_cast({gc_done, Node, NodeStats}, #state{phase = gc,
                                               gc_stats = Stats,
                                               pending_nodes = Pending,
                                               deleted_ages = DeletedAges,
                                               rr_reqs = RReqs,
                                               tags = Tags} = S) ->
    print_gc_stats(Node, NodeStats),
    NewStats = add_gc_stats(Stats, NodeStats),
    NewPending = gb_sets:delete(Node, Pending),
    S1 = case gb_sets:size(NewPending) of
             0 ->
                 % This was the last node we were waiting for to
                 % finish GC.  Update the deleted tag.
                 process_deleted(Tags, DeletedAges),
                 % Due to tag deletion by GC, we might be able to free
                 % up some entries from the tag cache in ddfs_master.
                 ddfs_master:refresh_tag_cache(),
                 % Update stats.
                 print_gc_stats(all, NewStats),
                 ddfs_master:update_gc_stats(NewStats),

                 lager:info("GC: entering rr_blobs phase"),
                 % Start the replicator process which will
                 % synchronously replicate any blobs it is told to,
                 % and then iterate over all the blobs.
                 Sr = S#state{rr_pid = start_replicator(self())},
                 Start = ets:first(gc_blobs),
                 Reqs = rereplicate_blob(Sr, Start),
                 Sr#state{phase = rr_blobs, rr_reqs = RReqs + Reqs};
             Remaining ->
                 lager:info("GC: ~p nodes pending in gc", [Remaining]),
                 S
         end,
    {noreply, S1#state{pending_nodes = NewPending,
                       gc_stats = NewStats,
                       last_response_time = now()}};

handle_cast({rr_blob, '$end_of_table'},
            #state{phase = rr_blobs, rr_pid = RR, rr_reqs = RReqs} = S) ->
    % We are done with sending replication requests; we now wait for
    % the replicator to terminate.
    lager:info("GC: sent ~p blob replication requests, entering rr_blobs_wait",
               [RReqs]),
    stop_replicator(RR),
    {noreply, S#state{phase = rr_blobs_wait}};
handle_cast({rr_blob, Next}, #state{phase = rr_blobs, rr_reqs = RReqs} = S) ->
    Reqs = rereplicate_blob(S, Next),
    {noreply, S#state{rr_reqs = RReqs + Reqs}};
handle_cast({add_replicas, BlobName, NewUrls}, #state{phase = Phase,
                                                      rr_reqs = RReqs} = S)
  when Phase =:= rr_blobs; Phase =:= rr_blobs_wait ->
    update_replicas(S, BlobName, NewUrls),
    lager:info("GC: ~p replication requests pending", [RReqs - 1]),
    {noreply, S#state{rr_reqs = RReqs - 1}};
handle_cast({add_replicas, _BlobName, _NewUrls} = M, #state{phase = Phase,
                                                            rr_reqs = RReqs} = S) ->
    lager:info("GC: ignoring late response ~p (~p,~p)", [M, RReqs, Phase]),
    {noreply, S};

handle_cast({rr_tags, [T|Tags], Count}, #state{phase = rr_tags} = S) ->
    S1 = update_tag(S, T, ?MAX_TAG_OP_RETRIES),
    lager:info("GC: updated tag ~p (~p)", [T, Count]),
    gen_server:cast(self(), {rr_tags, Tags, Count + 1}),
    {noreply, S1};
handle_cast({rr_tags, [], Count}, #state{phase = rr_tags, gc_peers = Peers,
                                  safe_blacklist = Blacklist} = S) ->
    % We are done with the RR phase, and hence with GC!
    lager:info("GC: ~p tags updated/replication done, done with GC!", [Count]),
    node_broadcast(Peers, end_rr),
    % Update ddfs_master with the safe_blacklist.
    ddfs_master:safe_gc_blacklist(Blacklist),
    cleanup_for_exit(S),
    {stop, shutdown, S}.

-type check_blob_result_msg() :: {check_blob_result, local_object(),
                                  check_blob_result()}.
-spec handle_info(check_blob_result_msg() | check_progress
                  | {'EXIT', pid(), term()} | {reference(), term()},
                  state()) -> gs_noreply() | gs_stop(shutdown).

handle_info({check_blob_result, LocalObj, Status},
            #state{phase = Phase, num_pending_reqs = NumPendingReqs} = S)
  when Phase =:= build_map;
       Phase =:= map_wait ->
    Checked = check_blob_result(LocalObj, Status),
    Pending = NumPendingReqs - Checked,
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

handle_info(check_progress, #state{phase = Phase, last_response_time = LRT} = S)
  when Phase =:= build_map; Phase =:= map_wait; Phase =:= gc ->
    Since = timer:now_diff(now(), LRT),
    case Since < ?GC_PROGRESS_INTERVAL of
        true ->
            % We have been making forward progress, restart the
            % progress timer.
            {ok, T} = timer:send_after(?GC_PROGRESS_INTERVAL, check_progress),
            {noreply, S#state{progress_timer = T}};
        false ->
            % We haven't made progress. Stop GC.
            lager:error("GC: progress timeout in ~p", [Phase]),
            cleanup_for_exit(S),
            {stop, shutdown, S}
    end;
handle_info(check_progress, S) ->
    % We don't need this timer in the RR phases.
    {noreply, S#state{progress_timer = undefined}};

handle_info({'EXIT', Pid, Reason}, S) when Pid =:= self() ->
    lager:error("GC: dying on error: ~p", [Reason]),
    cleanup_for_exit(S),
    {stop, stop_requested, S};

handle_info({'EXIT', RR, normal},
            #state{phase = rr_blobs_wait, rr_pid = RR,
                   blacklist = BlackList, tags = Tags} = S) ->
    % The RR process has finished normally, and we can proceed to the
    % second phase of RR: start updating the tags.  Also, we
    % initialize the safe blacklist here.
    lager:info("GC: done with blob replication, replicating tags (~p pending)",
               [length(Tags)]),
    gen_server:cast(self(), {rr_tags, Tags, 0}),
    {noreply, S#state{rr_pid = undefined,
                      safe_blacklist = gb_sets:from_list(BlackList),
                      phase = rr_tags}};
handle_info({'EXIT', RR, Reason}, #state{phase = Phase, rr_pid = RR} = S) ->
    % Unexpected exit of RR process; exit.
    lager:error("GC: unexpected exit of replicator in ~p: ~p", [Phase, Reason]),
    cleanup_for_exit(S),
    {stop, shutdown, S};

handle_info({'EXIT', Pid, Reason},
            #state{phase = Phase, gc_peers = Peers} = S) ->
    case find_node(Peers, Pid) of
        undefined ->
            {noreply, S};
        {Node, Failures} when Failures > ?MAX_GC_NODE_FAILURES ->
            lager:error("GC: too many failures (~p) on node ~p (~p)",
                        [Failures, Node, Phase]),
            cleanup_for_exit(S),
            {stop, shutdown, S};
        {Node, Failures} ->
            lager:warning("GC: Node ~p disconnected (~p): ~p (~p)",
                          [Node, Failures, Reason, Phase]),
            schedule_retry(Node),
            {noreply, S}
    end;

% handle late replies to "catch gen_server:call" (via ddfs_master).
handle_info({Ref, _Msg}, S) when is_reference(Ref) ->
    {noreply, S}.

%% ===================================================================
%% gen_server callback stubs

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _S) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec format_status(term(), [term()]) -> [term()].
format_status(_Opt, [_PDict, S]) ->
    [{data, [{"State", [{phase, S#state.phase}]}]}].

%% ===================================================================
%% peer connection management and messaging protocol

-spec schedule_retry(node()) -> ok.
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


-spec node_broadcast(gb_tree(), protocol_msg()) -> ok.
node_broadcast(Peers, Msg) ->
    lists:foreach(fun({Pid, _}) ->
                          node_send(Pid, Msg)
                  end, gb_trees:values(Peers)).

-spec node_send(pid(), protocol_msg()) -> ok.
node_send(Pid, Msg) ->
    Pid ! Msg,
    ok.

-spec cleanup_for_exit(state()) -> ok.
cleanup_for_exit(#state{gc_peers = Peers, progress_timer = Timer, rr_pid = RR}) ->
    lists:foreach(fun({Pid, _}) -> exit(Pid, terminate)
                  end, gb_trees:values(Peers)),
    case is_pid(RR) of
        true -> exit(RR, terminate);
        _ -> ok
    end,
    _ = timer:cancel(Timer),
    ok.

%% ===================================================================
%% build_map and map_wait phases

-spec get_all_tags() -> {ok, [tagname()], [node()]} | {error, term()}.
get_all_tags() ->
    get_all_tags(?MAX_TAG_OP_RETRIES).

get_all_tags(Retries) ->
    try case ddfs_master:get_tags(gc) of
            {ok, _Tags, _OkNodes} = AllTags -> AllTags;
            E -> E
        end
    catch _:_ when Retries =/= 0 -> get_all_tags(Retries - 1);
          _:_ ->                    {error, timeout}
    end.

-spec check_tag(tagname(), state(), non_neg_integer()) ->
                       {ok, non_neg_integer()} | {error, term()}.
check_tag(Tag, S, Retries) ->
    try case ddfs_master:tag_operation(gc_get, Tag, ?GET_TAG_TIMEOUT) of
            {{missing, deleted}, false} ->
                {ok,  0};
            {TagId, TagUrls, TagReplicas} ->
                {ok, check_tag(S, Tag, TagId, TagUrls, TagReplicas)};
            E ->
                E
        end
    catch _:_ when Retries =/= 0 -> check_tag(Tag, S, Retries - 1);
          _:_                    -> {error, timeout}
    end.

-spec check_tag(state(), tagname(), tagid(), [[url()]], [node()])
               -> non_neg_integer().
check_tag(S, <<"+deleted">> = Tag, TagId, _TagUrls, TagReplicas) ->
    record_tag(S, Tag, TagId, TagReplicas),
    % Optimize out the list traversal over TagUrls, since it contains
    % tag:// urls only.
    0;
check_tag(S, Tag, TagId, TagUrls, TagReplicas) ->
    record_tag(S, Tag, TagId, TagReplicas),
    lists:foldl(fun(BlobSet, Sent) -> check_blobset(S, BlobSet, Sent) end,
                0, TagUrls).

-spec record_tag(state(), object_name(), tagid(), [node()]) -> ok.
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
                    lager:warning("GC: Unknown host ~p", [Host]),
                    {BlobName, unknown};
                Node ->
                    {BlobName, Node}
            end
    end.

-spec check_blob_status(state(), {'ignore', 'unknown'} | local_object())
                       -> non_neg_integer().
check_blob_status(_S, {ignore, _}) -> 0;
check_blob_status(#state{gc_peers = Peers}, {ObjName, Node} = Key) ->
    case {find_peer(Peers, Node), ets:lookup(gc_blob_map, Key)} of
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

-spec check_blob_result(local_object(), check_blob_result()) -> non_neg_integer().
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
start_gc_phase(#state{gc_peers = Peers} = S) ->
    % We are done with building the in-use map.  Now, we need to
    % collect the nodes that host a replica of each known in-use blob,
    % and also add the nodes that host any recovered replicas.  We
    % store this in a new ETS, gc_blobs, which will store for each
    % blob, (a) the known locations, (b) any recovered locations,
    % and (c) for each blob, any new replicated locations.

    % gc_blobs: {Key :: object_name(),
    %            Present :: [object_location()],
    %            Recovered :: [object_location()],
    %            Update :: rep_update()}
    _ = ets:new(gc_blobs, [named_table, set, private]),

    _ = ets:foldl(
          % There is no clause for the 'pending' status, so that we
          % can assert if we start gc with any still-pending entries.
          fun({{BlobName, Node}, {true, Vol}}, _) ->
                  case ets:lookup(gc_blobs, BlobName) of
                      [] ->
                          Entry = {BlobName, [{Node, Vol}], [], noupdate},
                          ets:insert(gc_blobs, Entry);
                      [{_, Present, _, _}] ->
                          Acc = [{Node, Vol} | Present],
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

    lager:info("GC: entering gc phase"),
    node_broadcast(Peers, start_gc),
    % Update the last_response_time to indicate forward progress.
    S#state{num_pending_reqs  = 0,
            pending_nodes = gb_sets:from_list(gb_trees:keys(Peers)),
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
check_is_orphan(#state{blobk = BlobK, blacklist = BlackList},
                blob, BlobName, Node, Vol) ->
    MaxReps = BlobK + ?NUM_EXTRA_REPLICAS,
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
            PresentNodes = [N || {N, _V} <- Present],
            case {lists:member(Node, PresentNodes),
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
                  when BlackList =:= [],
                       length(Present) + length(Recovered) > MaxReps ->
                    % This is a newly recovered replica, but we have
                    % more than enough replicas, so we can afford
                    % marking this as an orphan.
                    lager:info("GC: discarding replica of ~p on ~p/~p",
                               [BlobName, Node, Vol]),
                    {ok, true};
                {false, false}
                  when BlackList =:= [] ->
                    % This is a usable, newly-recovered, lost replica;
                    % record the volume for later use.
                    lager:info("GC: recovering replica of ~p from ~p/~p",
                               [BlobName, Node, Vol]),
                    NewRecovered = [{Node, Vol} | Recovered],
                    ets:update_element(gc_blobs, BlobName, {3, NewRecovered}),
                    {ok, false};
                {false, false} ->
                    {RepNodes, _RepVols} = lists:unzip(Recovered),
                    Usable = find_usable(BlackList, lists:usort(RepNodes ++ PresentNodes)),
                    case length(Usable) > MaxReps of
                        true ->
                            {ok, true};
                        false ->
                            % Note that Node could belong to the blacklist; we
                            % still record the replica so that we can use it for
                            % re-replication if needed.
                            lager:info("GC: recovering replica of ~p from ~p/~p",
                                       [BlobName, Node, Vol]),
                            NewRecovered = [{Node, Vol} | Recovered],
                            ets:update_element(gc_blobs, BlobName, {3, NewRecovered}),
                            {ok, false}
                    end
            end
    end.

-spec init_gc_stats() -> gc_run_stats().
init_gc_stats() ->
    {{{0,0}, {0,0}}, {{0,0}, {0,0}}}.
-spec add_gc_stats(gc_run_stats(), gc_run_stats()) -> gc_run_stats().
add_gc_stats({T1, B1}, {T2, B2}) ->
    {add_obj_stats(T1, T2), add_obj_stats(B1, B2)}.
-spec add_obj_stats(obj_stats(), obj_stats()) -> obj_stats().
add_obj_stats({K1, D1}, {K2, D2}) ->
    {add_gc_stat(K1, K2), add_gc_stat(D1, D2)}.
-spec add_gc_stat(gc_stat(), gc_stat()) -> gc_stat().
add_gc_stat({F1, B1}, {F2, B2}) ->
    {F1 + F2, B1 + B2}.

-spec print_gc_stats(node(), gc_run_stats()) -> ok.
print_gc_stats(all, {Tags, Blobs}) ->
    lager:info("Total GC Stats: ~p  ~p",
               [obj_stats(tag, Tags), obj_stats(blob, Blobs)]);
print_gc_stats(Node, {Tags, Blobs}) ->
    lager:info("Node GC Stats for ~p: ~p  ~p",
               [Node, obj_stats(tag, Tags), obj_stats(blob, Blobs)]).
-spec obj_stats(object_type(), obj_stats()) -> term().
obj_stats(Type, {{KeptF, KeptB}, {DelF, DelB}}) ->
    {Type, "kept", {KeptF, KeptB}, "deleted", {DelF, DelB}}.


% GC5) Delete old deleted tags from the +deleted metatag
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

-spec process_deleted([object_name()], ets:tab()) -> ok.
process_deleted(Tags, Ages) ->
    lager:info("GC: Pruning +deleted"),
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
                      lager:info("REMOVED ~p from +DELETED", [Tag]),
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
-spec rereplicate_blob(state(), rr_next() | '$end_of_table') -> non_neg_integer().
rereplicate_blob(_S, '$end_of_table' = End) ->
    gen_server:cast(self(), {rr_blob, End}),
    0;
rereplicate_blob(S, BlobName) ->
    [{_, Present, Recovered, _}] = ets:lookup(gc_blobs, BlobName),
    {FinalReps, Reqs} = rereplicate_blob(S, BlobName, Present, Recovered, S#state.blobk),
    ets:update_element(gc_blobs, BlobName, {4, FinalReps}),
    Next = ets:next(gc_blobs, BlobName),
    gen_server:cast(self(), {rr_blob, Next}),
    Reqs.

% RR1) Re-replicate blobs that don't have enough replicas

-spec rereplicate_blob(state(), object_name(), [object_location()],
                       [object_location()], non_neg_integer())
                      -> {rep_result(), non_neg_integer()}.
rereplicate_blob(#state{blacklist = BL} = S,
                 BlobName, Present, Recovered, Blobk) ->
    PresentNodes = [N || {N, _V} <- Present],
    SafePresent = find_usable(BL, PresentNodes),
    SafeRecovered = [{N, V} || {N, V} <- Recovered, not lists:member(N, BL)],
    case {length(SafePresent), length(SafeRecovered)} of
        {NumPresent, NumRecovered}
          when NumRecovered =:= 0, NumPresent >= Blobk ->
            % No need for replication or blob update.
            {noupdate, 0};
        {NumPresent, NumRecovered}
          when NumRecovered > 0, NumPresent + NumRecovered >= Blobk ->
            % No need for new replication; containing tags need updating to
            % recover lost blob replicas.
            {{update, []}, 0};
        {0, 0}
          when Present =:= [], Recovered =:= [] ->
            % We have no good copies from which to generate new replicas;
            % we have no option but to live with the current information.
            lager:warning("GC: all replicas missing for ~p!!!", [BlobName]),
            {noupdate, 0};
        {NumPresent, NumRecovered} ->
            % Extra replicas are needed; we generate one new replica at a
            % time, in a single-shot way. We use any available replicas as
            % sources, including those from blacklisted nodes.
            {RepNodes, _RepVols} = lists:unzip(Recovered),
            OkNodes = RepNodes ++ PresentNodes,
            case {try_put_blob(S, BlobName, OkNodes, BL), NumRecovered} of
                {{error, E}, 0} ->
                    lager:info("GC: rr for ~p "
                               "(with ~p replicas recorded) "
                               "failed: ~p",
                               [BlobName, NumPresent, E]),
                    {noupdate, 0};
                {{error, E}, _} ->
                    lager:info("GC: rr for ~p "
                               "(with ~p/~p replicas recorded/recovered) "
                               "failed: ~p",
                               [BlobName, NumPresent, NumRecovered, E]),
                    % We should record the usable recovered replicas.
                    {{update, []}, 0};
                {pending, _} ->
                    lager:info("GC: rr for ~p "
                               "(with ~p/~p replicas recorded/recovered) "
                               "initiated",
                               [BlobName, NumPresent, NumRecovered]),
                    % Mark the blob as updatable (see update_replicas/3).
                    {{update, []}, 1}
            end
    end.

-spec try_put_blob(state(), object_name(), [node(),...], [node()]) ->
                          pending | {error, term()}.
try_put_blob(#state{rr_pid = RR, gc_peers = Peers}, BlobName, OkNodes, BL) ->
    case ddfs_master:new_blob(BlobName, 1, OkNodes ++ BL) of
        {ok, [PutUrl]} ->
            Srcs = [{find_peer(Peers, N), N} || N <- OkNodes],
            {SrcPeer, SrcNode} = disco_util:choose_random(Srcs),
            RealPutUrl = ddfs_util:cluster_url(PutUrl, put),
            RR ! {put_blob, BlobName, SrcPeer, SrcNode, RealPutUrl},
            pending;
        E ->
            {error, E}
    end.

% gen_server update handler.
-spec update_replicas(state(), object_name(), [url()]) -> ok.
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

-record(rep_state, {
          master       :: pid(),
          ref      = 0 :: non_neg_integer(),
          timeouts = 0 :: non_neg_integer()}).
-type rep_state() :: #rep_state{}.

-spec start_replicator(pid()) -> pid().
start_replicator(Master) ->
    spawn_link(fun() -> replicator(#rep_state{master = Master}) end).

-spec replicator(rep_state()) -> no_return().
replicator(#rep_state{ref = Ref, timeouts = TO} = S) ->
    receive
        {put_blob, BlobName, SrcPeer, SrcNode, PutUrl} ->
            S1 = do_put_blob(S, BlobName, SrcPeer, SrcNode, PutUrl),
            replicator(S1#rep_state{ref = Ref + 1});
        rr_end ->
            lager:info("GC: replication ending with Ref ~p, TO ~p", [Ref, TO]),
            ok
    end.

-spec stop_replicator(pid()) -> ok.
stop_replicator(RR) ->
    RR ! rr_end,
    ok.

-spec do_put_blob(rep_state(), object_name(), pid(), node(), binary())
                 -> rep_state().
do_put_blob(#rep_state{ref = Ref} = S, BlobName, SrcPeer, SrcNode, PutUrl) ->
    SrcPeer ! {put_blob, self(), Ref, BlobName, PutUrl},
    wait_put_blob(S, SrcNode, PutUrl).

-spec wait_put_blob(rep_state(), node(), url()) -> rep_state().
wait_put_blob(#rep_state{ref = Ref, timeouts = TO, master = Master} = S,
              SrcNode, PutUrl) ->
    receive
        {Ref, _B, _PU, {ok, BlobName, NewUrls}} ->
            lager:info("GC: replicated ~p (~p) to ~p", [BlobName, Ref, NewUrls]),
            add_replicas(Master, BlobName, NewUrls),
            S;
        {Ref, B, PU, E} ->
            lager:info("GC: error replicating ~p (~p) to ~p: ~p",
                       [B, Ref, PU, E]),
            S;
        {OldRef, _B, PU, {ok, BlobName, NewUrls}} ->
            % Delayed response.
            lager:info("GC: delayed replication of ~p (~p/~p) to ~p: ~p",
                       [BlobName, OldRef, Ref, PU, NewUrls]),
            add_replicas(Master, BlobName, NewUrls),
            wait_put_blob(S#rep_state{timeouts = TO - 1}, SrcNode, PutUrl);
        {OldRef, B, PU, OldResult} ->
            lager:info("GC: error replicating ~p (~p/~p) to ~p: ~p",
                       [B, OldRef, Ref, PU, OldResult]),
            wait_put_blob(S#rep_state{timeouts = TO - 1}, SrcNode, PutUrl)
    after ?GC_PUT_TIMEOUT ->
            lager:info("GC: replication timeout on ~p (~p) for ~p",
                       [SrcNode, Ref, PutUrl]),
            S#rep_state{timeouts = TO + 1}
    end.


%% ===================================================================
%% tag updates

-spec update_tag(state(), object_name(), non_neg_integer()) -> state().
update_tag(S, _T, 0) ->
    S;
update_tag(S, T, Retries) ->
    try case ddfs_master:tag_operation(gc_get, T, ?GET_TAG_TIMEOUT) of
            {{missing, deleted}, false} ->
                S;
            {TagId, TagUrls, TagReplicas} ->
                update_tag_body(S, T, TagId, TagUrls, TagReplicas);
            _E ->
                % If there is any error, we cannot ensure safety in
                % removing any blacklisted nodes; reset the safe
                % blacklist.
                lager:info("GC: Unable to retrieve tag ~p (rr_tags)", [T]),
                S#state{safe_blacklist = gb_sets:empty()}
        end
    catch _:_ -> update_tag(S, T, Retries - 1)
    end.

-spec log_blacklist_change(tagname(), gb_set(), gb_set()) -> ok.
log_blacklist_change(Tag, Old, New) ->
    case gb_sets:size(Old) =:= gb_sets:size(New) of
        true ->
            ok;
        false ->
            lager:info("GC: safe blacklist shrunk from ~p to ~p while processing ~p",
                       [gb_sets:to_list(Old), gb_sets:to_list(New), Tag])
    end.

% RR2) Update tags that contain blobs that were re-replicated, and/or
%      re-replicate tags that don't have enough replicas.

-spec update_tag_body(state(), tagname(), tagid(), [[url()]], [node()])
                     -> state().
update_tag_body(#state{safe_blacklist = SBL, blacklist = BL, tagk = TagK} = S,
                Tag, Id, TagUrls, TagReplicas) ->
    % Collect the blobs that need updating, and compute their new
    % replica locations.
    {Updates, SBL1} = collect_updates(S, TagUrls, SBL),
    UsableTagReplicas = find_usable(BL, TagReplicas),
    case {Updates, length(UsableTagReplicas)} of
        {[], NumTagReps} when NumTagReps >= TagK ->
            % There are no blob updates, and there are the requisite
            % number of tag replicas; tag doesn't need update.
            log_blacklist_change(Tag, SBL, SBL1),
            S#state{safe_blacklist = SBL1};
        _ ->
            % In all other cases, send the tag an update, and update
            % the safe_blacklist.
            SBL2 = gb_sets:subtract(SBL1, gb_sets:from_list(TagReplicas)),
            Msg = {gc_rr_update, Updates, BL, Id},
            lager:info("Updating tag ~p with ~p (blacklist ~p)",
                       [Id, Updates, BL]),
            ddfs_master:tag_notify(Msg, Tag),
            log_blacklist_change(Tag, SBL, SBL2),
            S#state{safe_blacklist = SBL2}
    end.

-spec collect_updates(state(), [[url()]], gb_set()) -> {[blob_update()], gb_set()}.
collect_updates(S, BlobSets, SafeBlacklist) ->
    collect(S, BlobSets, {[], SafeBlacklist}).
collect(_S, [], {_Updates, _SBL} = Result) ->
    Result;
collect(S, [[]|Rest], {_Updates, _SBL} = Acc) ->
    collect(S, Rest, Acc);
collect(#state{blacklist = BL, blobk = BlobK} = S,
        [BlobSet|Rest], {Updates, SBL} = Acc) ->
    {[BlobName|_], Nodes} = lists:unzip([ddfs_url(Url) || Url <- BlobSet]),
    case BlobName of
        ignore ->
            collect(S, Rest, Acc);
        _ ->
            % Any referenced nodes are not safe to be removed from DDFS.
            NewSBL = gb_sets:subtract(SBL, gb_sets:from_list(Nodes)),

            NewLocations = usable_locations(S, BlobName) -- BlobSet,
            BListed = find_unusable(BL, Nodes),
            Usable  = find_usable(BL, Nodes),
            CanFilter = [] =/= BListed andalso length(Usable) >= BlobK,
            case {NewLocations, CanFilter} of
                {[], false} ->
                    % Blacklist filtering is not needed, and there are
                    % no updates.
                    collect(S, Rest, {Updates, NewSBL});
                {[], true} ->
                    % Safely remove blacklisted nodes from the
                    % blobset.  (The tag will perform another safety
                    % check before removal using the tag id.)
                    Update = {BlobName, filter},
                    collect(S, Rest, {[Update | Updates], NewSBL});
                {[_|_], _} ->
                    % There are new usable locations for the blobset.
                    Update = {BlobName, NewLocations},
                    collect(S, Rest, {[Update | Updates], NewSBL})
            end
    end.

-spec usable_locations(state(), object_name()) -> [url()].
usable_locations(S, BlobName) ->
    case ets:lookup(gc_blobs, BlobName) of
        [] ->
            % New blob added after GC/RR started.
            [];
        [{_, P, R, noupdate}] ->
            usable_locations(S, BlobName, P ++ R, []);
        [{_, P, R, {update, NewUrls}}] ->
            usable_locations(S, BlobName, P ++ R, NewUrls)
    end.

-spec usable_locations(state(), object_name(), [object_location()],
                       [url()]) -> [url()].
usable_locations(#state{blacklist = BL, root = Root},
                 BlobName, Locations, NewUrls) ->
    CurUrls = [url(N, V, BlobName, Root) || {N, V} <- Locations,
                                            not lists:member(N, BL)],
    lists:usort(CurUrls ++ NewUrls).

url(N, V, Blob, Root) ->
    {ok, _Local, Url} = ddfs_util:hashdir(Blob, disco:host(N), "blob", Root, V),
    Url.
