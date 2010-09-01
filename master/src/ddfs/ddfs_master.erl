
-module(ddfs_master).
-behaviour(gen_server).

-export([start_link/0, stop/0, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-define(WEB_PORT, 8011).

-include("config.hrl").

-record(state, {nodes :: [{node(), {non_neg_integer(), non_neg_integer()}}],
                tags :: gb_tree(),
                tag_cache :: 'false' | gb_set(),
                blacklisted :: [node()]}).

start_link() ->
    error_logger:info_report([{"DDFS master starts"}]),
    case gen_server:start_link({local, ddfs_master}, ddfs_master, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

stop() -> not_implemented.

init(_Args) ->
    spawn_link(fun() -> monitor_diskspace() end),
    spawn_link(fun() -> ddfs_gc:start_gc() end),
    spawn_link(fun() -> refresh_tag_cache_proc() end),
    put(put_port, disco:get_setting("DDFS_PUT_PORT")),
    {ok, #state{tags = gb_trees:empty(),
                tag_cache = false,
                nodes = [],
                blacklisted = []}}.

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call({get_nodeinfo, all}, _From, #state{nodes = Nodes} = S) ->
    {reply, {ok, Nodes}, S};

handle_call(get_nodes, _From, #state{nodes = Nodes} = S) ->
    {reply, {ok, [Node || {Node, _} <- Nodes]}, S};

handle_call({choose_nodes, K, Exclude}, _, #state{blacklisted = BL} = S) ->
    % Node selection algorithm:
    % 1. try to choose K nodes randomly from all the nodes which have
    %    more than ?MIN_FREE_SPACE bytes free space available and which
    %    are not excluded or blacklisted.
    % 2. if K nodes cannot be found this way, choose the K emptiest
    %    nodes which are not excluded or blacklisted.
    Primary = [Node || {Node, {Free, _Total}} <- S#state.nodes,
                Free > ?MIN_FREE_SPACE / 1024] -- (Exclude ++ BL),
    if length(Primary) >= K ->
        {reply, {ok, ddfs_util:choose_random(Primary, K)}, S};
    true ->
        Secondary = lists:sublist([N || {N, _} <- lists:reverse(
            lists:keysort(2, S#state.nodes))] -- (Exclude ++ BL), K),
        {reply, {ok, Secondary}, S}
    end;

handle_call({new_blob, _, K, _}, _, #state{nodes = N} = S) when K > length(N) ->
    {reply, too_many_replicas, S};

handle_call({new_blob, Obj, K, Exclude}, _, S) ->
    {_, {ok, Nodes}, _} = handle_call({choose_nodes, K, Exclude}, none, S),
    Urls = [["http://", disco:host(Node), ":", get(put_port), "/ddfs/", Obj]
            || Node <- Nodes],
    {reply, {ok, Urls}, S};

% Tag request: Start a new tag server if one doesn't exist already. Forward
% the request to the tag server.
handle_call({tag, _M, _Tag}, _From, #state{nodes = []} = S) ->
    {reply, {error, no_nodes}, S};

handle_call({tag, M, Tag}, From, #state{tags = Tags, tag_cache = Cache} = S) ->
    {Pid, TagsN} =
        case gb_trees:lookup(Tag, Tags) of
            none ->
                Status =
                    case Cache =/= false andalso gb_sets:is_element(Tag, Cache) of
                        false when Cache =/= false -> notfound;
                        _ -> false
                    end,
                {ok, Server} = ddfs_tag:start(Tag, Status),
                erlang:monitor(process, Server),
                {Server, gb_trees:insert(Tag, Server, Tags)};
            {value, P} ->
                {P, Tags}
        end,
    gen_server:cast(Pid, {M, From}),
    {noreply, S#state{tags = TagsN,
        tag_cache = Cache =/= false andalso gb_sets:add(Tag, Cache)}};

handle_call({get_tags, Mode}, From, #state{nodes = Nodes} = S) ->
    spawn(fun() ->
        gen_server:reply(From, get_tags(Mode, [N || {N, _} <- Nodes]))
    end),
    {noreply, S}.

handle_cast({update_tag_cache, TagCache}, S) ->
    {noreply, S#state{tag_cache = TagCache}};

handle_cast({update_nodes, NewNodes}, #state{nodes = Nodes, tags = Tags} = S) ->
    error_logger:info_report({"DDFS UPDATE NODES", NewNodes}),
    Blacklisted = [Node || {Node, true} <- NewNodes],
    OldNodes = gb_trees:from_orddict(Nodes),
    UpdatedNodes = lists:keysort(1, [case gb_trees:lookup(Node, OldNodes) of
                                         none ->
                                             {Node, {0, 0}};
                                         {value, OldStats} ->
                                             {Node, OldStats}
                                     end || {Node, _Blacklisted} <- NewNodes]),
    if
        UpdatedNodes =/= Nodes ->
            [gen_server:cast(Pid, {die, none}) || Pid <- gb_trees:values(Tags)],
            spawn(fun() ->
                          refresh_tag_cache([Node || {Node, _} <- UpdatedNodes])
                  end),
            {noreply, S#state{nodes = UpdatedNodes,
                              blacklisted = Blacklisted,
                              tag_cache = false,
                              tags = gb_trees:empty()}};
        true ->
            {noreply, S#state{blacklisted = Blacklisted}}
    end;

handle_cast({update_nodestats, NewNodes}, #state{nodes = Nodes} = S) ->
    UpdatedNodes = [case gb_trees:lookup(Node, NewNodes) of
                        none ->
                            {Node, Stats};
                        {value, NewStats} ->
                            {Node, NewStats}
                    end || {Node, Stats} <- Nodes],
    {noreply, S#state{nodes = UpdatedNodes}}.

handle_info({'DOWN', _, _, Pid, _}, S) ->
    {noreply, S#state{tags = gb_trees:from_orddict(
        [X || {_, V} = X <- gb_trees:to_list(S#state.tags), V =/= Pid])}}.

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec get_tags('all' | 'filter', [node()]) -> {[node()], [node()], [binary()]};
              ('safe', [node()]) -> {'ok', [binary()]} | 'too_many_failed_nodes'.
get_tags(all, Nodes) ->
    {Replies, Failed} = gen_server:multi_call(Nodes,
        ddfs_node, get_tags, ?NODE_TIMEOUT),
    {OkNodes, Tags} = lists:unzip(Replies),
    {OkNodes, Failed, lists:usort(lists:flatten(Tags))};

get_tags(filter, Nodes) ->
    {OkNodes, Failed, Tags} = get_tags(all, Nodes),
    case gen_server:call(ddfs_master,
            {tag, get_deleted, <<"+deleted">>}, ?NODEOP_TIMEOUT) of
        {ok, Deleted} ->
            TagSet = gb_sets:from_ordset(Tags),
            DelSet = gb_sets:insert(<<"+deleted">>, Deleted),
            NotDeleted = gb_sets:to_list(gb_sets:subtract(TagSet, DelSet)),
            {OkNodes, Failed, NotDeleted};
        E ->
            E
    end;

get_tags(safe, Nodes) ->
    TagMinK = list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS")),
    case get_tags(filter, Nodes) of
        {_OkNodes, Failed, Tags} when length(Failed) < TagMinK ->
            {ok, Tags};
        _ ->
            too_many_failed_nodes
    end.

-spec monitor_diskspace() -> no_return().
monitor_diskspace() ->
    {ok, Nodes} = gen_server:call(ddfs_master, get_nodes),
    {Space, _F} = gen_server:multi_call(Nodes,
                                        ddfs_node,
                                        get_diskspace,
                                        ?NODE_TIMEOUT),
    gen_server:cast(ddfs_master,
                    {update_nodestats,
                     gb_trees:from_orddict(lists:keysort(1, Space))}),
    timer:sleep(?DISKSPACE_INTERVAL),
    monitor_diskspace().

-spec refresh_tag_cache_proc() -> no_return().
refresh_tag_cache_proc() ->
    {ok, Nodes} = gen_server:call(ddfs_master, get_nodes),
    refresh_tag_cache(Nodes),
    timer:sleep(?TAG_CACHE_INTERVAL),
    refresh_tag_cache_proc().

-spec refresh_tag_cache([node()]) -> 'ok'.
refresh_tag_cache(Nodes) ->
    TagMinK = list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS")),
    {Replies, Failed} = gen_server:multi_call(Nodes,
        ddfs_node, get_tags, ?NODE_TIMEOUT),
    if Nodes =/= [], length(Failed) < TagMinK ->
        {_OkNodes, Tags} = lists:unzip(Replies),
        gen_server:cast(ddfs_master,
            {update_tag_cache, gb_sets:from_list(lists:flatten(Tags))});
    true -> ok
    end.
