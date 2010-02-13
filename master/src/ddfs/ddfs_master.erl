
-module(ddfs_master).
-behaviour(gen_server).

-export([start_link/0, stop/0, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-define(WEB_PORT, 8011).

-include("config.hrl").

-record(state, {nodes, put_port, tags, blacklisted}).

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
    put(put_port, disco:get_setting("DDFS_PUT_PORT")),
    {ok, #state{tags = gb_trees:empty(), nodes = [], blacklisted = []}}.

handle_call(get_nodes, _, S) ->
    {reply, {ok, [N || {_, N} <- S#state.nodes]}, S};

handle_call({choose_nodes, K, Exclude}, _, #state{blacklisted = BL} = S) ->
    Nodes = lists:sublist([N || {_, N} <- lists:reverse(
        lists:keysort(1, S#state.nodes))] -- (Exclude ++ BL), K),
    {reply, {ok, Nodes}, S};

handle_call({new_blob, BlobName, K}, From, S) ->
    Obj = [BlobName, "$", ddfs_util:timestamp()],
    handle_call({new_blob, Obj, K, []}, From, S);

handle_call({new_blob, _, K, _}, _, #state{nodes = N} = S) when K > length(N) ->
    {reply, too_many_replicas, S};

handle_call({new_blob, Obj, K, Exclude}, _, S) ->
    {_, {ok, Nodes}, _} = handle_call({choose_nodes, K, Exclude}, none, S),
    Urls = [["http://", string:sub_word(atom_to_list(N), 2, $@),
                ":", get(put_port), "/", Obj] || N <- Nodes], 
    {reply, {ok, Urls}, S};

% Tag request: Start a new tag server if one doesn't exist already. Forward
% the request to the tag server.
handle_call({tag, M, Tag}, From, #state{tags = Tags} = S) ->
    {Pid, TagsN} =
        case gb_trees:lookup(Tag, Tags) of
            none ->
                {ok, Server} = ddfs_tag:start(Tag),
                erlang:monitor(process, Server),
                {Server, gb_trees:insert(Tag, Server, Tags)};
            {value, P} ->
                {P, Tags}
        end,
    gen_server:cast(Pid, {M, From}),
    {noreply, S#state{tags = TagsN}};

handle_call({get_tags, Mode}, From, #state{nodes = Nodes} = S) ->
    spawn(fun() -> 
        gen_server:reply(From, get_tags(Mode, [N || {_, N} <- Nodes]))
    end),
    {noreply, S}.

handle_cast({update_nodes, NewNodes}, #state{nodes = Nodes} = S) ->
    Blacklisted = [Node || {Node, true} <- NewNodes],
    Nodes = lists:ukeymerge(2, Nodes,
        lists:sort([{0, Node} || {Node, _Blacklisted} <- NewNodes])),
    {noreply, S#state{nodes = Nodes, blacklisted = Blacklisted}};

handle_cast({update_nodestats, NewNodes}, #state{nodes = Nodes} = S) ->
    {noreply, S#state{nodes = lists:ukeymerge(2, NewNodes, Nodes)}}.

handle_info({'DOWN', _, _, Pid, _}, S) ->
    {noreply, S#state{tags = gb_trees:from_orddict(
        [X || {_, V} = X <- gb_trees:to_list(S#state.tags), V =/= Pid])}}.

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

get_tags(all, Nodes) ->
    {Replies, Failed} = gen_server:multi_call(Nodes, 
        ddfs_node, get_tags, ?NODE_TIMEOUT),
    {OkNodes, Tags} = lists:unzip(Replies),
    {OkNodes, Failed, lists:usort(lists:flatten(Tags))};

get_tags(filter, Nodes) ->
    {OkNodes, Failed, Tags} = get_tags(all, Nodes),
    {ok, Deleted} = gen_server:call(ddfs_master,
        {tag, get_urls, <<"+deleted">>}, ?NODEOP_TIMEOUT),
    TagSet = gb_sets:from_ordset(Tags),
    DelSet0 = gb_sets:from_ordset(
        [T || [<<"tag://", T/binary>>] <- Deleted]),
    DelSet = gb_sets:insert(<<"+deleted">>, DelSet0),
    {OkNodes, Failed, gb_sets:to_list(gb_sets:subtract(TagSet, DelSet))}.
    
monitor_diskspace() ->
    {ok, Nodes} = gen_server:call(ddfs_master, get_nodes),
    {Replies, _} = gen_server:multi_call(Nodes,
        ddfs_node, get_volumes, ?NODE_TIMEOUT),
    gen_server:cast(ddfs_master, {update_nodestats, lists:keysort(2,
        [{lists:sum([S || {S, _} <- Vol]), N} || {N, {Vol, _}} <- Replies])}),
    timer:sleep(?DISKSPACE_INTERVAL),
    monitor_diskspace().


