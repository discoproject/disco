
-module(ddfs_master).
-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-define(WEB_PORT, 8011).

-include("config.hrl").

-record(state, {nodes, put_port, tags, tagk}).

start_link(Config) ->
    error_logger:info_report([{"DDFS master starts"}]),
    case gen_server:start_link({local, ddfs_master}, ddfs_master, Config, []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

stop() -> not_implemented.

init(_Config) ->
    {ok, _Pid} = web:start([{port, ?WEB_PORT}]),
    Nodes = ['ddfs@dev-01', 'ddfs@dev-02', 'ddfs@dev-03'],
    
    spawn_link(fun() -> monitor_diskspace() end),
    spawn_link(fun() -> ddfs_gc:start_gc() end),
    
    % Port = application:get_env(put_port) ...
    {ok, #state{put_port = "8000",
                tags = gb_trees:empty(),
                tagk = lists:min([length(Nodes), ?TAG_REPLICAS]),
                nodes = lists:sort([{0, N} || N <- Nodes])}}.

handle_call(get_nodes, _, S) ->
    {reply, {ok, [N || {_, N} <- S#state.nodes]}, S};

% XXX: implement blacklisting by including bl'ed nodes in the Exclude list
handle_call({choose_nodes, K, Exclude}, _, S) ->
    Nodes = lists:sublist([N || {_, N} 
        <- lists:reverse(lists:keysort(1, S#state.nodes))] -- Exclude, K),
    {reply, {ok, Nodes}, S};

handle_call({new_blob, BlobName, K}, From, S) ->
    Obj = [BlobName, "$", ddfs_util:timestamp()],
    handle_call({new_blob, Obj, K, []}, From, S);

handle_call({new_blob, _, K, _}, _, #state{nodes = N} = S) when K > length(N) ->
    {reply, too_many_replicas, S};

handle_call({new_blob, Obj, K, Exclude}, _, S) ->
    P = S#state.put_port, 
    {_, {ok, Nodes}, _} = handle_call({choose_nodes, K, Exclude}, none, S),
    Urls = [["http://", string:sub_word(atom_to_list(N), 2, $@),
                ":", P, "/", Obj] || N <- Nodes], 
    {reply, {ok, Urls}, S};

% Tag request: Start a new tag server if one doesn't exist already. Forward
% the request to the tag server.
handle_call({tag, M, Tag}, From, #state{tags = Tags} = S) ->
    {Pid, TagsN} =
        case gb_trees:lookup(Tag, Tags) of
            none ->
                {ok, Server} = ddfs_tag:start(Tag, S#state.tagk),
                erlang:monitor(process, Server),
                {Server, gb_trees:insert(Tag, Server, Tags)};
            {value, P} ->
                {P, Tags}
        end,
    gen_server:cast(Pid, {M, From}),
    {noreply, S#state{tags = TagsN}};

handle_call({get_tags, Mode}, From, #state{nodes = Nodes, tagk = TagK} = S) ->
    spawn(fun() -> 
        gen_server:reply(From, get_tags(Mode, [N || {_, N} <- Nodes], TagK))
    end),
    {noreply, S}.

handle_cast({update_nodestats, NewNodes}, #state{nodes = Nodes} = S) ->
    {noreply, S#state{nodes = lists:ukeymerge(2, NewNodes, Nodes)}}.

handle_info({'DOWN', _, _, Pid, _}, S) ->
    {noreply, S#state{tags = gb_trees:from_orddict(
        [X || {_, V} = X <- gb_trees:to_list(S#state.tags), V =/= Pid])}}.

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

get_tags(all, Nodes, TagK) ->
    {Replies, Failed} = gen_server:multi_call(Nodes, 
        ddfs_node, get_tags, ?NODE_TIMEOUT),
    if length(Failed) >= TagK ->
        {error, too_many_failed_nodes};
    true ->
        {OkNodes, Tags} = lists:unzip(Replies),
        {ok, OkNodes, lists:usort(lists:flatten(Tags))}
    end;

get_tags(filter, Nodes, TagK) ->
    case get_tags(all, Nodes, TagK) of
        {ok, _, Tags} ->
            {ok, Deleted} = gen_server:call(ddfs_master,
                {tag, get_urls, <<"+deleted">>}, ?NODEOP_TIMEOUT),
            TagSet = gb_sets:from_ordset(Tags),
            DelSet0 = gb_sets:from_ordset(
                [T || [<<"tag://", T/binary>>] <- Deleted]),
            DelSet = gb_sets:insert(<<"+deleted">>, DelSet0),
            {ok, gb_sets:to_list(gb_sets:subtract(TagSet, DelSet))};
        E -> E
    end.
    
monitor_diskspace() ->
    {ok, Nodes} = gen_server:call(ddfs_master, get_nodes),
    {Replies, _} = gen_server:multi_call(Nodes,
        ddfs_node, get_volumes, ?NODE_TIMEOUT),
    gen_server:cast(ddfs_master, {update_nodestats, lists:keysort(2,
        [{lists:sum([S || {S, _} <- Vol]), N} || {N, {Vol, _}} <- Replies])}),
    timer:sleep(?DISKSPACE_INTERVAL),
    monitor_diskspace().


