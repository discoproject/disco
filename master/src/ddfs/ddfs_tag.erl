
-module(ddfs_tag).
-behaviour(gen_server).

-include("config.hrl").

-export([start/2, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-type replica() :: {timer:timestamp(), nonempty_string()}.
-record(state, {tag, % :: binary(),
                data :: 'false'  | 'notfound' | 'deleted' | {'error', _}
                    | binary(),
                delayed :: {[{pid(), _}], [[binary()]]},
                timeout :: non_neg_integer(),
                replicas :: 'false' | 'too_many_failed_nodes'
                    | [{replica(), node()}],
                url_cache :: 'false' | gb_set()}).

% THOUGHT: Eventually we want to partition tag keyspace instead
% of using a single global keyspace. This can be done relatively
% easily with consistent hashing: Each tag has a set of nodes
% (partition) which it operates on. If tag can't be found in its
% partition (due to addition of many new nodes, for instance),
% partition to be searched can be increased incrementally until
% the tag is found.
%
% Correspondingly GC'ing must ensure that K replicas for a tag
% are found within its partition rather than globally.

-spec start(binary(), 'notfound' | 'false') -> _.
start(TagName, Status) ->
    gen_server:start(ddfs_tag, {TagName, Status}, []).

-spec init({binary(), 'notfound' | 'false'}) -> {'ok', #state{}}.
init({TagName, Status}) ->
    put(min_tagk, list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS"))),
    put(tagk, list_to_integer(disco:get_setting("DDFS_TAG_REPLICAS"))),

    {ok, #state{tag = TagName,
                data = Status,
                delayed = false,
                replicas = false,
                url_cache = false,
                timeout =
                    if Status =:= notfound ->
                        ?TAG_EXPIRES_ONERROR;
                    true ->
                        ?TAG_EXPIRES
                    end
    }}.


%%% Note to the reader!
%%%
%%% handle_casts below form a state machine. The order in which the functions
%%% are specified is very significant. The easiest way to understand the state
%%% machine is to follow the logic from the top to the bottom.
%%%

% We don't want to cache requests made by garbage collector
handle_cast({gc_get, ReplyTo}, #state{data = false} = S) ->
    handle_cast({gc_get0, ReplyTo}, S#state{timeout = 100});

% On the other hand, if tag is already cached, GC'ing should
% not trash it
handle_cast({gc_get, ReplyTo}, S) ->
    handle_cast({gc_get0, ReplyTo}, S);

% First request for this tag: No tag data loaded - load it
handle_cast(M, #state{data = false} = S) ->
    {Data, Replicas, Timeout} =
        case is_tag_deleted(S#state.tag) of
            true ->
                {deleted, false, ?TAG_EXPIRES_ONERROR};
            false ->
                case get_tagdata(S#state.tag) of
                    {ok, TagData, Repl} ->
                        {TagData, Repl, S#state.timeout};
                    E ->
                        {E, false, ?TAG_EXPIRES_ONERROR}
                end;
            E ->
                {E, false, ?TAG_EXPIRES_ONERROR}
        end,
    handle_cast(M, S#state{
                data = Data,
                replicas = Replicas,
                timeout = lists:min([Timeout, S#state.timeout])});

% Delayed update requested but loading tag data failed, reply an error
handle_cast({{delayed_update, _, _}, ReplyTo},
            #state{data = {error, _}} = S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

% Delayed update with an empty buffer, initialize the buffer and a flush process
handle_cast({{delayed_update, _, _}, _} = M, #state{delayed = false} = S) ->
    spawn(fun() ->
        timer:sleep(?DELAYED_FLUSH_INTERVAL),
        ddfs:get_tag(ddfs_master, binary_to_list(S#state.tag))
    end),
    handle_cast(M, S#state{delayed = gb_trees:empty()});

% Normal delayed update, add request to the buffer, reply later
handle_cast({{delayed_update, Urls, Opt}, ReplyTo},
            #state{delayed = Buffer} = S) ->
    % We must handle updates with different set of options separately.
    % Thus requests are indexed by the normalized set of options (OptKey)
    OptKey = lists:sort(proplists:compact(Opt)),
    case validate_urls(Urls) of
        true ->
            NewBuffer =
                case gb_trees:lookup(OptKey, Buffer) of
                    none ->
                        gb_trees:insert(OptKey, {[ReplyTo], Urls}, Buffer);
                    {value, {OldWaiters, OldUrls}} ->
                        NWaiters = [ReplyTo|OldWaiters],
                        NUrls = Urls ++ OldUrls,
                        gb_trees:enter(OptKey, {NWaiters, NUrls}, Buffer)
                end,
            {noreply, S#state{delayed = NewBuffer}, S#state.timeout};
        false ->
            gen_server:reply(ReplyTo, {error, invalid_url_object}),
            {noreply, S, S#state.timeout}
    end;

% Before handling any other requests, flush pending delayed updates
handle_cast(M, #state{delayed = Buffer} = S0) when Buffer =/= false ->
    Fun =
        fun({Opt, {Waiters, Urls}}, S) ->
            {noreply, NS, _} = handle_cast({{update, Urls, Opt}, Waiters}, S),
            NS
        end,
    NewState =
        lists:foldl(Fun, S0#state{delayed = false}, gb_trees:to_list(Buffer)),
    handle_cast(M, NewState);

handle_cast({die, _}, S) ->
    {stop, normal, S};

handle_cast({get, ReplyTo}, S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

handle_cast({_, ReplyTo}, #state{data = {error, _}} = S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

handle_cast({gc_get0, ReplyTo}, S) ->
    gen_server:reply(ReplyTo, {S#state.data, S#state.replicas}),
    {noreply, S, S#state.timeout};

handle_cast({{update, _, _}, _ReplyTo} = M, #state{data = notfound} = S) ->
    update(M, [], S);

handle_cast({{update, _, _}, _ReplyTo} = M, #state{data = deleted} = S) ->
    update(M, [], S);

handle_cast({{update, _Urls, _Opt}, ReplyTo} = M, #state{data = D} = S) ->
    case parse_tagurls(D) of
        {ok, OldUrls} ->
            update(M, OldUrls, S);
        {error, _} = E ->
            send_replies(ReplyTo, E),
            {noreply, S, S#state.timeout}
    end;

handle_cast({{put, Urls}, ReplyTo}, S) ->
    case put_transaction(validate_urls(Urls), Urls, S) of
        {ok, DestNodes, DestUrls, TagData} ->
            if S#state.data == deleted ->
                {ok, _} = remove_from_deleted(S#state.tag);
            true -> ok
            end,
            send_replies(ReplyTo, {ok, DestUrls}),
            {noreply,
             S#state{data = TagData, replicas = DestNodes, url_cache = false},
             S#state.timeout};
        {error, _} = E ->
            send_replies(ReplyTo, E),
            {noreply, S#state{url_cache = false}, S#state.timeout}
    end;

% Special operations for the +deleted metatag

handle_cast(M, #state{url_cache = false, data = notfound} = S) ->
    handle_cast(M, S#state{url_cache = gb_sets:empty()});

handle_cast(M, #state{url_cache = false, data = Data} = S) ->
    {ok, Urls} = parse_tagurls(Data),
    handle_cast(M, S#state{url_cache = init_url_cache(Urls)});

handle_cast({{is_deleted, Tag}, ReplyTo}, #state{url_cache = Deleted} = S) ->
    Exists = gb_sets:is_member(ddfs_util:name_from_url(Tag), Deleted),
    gen_server:reply(ReplyTo, Exists),
    {noreply, S, S#state.timeout};

handle_cast({get_deleted, ReplyTo}, #state{url_cache = Deleted} = S) ->
    gen_server:reply(ReplyTo, {ok, Deleted}),
    {noreply, S, S#state.timeout};

handle_cast({{remove_deleted, Url}, ReplyTo}, #state{url_cache = Deleted} = S) ->
    Deleted0 = gb_sets:delete_any(ddfs_util:name_from_url(Url), Deleted),
    Deleted1 = [[<<"tag://", Tag/binary>>] || Tag <- gb_sets:to_list(Deleted0)],
    handle_cast({{put, Deleted1}, ReplyTo}, S).

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call(_, _, S) -> {reply, ok, S}.

handle_info(timeout, S) ->
    handle_cast({die, none}, S).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

update({{update, Urls, Opt}, ReplyTo}, OldUrls, State) ->
    case validate_urls(Urls) of
        true ->
            NoDup = proplists:is_defined(nodup, Opt),
            {Cache, Merged} = merge_urls(Urls,
                                         OldUrls,
                                         NoDup,
                                         State#state.url_cache),
            {noreply, NState, _} = handle_cast({{put, Merged}, ReplyTo}, State),
            {noreply, NState#state{url_cache = Cache}, NState#state.timeout};
        false ->
            send_replies(ReplyTo, {error, invalid_url_object}),
            {noreply, State, State#state.timeout}
    end.

% Normal update: Just add new urls to the list
merge_urls(NewUrls, OldUrls, false, _Cache) ->
    {false, NewUrls ++ OldUrls};

% Set update (nodup): Remove duplicates.
%
% Note that merge_urls should keep the original order of
% both NewUrls and OldUrls list. It should drop entries from
% NewUrls that are duplicates or already exist in OldUrls.
merge_urls(NewUrls, OldUrls, true, false) ->
    merge_urls(NewUrls, OldUrls, true, init_url_cache(OldUrls));

merge_urls(NewUrls, OldUrls, true, Cache) ->
    find_unseen(NewUrls, Cache, OldUrls).

find_unseen([], Seen, Urls) ->
    {Seen, Urls};
find_unseen([[Url|_] = Repl|Rest], Seen, Urls) ->
    Name = ddfs_util:name_from_url(Url),
    case {Name, gb_sets:is_member(Name, Seen)} of
        {false, _} ->
            find_unseen(Rest, Seen, [Repl|Urls]);
        {_, false} ->
            find_unseen(Rest, gb_sets:add(Name, Seen), [Repl|Urls]);
        {_, true} ->
            find_unseen(Rest, Seen, Urls)
    end.

init_url_cache(Urls) ->
    gb_sets:from_list([ddfs_util:name_from_url(Url) || [Url|_] <- Urls]).

send_replies(ReplyTo, Message) when is_tuple(ReplyTo) ->
    send_replies([ReplyTo], Message);
send_replies(ReplyToList, Message) ->
    [gen_server:reply(Re, Message) || Re <- ReplyToList].

-spec get_tagdata(binary()) -> 'notfound' | {'error', _}
                             | {'ok', binary(), [{replica(), node()}]}.
get_tagdata(Tag) ->
    {ok, Nodes} = gen_server:call(ddfs_master, get_nodes),
    {Replies, Failed} = gen_server:multi_call(Nodes, ddfs_node,
        {get_tag_timestamp, Tag}, ?NODE_TIMEOUT),
    TagMinK = get(min_tagk),
    case [{TagNfo, Node} || {Node, {ok, TagNfo}} <- Replies] of
        _ when length(Failed) >= TagMinK ->
            {error, too_many_failed_nodes};
        [] ->
            notfound;
        L ->
            {{Time, _Vol}, _Node} = lists:max(L),
            Replicas = [X || {{T, _}, _} = X <- L, T == Time],
            {TagNfo, SrcNode} = ddfs_util:choose_random(Replicas),
            TagName = ddfs_util:pack_objname(Tag, Time),
            case catch gen_server:call({ddfs_node, SrcNode},
                    {get_tag_data, TagName, TagNfo}, ?NODE_TIMEOUT) of
                {ok, Data} ->
                    {ok, Data, Replicas};
                {'EXIT', timeout} ->
                    {error, timeout};
                E ->
                    E
            end
    end.

-spec validate_urls([binary()]) -> bool().
validate_urls(Urls) ->
    [] =:= (catch lists:flatten([[1 || X <- L, not is_binary(X)] || L <- Urls])).

-spec parse_tagurls(binary()) -> {'error', _} | {'ok', [binary()]}.
parse_tagurls(TagData) ->
    case catch mochijson2:decode(TagData) of
        {'EXIT', _} ->
            {error, corrupted_json};
        {struct, Body} ->
            case lists:keysearch(<<"urls">>, 1, Body) of
                {value, {_, Urls}} ->
                    {ok, Urls};
                _ ->
                    {error, invalid_object}
            end;
        _ ->
            {error, invalid_json}
    end.

% Put transaction:
% 1. choose nodes
% 2. multicall nodes, send TagData -> receive temporary file names
% 3. if failures -> retry with other nodes
% 4. if all nodes fail before tagk replicas are written, fail
% 5. multicall with temp file names, rename
% 6. if all fail, fail
% 7. if at least one multicall succeeds, return updated tagdata, desturls

-spec put_transaction(bool(), [binary()], #state{}) ->
    {'error', _} | {'ok', [node()], [binary()], binary()}.
put_transaction(false, _, _) -> {error, invalid_url_object};
put_transaction(true, Urls, S) ->
    % XXX: compress data here!
    TagName = ddfs_util:pack_objname(S#state.tag, now()),
    TagData = list_to_binary(mochijson2:encode({struct,
                [
                    {<<"id">>, TagName},
                    {<<"version">>, 1},
                    {<<"urls">>, Urls},
                    {<<"last-modified">>, ddfs_util:format_timestamp()}
                ]})),
    put_distribute({TagName, TagData}).

-spec put_distribute({binary(), binary()}) ->
    {'error', _} | {'ok', [node()], [binary()], binary()}.
put_distribute(Msg) ->
    case put_distribute(Msg, get(tagk), [], []) of
        {ok, TagVol} ->
            put_commit(Msg, TagVol);
        {error, _} = E ->
            E
    end.

-spec put_distribute({binary(), binary()}, non_neg_integer(),
    [{node(), binary()}], [node()]) ->
        {'error', _} | {'ok', [{node(), binary()}]}.
put_distribute(_, K, OkNodes, _) when K == length(OkNodes) ->
    {ok, OkNodes};

put_distribute(Msg, K, OkNodes, Exclude) ->
    TagMinK = get(min_tagk),
    K0 = K - length(OkNodes),
    {ok, Nodes} = gen_server:call(ddfs_master, {choose_nodes, K0, Exclude}),
    if
        Nodes =:= [], length(OkNodes) < TagMinK ->
            {error, replication_failed};
        Nodes =:= [] ->
            {ok, OkNodes};
        true ->
            {Replies, Failed} = gen_server:multi_call(Nodes,
                                                      ddfs_node,
                                                      {put_tag_data, Msg},
                                                      ?NODE_TIMEOUT),
            put_distribute(Msg, K,
                           OkNodes ++ [{Node, VolName}
                                       || {Node, {ok, VolName}} <- Replies],
                           Exclude ++ [Node || {Node, _} <- Replies] ++ Failed)
    end.

-spec put_commit({binary(), binary()}, [{node(), binary()}]) ->
    {'error', 'commit_failed'} | {'ok', [node()], [binary()], binary()}.

put_commit({TagName, TagData}, TagVol) ->
    {Nodes, _} = lists:unzip(TagVol),
    {NodeUrls, _} = gen_server:multi_call(Nodes,
                                          ddfs_node,
                                          {put_tag_commit, TagName, TagVol},
                                          ?NODE_TIMEOUT),
    case [Url || {_Node, {ok, Url}} <- NodeUrls] of
        [] ->
            {error, commit_failed};
        Urls ->
            {ok, Nodes, Urls, TagData}
    end.

is_tag_deleted(<<"+deleted">>) -> false;
is_tag_deleted(Tag) ->
    BTag = <<"tag://", Tag/binary>>,
    Msg = {tag, {is_deleted, BTag}, <<"+deleted">>},
    gen_server:call(ddfs_master, Msg, ?NODEOP_TIMEOUT).

remove_from_deleted(Tag) ->
    Msg = {tag, {remove_deleted, <<"tag://", Tag/binary>>}, <<"+deleted">>},
    gen_server:call(ddfs_master, Msg, ?NODEOP_TIMEOUT).

