
-module(ddfs_tag).
-behaviour(gen_server).

-include("config.hrl").

-export([start/2, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-type replica() :: {timer:timestamp(), nonempty_string()}.
-type user_attr() :: [{binary(), binary()}].
-record(tagcontent, {id :: binary(),
                     last_modified :: binary(),
                     urls = [] :: [binary()],
                     user = [] :: user_attr()}).
-type tagcontent() :: #tagcontent{}.
-record(state, {tag, % :: binary(),
                data :: 'false'  | 'notfound' | 'deleted' | {'error', _}
                    | #tagcontent{},
                delayed :: {[{pid(), _}], [[binary()]]},
                timeout :: non_neg_integer(),
                replicas :: 'false' | 'too_many_failed_nodes'
                    | [{replica(), node()}],
                url_cache :: 'false' | gb_set()}).

-spec new_tagcontent(binary(), [binary()], user_attr()) -> tagcontent().
new_tagcontent(TagName, Urls, User) ->
    #tagcontent{id = TagName,
                last_modified = ddfs_util:format_timestamp(),
                urls = Urls,
                user = User}.

-spec make_tagdata(tagcontent()) -> binary().
make_tagdata(D) ->
    list_to_binary(mochijson2:encode({struct,
        [
            {<<"version">>, 1},
            {<<"id">>, D#tagcontent.id},
            {<<"last-modified">>, D#tagcontent.last_modified},
            {<<"urls">>, D#tagcontent.urls},
            {<<"user-data">>, {struct, D#tagcontent.user}}
        ]})).

-spec tag_lookup(any(), [binary()], [any()]) ->
     {'ok', [any()]} | {'error', _}.
tag_lookup(_, [], Results) ->
    {ok, lists:reverse(Results)};
tag_lookup(JsonBody, [Key|Rest], Results) ->
    case lists:keysearch(Key, 1, JsonBody) of
        {value, {_, Attrib}} ->
            tag_lookup(JsonBody, Rest, [Attrib|Results]);
        false ->
            case Key of
                <<"user-data">> ->
                    tag_lookup(JsonBody, Rest, [{struct, []} | Results]);
                _ ->
                    {error, not_found}
            end
    end.

-spec parse_tagcontent(binary()) -> {'error', _} | {'ok', tagcontent()}.
parse_tagcontent(TagData) ->
    case catch mochijson2:decode(TagData) of
        {'EXIT', _} ->
            {error, corrupted_json};
        {struct, Body} ->
            case tag_lookup(Body,
                            [<<"id">>, <<"last-modified">>, <<"urls">>, <<"user-data">>],
                            []) of
                {ok, [Id, LastModified, Urls, {struct, UserData}]} ->
                    {ok, #tagcontent{id = Id,
                                     last_modified = LastModified,
                                     urls = Urls,
                                     user = UserData}};
                _ -> {error, invalid_object}
            end
    end.

-spec get_current_values(#state{}) -> {[binary()], user_attr()}.
get_current_values(S) ->
    if is_record(S#state.data, tagcontent) ->
            C = S#state.data,
            {C#tagcontent.urls, C#tagcontent.user};
       true ->
            {[], []}
    end.

-spec get_new_values(#state{}, atom() | {'user', binary()}, binary()) ->
          {'error', _} | {'ok', {[binary()], user_attr()}}.
get_new_values(S, Field, Value) ->
    {CurrentUrls, CurrentUser} = get_current_values(S),
    case Field of
        urls ->
            case validate_urls(Value) of
                true ->
                    {ok, {Value, CurrentUser}};
                false ->
                    {error, invalid_url_object}
            end;
        {user, K} ->
            {ok, {CurrentUrls, [{K, Value} | proplists:delete(K, CurrentUser)]}}
    end.

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
                delayed = {[], []},
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
                        case parse_tagcontent(TagData) of
                            {ok, Content} ->
                                {Content, Repl, S#state.timeout};
                            E ->
                                {E, false, ?TAG_EXPIRES_ONERROR}
                        end;
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
handle_cast({{delayed_update, _}, ReplyTo}, #state{data = {error, _}} = S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

% Normal delayed update: Append urls to the buffer and exit.
% The requester will get a reply later.
handle_cast({{delayed_update, Urls}, ReplyTo},
        #state{delayed = {Waiters, OldUrls}, tag = Tag} = S) ->
    if OldUrls =:= [] ->
        spawn(fun() ->
            timer:sleep(?DELAYED_FLUSH_INTERVAL),
            ddfs:get_tag(ddfs_master, binary_to_list(Tag), all)
        end);
    true -> ok
    end,
    case validate_urls(Urls) of
        true ->
            {noreply, S#state{delayed = {[ReplyTo|Waiters], Urls ++ OldUrls}},
                S#state.timeout};
        false ->
            gen_server:reply(ReplyTo, {error, invalid_url_object}),
            {noreply, S, S#state.timeout}
    end;

% Before handling any other requests, flush pending delayed updates
handle_cast(M, #state{delayed = {Waiters, Urls}} = S0) when Urls =/= [] ->
    {noreply, S, _} = handle_cast({{update, Urls}, Waiters},
        S0#state{delayed = {[], []}}),
    handle_cast(M, S);

handle_cast({die, _}, S) ->
    {stop, normal, S};

handle_cast({{get, Attrib}, ReplyTo}, #state{data = #tagcontent{} = D} = S) ->
    R = case Attrib of
            all ->
                make_tagdata(S#state.data);
            urls ->
                list_to_binary(mochijson2:encode(D#tagcontent.urls));
            {user, A} ->
                case proplists:lookup(A, D#tagcontent.user) of
                    {_, V} -> mochijson2:encode(V);
                    _ -> unknown_attribute
                end
        end,
    gen_server:reply(ReplyTo, R),
    {noreply, S, S#state.timeout};

handle_cast({{get, _}, ReplyTo}, S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

handle_cast({_, ReplyTo}, #state{data = {error, _}} = S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

handle_cast({gc_get0, ReplyTo}, #state{data = #tagcontent{} = D} = S) ->
    R = {D#tagcontent.id, D#tagcontent.urls, S#state.replicas},
    gen_server:reply(ReplyTo, R),
    {noreply, S, S#state.timeout};
handle_cast({gc_get0, ReplyTo}, S) ->
    gen_server:reply(ReplyTo, {S#state.data, S#state.replicas}),
    {noreply, S, S#state.timeout};

handle_cast({{update, Urls}, ReplyTo}, #state{data = notfound} = S) ->
    handle_cast({{put, urls, Urls}, ReplyTo}, S);

handle_cast({{update, Urls}, ReplyTo}, #state{data = deleted} = S) ->
    handle_cast({{put, urls, Urls}, ReplyTo}, S);

handle_cast({{update, Urls}, ReplyTo}, #state{data = C} = S) ->
    case validate_urls(Urls) of
        true ->
            handle_cast({{put, urls, Urls ++ C#tagcontent.urls}, ReplyTo}, S);
        false ->
            send_replies(ReplyTo, {error, invalid_url_object}),
            {noreply, S, S#state.timeout}
    end;

handle_cast({{put, Field, Value}, ReplyTo}, S) ->
    case get_new_values(S, Field, Value) of
        {ok, {Urls, User}} ->
            TagName = ddfs_util:pack_objname(S#state.tag, now()),
            TagContent = new_tagcontent(TagName, Urls, User),
            TagData = make_tagdata(TagContent),
            case put_distribute({TagName, TagData, TagContent}) of
                {ok, DestNodes, DestUrls, Data} ->
                    if S#state.data == deleted ->
                            {ok, _} = remove_from_deleted(S#state.tag);
                       true -> ok
                    end,
                    send_replies(ReplyTo, {ok, DestUrls}),
                    S1 = S#state{data = Data, replicas = DestNodes},
                    {noreply, S1, S#state.timeout};
                {error, _} = E ->
                    send_replies(ReplyTo, E),
                    {noreply, S, S#state.timeout}
            end;
        {error, _} = E ->
            send_replies(ReplyTo, E),
            {noreply, S, S#state.timeout}
    end;

% Special operations for the +deleted metatag

handle_cast(M, #state{url_cache = false, data = notfound} = S) ->
    handle_cast(M, S#state{url_cache = gb_sets:empty()});

handle_cast(M, #state{url_cache = false, data = Data} = S) ->
    handle_cast(M, S#state{url_cache = gb_sets:from_list(Data#tagcontent.urls)});

handle_cast({{insert_deleted, Url}, ReplyTo}, #state{url_cache = Deleted} = S) ->
    DeletedU = gb_sets:add(Url, Deleted),
    handle_cast({{put, urls, gb_sets:to_list(DeletedU)}, ReplyTo},
        S#state{url_cache = DeletedU});

handle_cast({get_deleted, ReplyTo}, #state{url_cache = Deleted} = S) ->
    gen_server:reply(ReplyTo, {ok, Deleted}),
    {noreply, S, S#state.timeout};

handle_cast({{remove_deleted, Url}, ReplyTo}, #state{url_cache = Deleted} = S) ->
    DeletedU = gb_sets:delete_any(Url, Deleted),
    handle_cast({{put, urls, gb_sets:to_list(DeletedU)}, ReplyTo},
        S#state{url_cache = DeletedU}).

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call(_, _, S) -> {reply, ok, S}.

handle_info(timeout, S) ->
    handle_cast({die, none}, S).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

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
    [] == (catch lists:flatten([[1 || X <- L, not is_binary(X)] || L <- Urls])).

% Put transaction:
% 1. choose nodes
% 2. multicall nodes, send TagData -> receive temporary file names
% 3. if failures -> retry with other nodes
% 4. if all nodes fail before tagk replicas are written, fail
% 5. multicall with temp file names, rename
% 6. if all fail, fail
% 7. if at least one multicall succeeds, return updated tagdata, desturls

-spec put_distribute({binary(), binary(), tagcontent()}) ->
    {'error', _} | {'ok', [node()], [binary()], tagcontent()}.
put_distribute({TagName, _, TagContent} = Msg) ->
    case put_distribute(Msg, get(tagk), [], []) of
        {ok, TagVol} -> put_commit({TagName, TagContent}, TagVol);
        {error, _} = E -> E
    end.

-spec put_distribute({binary(), binary(), tagcontent()}, non_neg_integer(),
    [{node(), binary()}], [node()]) ->
        {'error', _} | {'ok', [{node(), binary()}]}.
put_distribute(_, K, OkNodes, _) when K == length(OkNodes) ->
    {ok, OkNodes};

put_distribute({TagName, TagData, _} = Msg, K, OkNodes, Exclude) ->
    TagMinK = get(min_tagk),
    K0 = K - length(OkNodes),
    {ok, Nodes} = gen_server:call(ddfs_master, {choose_nodes, K0, Exclude}),
    if Nodes =:= [], length(OkNodes) < TagMinK ->
        {error, replication_failed};
    Nodes =:= [] ->
        {ok, OkNodes};
    true ->
        {Replies, Failed} = gen_server:multi_call(Nodes,
                ddfs_node, {put_tag_data, {TagName, TagData}}, ?NODE_TIMEOUT),
        put_distribute(Msg, K,
            OkNodes ++ [{N, Vol} || {N, {ok, Vol}} <- Replies],
            Exclude ++ [N || {N, _} <- Replies] ++ Failed)
    end.

-spec put_commit({binary(), tagcontent()}, [{node(), binary()}]) ->
    {'error', 'commit_failed'} | {'ok', [node()], [binary()], tagcontent()}.

put_commit({TagName, TagContent}, TagVol) ->
    {Nodes, _} = lists:unzip(TagVol),
    {Ok, _} = gen_server:multi_call(Nodes, ddfs_node,
                {put_tag_commit, TagName, TagVol}, ?NODE_TIMEOUT),
    case [U || {_, {ok, U}} <- Ok] of
        [] ->
            {error, commit_failed};
        Urls ->
            {ok, Nodes, Urls, TagContent}
    end.

is_tag_deleted(<<"+deleted">>) -> false;
is_tag_deleted(Tag) ->
    case gen_server:call(ddfs_master,
            {tag, get_deleted, <<"+deleted">>}, ?NODEOP_TIMEOUT) of
        {ok, Deleted} ->
            gb_sets:is_member([<<"tag://", Tag/binary>>], Deleted);
        E -> E
    end.

remove_from_deleted(Tag) ->
    gen_server:call(ddfs_master, {tag, {remove_deleted, [<<"tag://", Tag/binary>>]},
        <<"+deleted">>}, ?NODEOP_TIMEOUT).
