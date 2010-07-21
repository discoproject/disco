
-module(ddfs_tag).
-behaviour(gen_server).

-include("config.hrl").

-export([start/2, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-type replica() :: {timer:timestamp(), nonempty_string()}.
-type attrib() :: 'all' | 'urls' | 'read_token' | 'write_token'
                | {'user', binary()}.
-type user_attr() :: [{binary(), binary()}].
% An 'internal' token is also used by internal consumers, but never stored.
-type token() :: 'null' | binary().
-type replyto() :: {pid(), reference()}.

-record(tagcontent, {id :: binary(),
                     last_modified :: binary(),
                     read_token = null :: token(),
                     write_token = null :: token(),
                     urls = [] :: [binary()],
                     user = [] :: user_attr()}).
-type tagcontent() :: #tagcontent{}.
-record(state, {tag, % :: binary(),
                data :: 'false'  | 'notfound' | 'deleted' | {'error', _}
                    | #tagcontent{},
                delayed :: {[replyto()], [[binary()]]},
                timeout :: non_neg_integer(),
                replicas :: 'false' | 'too_many_failed_nodes'
                    | [{replica(), node()}],
                url_cache :: 'false' | gb_set()}).

-spec token_encode(token()) -> binary().
token_encode(T) ->
    if T == null ->
            mochijson2:encode(T);
       true ->
            list_to_binary(mochijson2:encode(T))
    end.

-spec new_tagcontent(binary(), token(), token(), [binary()], user_attr())
    -> tagcontent().
new_tagcontent(TagName, ReadToken, WriteToken, Urls, User) ->
    #tagcontent{id = TagName,
                last_modified = ddfs_util:format_timestamp(),
                read_token = ReadToken,
                write_token = WriteToken,
                urls = Urls,
                user = User}.

-spec make_tagdata(tagcontent()) -> binary().
make_tagdata(D) ->
    list_to_binary(mochijson2:encode({struct,
        [
            {<<"version">>, 1},
            {<<"id">>, D#tagcontent.id},
            {<<"last-modified">>, D#tagcontent.last_modified},
            {<<"read-token">>, D#tagcontent.read_token},
            {<<"write-token">>, D#tagcontent.write_token},
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
                <<"read-token">> ->
                    tag_lookup(JsonBody, Rest, [null|Results]);
                <<"write-token">> ->
                    tag_lookup(JsonBody, Rest, [null|Results]);
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
                            [<<"id">>, <<"last-modified">>,
                             <<"read-token">>, <<"write-token">>,
                             <<"urls">>, <<"user-data">>],
                            []) of
                {ok, [Id, LastModified, ReadToken, WriteToken,
                      Urls, {struct, UserData}]} ->
                    {ok, #tagcontent{id = Id,
                                     last_modified = LastModified,
                                     read_token = ReadToken,
                                     write_token = WriteToken,
                                     urls = Urls,
                                     user = UserData}};
                _ -> {error, invalid_object}
            end
    end.

-spec get_current_values(#state{}) -> {token(), token(),
                                       [binary()], user_attr()}.
get_current_values(#state{data = #tagcontent{} = D}) ->
    {D#tagcontent.read_token, D#tagcontent.write_token,
     D#tagcontent.urls, D#tagcontent.user};
get_current_values(#state{data = _}) ->
    {null, null, [], []}.

-spec get_new_values(#state{}, attrib(), binary()) ->
          {'error', _} | {'ok', {token(), token(), [binary()], user_attr()}}.
get_new_values(S, Field, Value) ->
    {ReadToken, WriteToken, Urls, User} = get_current_values(S),
    case Field of
        read_token ->
            T = list_to_binary(Value),
            {ok, {T, WriteToken, Urls, User}};
        write_token ->
            T = list_to_binary(Value),
            {ok, {ReadToken, T, Urls, User}};
        urls ->
            case validate_urls(Value) of
                true ->
                    {ok, {ReadToken, WriteToken, Value, User}};
                false ->
                    {error, invalid_url_object}
            end;
        {user, K} ->
            {ok, {ReadToken, WriteToken, Urls,
                  [{K, Value} | proplists:delete(K, User)]}}
    end.

-spec check_write_token(binary(), replyto(), #state{},
                        fun(() -> #state{})) -> #state{}.
check_write_token(Token, ReplyTo, S, Op) ->
    {_, WriteToken, _, _} = get_current_values(S),
    check_token(Token, ReplyTo, WriteToken, Op, S).

-spec check_read_token(binary(), replyto(), #state{},
                       fun(() -> #state{})) -> #state{}.
check_read_token(Token, ReplyTo, S, Op) ->
    {ReadToken, _, _, _} = get_current_values(S),
    check_token(Token, ReplyTo, ReadToken, Op, S).

-spec check_token(token(), replyto(), token(),
                  fun(() -> #state{}), #state{}) -> #state{}.
check_token(Token, ReplyTo, CurrentToken, Op, S) ->
    Auth = (Token =:= internal) or (Token =:= CurrentToken)
        or (CurrentToken =:= null),
    case Auth of
        true ->
            Op();
        false ->
            send_replies(ReplyTo, {error, unauthorized}),
            S
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
handle_cast({{delayed_update, _, _}, ReplyTo}, #state{data = {error, _}} = S) ->
    gen_server:reply(ReplyTo, S#state.data),
    {noreply, S, S#state.timeout};

% Normal delayed update: Append urls to the buffer and exit.
% The requester will get a reply later.
handle_cast({{delayed_update, Urls, Token}, ReplyTo},
            #state{delayed = {Waiters, OldUrls}, tag = Tag} = S) ->
    S1 =
        check_write_token(Token, ReplyTo, S,
                          fun() ->
                              do_delayed_update(Urls, ReplyTo, Waiters,
                                                OldUrls, Tag, S)
                          end),
    {noreply, S1, S1#state.timeout};

% Before handling any other requests, flush pending delayed updates
handle_cast(M, #state{delayed = {Waiters, Urls}} = S0) when Urls =/= [] ->
    {noreply, S, _} = handle_cast({{update, Urls, internal}, Waiters},
        S0#state{delayed = {[], []}}),
    handle_cast(M, S);

handle_cast({{delete, Token}, ReplyTo}, S) ->
    S1 = check_write_token(Token, ReplyTo, S,
                           fun () ->
                               do_delete(ReplyTo, S)
                           end),
    gen_server:reply(ReplyTo, ok),
    handle_cast({die, none}, S1);

handle_cast({die, _}, S) ->
    {stop, normal, S};

handle_cast({{get, Attrib, Token}, ReplyTo}, #state{data = #tagcontent{} = D} = S) ->
    S1 = check_read_token(Token, ReplyTo, S,
                          fun () ->
                                  do_get(Attrib, ReplyTo, D, S)
                          end),
    {noreply, S1, S1#state.timeout};

handle_cast({{get, _, _}, ReplyTo}, S) ->
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

handle_cast({{update, Urls, Token}, ReplyTo}, #state{data = notfound} = S) ->
    handle_cast({{put, urls, Urls, Token}, ReplyTo}, S);

handle_cast({{update, Urls, Token}, ReplyTo}, #state{data = deleted} = S) ->
    handle_cast({{put, urls, Urls, Token}, ReplyTo}, S);

handle_cast({{update, Urls, Token}, ReplyTo}, #state{data = C} = S) ->
    case validate_urls(Urls) of
        true ->
            handle_cast({{put, urls, Urls ++ C#tagcontent.urls, Token}, ReplyTo}, S);
        false ->
            send_replies(ReplyTo, {error, invalid_url_object}),
            {noreply, S, S#state.timeout}
    end;

handle_cast({{put, Field, Value, Token}, ReplyTo}, S) ->
    S1 = check_write_token(Token, ReplyTo, S,
                          fun () ->
                              do_put(Field, Value, ReplyTo, S)
                          end),
    {noreply, S1, S1#state.timeout};

% Special operations for the +deleted metatag

handle_cast(M, #state{url_cache = false, data = notfound} = S) ->
    handle_cast(M, S#state{url_cache = gb_sets:empty()});

handle_cast(M, #state{url_cache = false, data = Data} = S) ->
    handle_cast(M, S#state{url_cache = gb_sets:from_list(Data#tagcontent.urls)});

handle_cast({{insert_deleted, Url}, ReplyTo}, #state{url_cache = Deleted} = S) ->
    DeletedU = gb_sets:add(Url, Deleted),
    handle_cast({{put, urls, gb_sets:to_list(DeletedU), internal}, ReplyTo},
        S#state{url_cache = DeletedU});

handle_cast({get_deleted, ReplyTo}, #state{url_cache = Deleted} = S) ->
    gen_server:reply(ReplyTo, {ok, Deleted}),
    {noreply, S, S#state.timeout};

handle_cast({{remove_deleted, Url}, ReplyTo}, #state{url_cache = Deleted} = S) ->
    DeletedU = gb_sets:delete_any(Url, Deleted),
    handle_cast({{put, urls, gb_sets:to_list(DeletedU), internal}, ReplyTo},
        S#state{url_cache = DeletedU}).

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call(_, _, S) -> {reply, ok, S}.

handle_info(timeout, S) ->
    handle_cast({die, none}, S).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec send_replies(replyto() | [replyto()], _) -> _.
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

-spec do_delayed_update([[binary()]], replyto(), [replyto()], [[binary()]],
                        binary(), #state{}) -> #state{}.
do_delayed_update(Urls, ReplyTo, Waiters, OldUrls, Tag, S) ->
    if OldUrls =:= [] ->
            spawn(fun() ->
                      timer:sleep(?DELAYED_FLUSH_INTERVAL),
                      ddfs:get_tag(ddfs_master, binary_to_list(Tag),
                                   all, internal)
                  end);
       true ->
            ok
    end,
    case validate_urls(Urls) of
        true ->
            S#state{delayed = {[ReplyTo|Waiters],
                               Urls ++ OldUrls}};
        false ->
            gen_server:reply(ReplyTo, {error, invalid_url_object}),
            S
    end.

-spec do_get(attrib(), replyto(), tagcontent(), #state{}) -> #state{}.
do_get(Attrib, ReplyTo, D, S) ->
    R = case Attrib of
            all ->
                make_tagdata(S#state.data);
            urls ->
                list_to_binary(mochijson2:encode(D#tagcontent.urls));
            read_token ->
                token_encode(D#tagcontent.read_token);
            write_token ->
                token_encode(D#tagcontent.write_token);
            {user, A} ->
                case proplists:lookup(A, D#tagcontent.user) of
                    {_, V} -> list_to_binary(mochijson2:encode(V));
                    _ -> {error, unknown_attribute}
                end
        end,
    gen_server:reply(ReplyTo, R),
    S.

-spec do_put(attrib(), binary(), replyto(), #state{}) -> #state{}.
do_put(Field, Value, ReplyTo, S) ->
    case get_new_values(S, Field, Value) of
        {ok, {ReadToken, WriteToken, Urls, User}} ->
            TagName = ddfs_util:pack_objname(S#state.tag, now()),
            TagContent = new_tagcontent(TagName, ReadToken, WriteToken,
                                        Urls, User),
            TagData = make_tagdata(TagContent),
            case put_distribute({TagName, TagData, TagContent}) of
                {ok, DestNodes, DestUrls, Data} ->
                    if S#state.data == deleted ->
                            {ok, _} = remove_from_deleted(S#state.tag);
                       true -> ok
                    end,
                    send_replies(ReplyTo, {ok, DestUrls}),
                    S#state{data = Data, replicas = DestNodes};
                {error, _} = E ->
                    send_replies(ReplyTo, E),
                    S
            end;
        {error, _} = E ->
            send_replies(ReplyTo, E),
            S
    end.

-spec do_delete(replyto(), #state{}) -> #state{}.
do_delete(ReplyTo, S) ->
    {ok, _} = add_to_deleted(S#state.tag),
    gen_server:reply(ReplyTo, ok),
    S.

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
    gen_server:call(ddfs_master, {tag, {remove_deleted,
                                        [<<"tag://", Tag/binary>>]},
                                  <<"+deleted">>},
                    ?NODEOP_TIMEOUT).

add_to_deleted(Tag) ->
    gen_server:call(ddfs_master, {tag, {insert_deleted,
                                        [list_to_binary(["tag://", Tag])]},
                                  <<"+deleted">>},
                    ?NODEOP_TIMEOUT).
