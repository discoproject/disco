
-module(ddfs_tag).
-behaviour(gen_server).

-include("config.hrl").
-include("ddfs_tag.hrl").

-export([start/2, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-type replica() :: {timer:timestamp(), nonempty_string()}.
-type replyto() :: {pid(), reference()}.

-record(state, {tag :: tagname(),
                data :: 'false'  | 'notfound' | 'deleted' | {'error', _}
                    | #tagcontent{},
                delayed :: {[replyto()], [[binary()]]},
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

-spec start(tagname(), 'notfound' | 'false') ->
            'ignore' | {'error',_} | {'ok',pid()}.
start(TagName, Status) ->
    gen_server:start(ddfs_tag, {TagName, Status}, []).

-spec init({tagname(), 'notfound' | 'false'}) -> {'ok', #state{}}.
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
                        case ddfs_tag_util:decode_tagcontent(TagData) of
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
    Do = fun(_TokenType) ->
            do_delayed_update(Urls, ReplyTo, Waiters, OldUrls, Tag, S)
         end,
    S1 = authorize(write, Token, ReplyTo, S, Do),
    {noreply, S1, S1#state.timeout};

% Before handling any other requests, flush pending delayed updates
handle_cast(M, #state{delayed = {Waiters, Urls}} = S0) when Urls =/= [] ->
    {noreply, S, _} = handle_cast({{update, Urls, internal}, Waiters},
        S0#state{delayed = {[], []}}),
    handle_cast(M, S);

handle_cast({{delete, Token}, ReplyTo}, S) ->
    Do = fun(_TokenType) -> do_delete(ReplyTo, S) end,
    S1 = authorize(write, Token, ReplyTo, S, Do),
    gen_server:reply(ReplyTo, ok),
    handle_cast({die, none}, S1);

handle_cast({die, _}, S) ->
    {stop, normal, S};

handle_cast({{get, Attrib, Token}, ReplyTo},
            #state{data = #tagcontent{} = D} = S) ->
    Do = fun(TokenType) ->
            gen_server:reply(ReplyTo, do_get(TokenType, Attrib, D)),
            S
         end,
    S1 = authorize(read, Token, ReplyTo, S, Do),
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
    case ddfs_tag_util:validate_urls(Urls) of
        true ->
            NewUrls = Urls ++ C#tagcontent.urls,
            handle_cast({{put, urls, NewUrls, Token}, ReplyTo}, S);
        false ->
            send_replies(ReplyTo, {error, invalid_url_object}),
            {noreply, S, S#state.timeout}
    end;

handle_cast({{put, Field, Value, Token}, ReplyTo}, S) ->
    case ddfs_tag_util:validate_value(Field, Value) of
        true ->
            Do = fun (_TokenType) -> do_put(Field, Value, ReplyTo, S) end,
            S1 = authorize(write, Token, ReplyTo, S, Do),
            {noreply, S1, S1#state.timeout};
        false ->
            send_replies(ReplyTo, {error, invalid_attribute_value}),
            {noreply, S, S#state.timeout}
    end;

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

-spec authorize(tokentype(), binary(), replyto(), #state{},
                fun((tokentype()) -> #state{})) -> #state{}.
authorize(TokenType,
          Token,
          ReplyTo,
          #state{data = #tagcontent{read_token = ReadToken,
                                    write_token = WriteToken} = S},
          Op) ->
    authorize(TokenType, Token, ReplyTo, ReadToken, WriteToken, S, Op);

authorize(TokenType, Token, ReplyTo, State, Op) ->
    authorize(TokenType, Token, ReplyTo, null, null, State, Op).

authorize(TokenType, Token, ReplyTo, ReadToken, WriteToken, S, Op) ->
    case ddfs_tag_util:check_token(TokenType, Token, ReadToken, WriteToken) of
        false ->
            send_replies(ReplyTo, {error, unauthorized}),
            S;
        TokenPrivilege ->
            Op(TokenPrivilege)
    end.

-spec send_replies(replyto() | [replyto()],
                   {'error', 'commit_failed' |
                             'invalid_url_object' |
                             'replication_failed' |
                             'unauthorized'} | 
                   {'ok', [binary(),...]}) -> [any()].
send_replies(ReplyTo, Message) when is_tuple(ReplyTo) ->
    send_replies([ReplyTo], Message);
send_replies(ReplyToList, Message) ->
    [gen_server:reply(Re, Message) || Re <- ReplyToList].

-spec get_tagdata(tagname()) -> 'notfound' | {'error', _}
                             | {'ok', binary(), [{replica(), node()}]}.
get_tagdata(TagName) ->
    {ok, Nodes} = gen_server:call(ddfs_master, get_nodes),
    {Replies, Failed} = gen_server:multi_call(Nodes, ddfs_node,
        {get_tag_timestamp, TagName}, ?NODE_TIMEOUT),
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
            TagID = ddfs_util:pack_objname(TagName, Time),
            case catch gen_server:call({ddfs_node, SrcNode},
                    {get_tag_data, TagID, TagNfo}, ?NODE_TIMEOUT) of
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
                                   read_token, internal)
                  end);
       true ->
            ok
    end,
    case ddfs_tag_util:validate_urls(Urls) of
        true ->
            S#state{delayed = {[ReplyTo|Waiters],
                               Urls ++ OldUrls}};
        false ->
            gen_server:reply(ReplyTo, {error, invalid_url_object}),
            S
    end.

jsonbin(X) ->
    iolist_to_binary(mochijson2:encode(X)).

-spec do_get(tokentype(), attrib() | all, tagcontent()) ->
             binary() | {'error','unauthorized' | 'unknown_attribute'}.
do_get(TokenType, all, D) ->
    ddfs_tag_util:encode_tagcontent_secure(D, TokenType);

do_get(_TokenType, urls, D) ->
    jsonbin(D#tagcontent.urls);

do_get(_TokenType, read_token, D) ->
    jsonbin(D#tagcontent.read_token);

do_get(read, write_token, _D) ->
    {error, unauthorized};

do_get(write, write_token, D) ->
    jsonbin(D#tagcontent.write_token);

do_get(_TokenType, {user, A}, D) ->
    case proplists:lookup(A, D#tagcontent.user) of
        {_, V} -> jsonbin(V);
        _ -> {error, unknown_attribute}
    end.

% Put transaction:
% 1. choose nodes
% 2. multicall nodes, send TagData -> receive temporary file names
% 3. if failures -> retry with other nodes
% 4. if all nodes fail before tagk replicas are written, fail
% 5. multicall with temp file names, rename
% 6. if all fail, fail
% 7. if at least one multicall succeeds, return updated tagdata, desturls

-spec do_put(attrib(), string(), replyto(), #state{}) -> #state{}.
do_put(Field, Value, ReplyTo, #state{tag = TagName, data = TagData} = S) ->
    case ddfs_tag_util:update_tagcontent(TagName, Field, Value, TagData) of
        {ok, TagContent} ->
            NewTagData = ddfs_tag_util:encode_tagcontent(TagContent),
            TagID = TagContent#tagcontent.id,
            case put_distribute({TagID, NewTagData}) of
                {ok, DestNodes, DestUrls} ->
                    if TagData =:= deleted ->
                            {ok, _} = remove_from_deleted(TagName);
                       true -> ok
                    end,
                    send_replies(ReplyTo, {ok, DestUrls}),
                    S#state{data = TagContent, replicas = DestNodes};
                {error, _} = E ->
                    send_replies(ReplyTo, E),
                    S
            end;
        {error, _} = E ->
            send_replies(ReplyTo, E),
            S
    end.

-spec put_distribute({tagid(),binary()}) ->
    {'error','commit_failed' | 'replication_failed'} | {'ok',[node()],[binary()]}.
put_distribute({TagID, _} = Msg) ->
    case put_distribute(Msg, get(tagk), [], []) of
        {ok, TagVol} ->
            put_commit(TagID, TagVol);
        {error, _} = E ->
            E
    end.

-spec put_distribute({tagid(), binary()}, non_neg_integer(),
    [{node(), binary()}], [node()]) ->
        {error, replication_failed} | {ok, [{node(), binary()}]}.
put_distribute(_, K, OkNodes, _Exclude) when K == length(OkNodes) ->
    {ok, OkNodes};

put_distribute({TagID, TagData} = Msg, K, OkNodes, Exclude) ->
    TagMinK = get(min_tagk),
    K0 = K - length(OkNodes),
    {ok, Nodes} = gen_server:call(ddfs_master, {choose_nodes, K0, Exclude}),
    if
        Nodes =:= [], length(OkNodes) < TagMinK ->
            {error, replication_failed};
        Nodes =:= [] ->
            {ok, OkNodes};
        true ->
            PutMsg = {TagID, TagData},
            {Replies, Failed} = gen_server:multi_call(Nodes,
                                                      ddfs_node,
                                                      {put_tag_data, PutMsg},
                                                      ?NODE_TIMEOUT),
            put_distribute(Msg, K,
                           OkNodes ++ [{Node, VolName}
                                       || {Node, {ok, VolName}} <- Replies],
                           Exclude ++ [Node || {Node, _} <- Replies] ++ Failed)
    end.

-spec put_commit(tagid(), [{node(), binary()}]) ->
    {'error', 'commit_failed'} | {'ok', [node()], [binary(), ...]}.
put_commit(TagID, TagVol) ->
    {Nodes, _} = lists:unzip(TagVol),
    {NodeUrls, _} = gen_server:multi_call(Nodes,
                                          ddfs_node,
                                          {put_tag_commit, TagID, TagVol},
                                          ?NODE_TIMEOUT),
    case [Url || {_Node, {ok, Url}} <- NodeUrls] of
        [] ->
            {error, commit_failed};
        Urls ->
            {ok, Nodes, Urls}
    end.

-spec do_delete(replyto(), #state{}) -> #state{}.
do_delete(ReplyTo, S) ->
    {ok, _} = add_to_deleted(S#state.tag),
    gen_server:reply(ReplyTo, ok),
    S.

-spec is_tag_deleted(tagname()) -> _.
is_tag_deleted(<<"+deleted">>) -> false;
is_tag_deleted(Tag) ->
    case gen_server:call(ddfs_master,
            {tag, get_deleted, <<"+deleted">>}, ?NODEOP_TIMEOUT) of
        {ok, Deleted} ->
            gb_sets:is_member([<<"tag://", Tag/binary>>], Deleted);
        E -> E
    end.

-spec remove_from_deleted(tagname()) -> _.
remove_from_deleted(Tag) ->
    gen_server:call(ddfs_master, {tag, {remove_deleted,
                                        [<<"tag://", Tag/binary>>]},
                                  <<"+deleted">>},
                    ?NODEOP_TIMEOUT).

-spec add_to_deleted(tagname()) -> _.
add_to_deleted(Tag) ->
    gen_server:call(ddfs_master, {tag, {insert_deleted,
                                        [list_to_binary(["tag://", Tag])]},
                                  <<"+deleted">>},
                    ?NODEOP_TIMEOUT).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

