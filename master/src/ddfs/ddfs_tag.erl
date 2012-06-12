
-module(ddfs_tag).
-behaviour(gen_server).

-include("common_types.hrl").
-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_gc.hrl").
-include("ddfs_tag.hrl").
-include("gs_util.hrl").

-export([start/2, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-type replica() :: {erlang:timestamp(), nonempty_string()}.
-type replyto() :: {pid(), reference()}.

-record(state, {tag :: tagname(),
                data :: none |
                        {missing, notfound} |
                        {missing, deleted} |
                        {error, _} |
                        {ok, #tagcontent{}},
                delayed :: false | gb_tree(),
                timeout :: non_neg_integer(),
                replicas :: false | too_many_failed_nodes
                    | [{replica(), node()}],
                url_cache :: false | gb_set()}).
-type state() :: #state{}.

% API messages.
-type get_msg() :: {get, attrib() | all, token()}.
-type put_msg() :: {put, attrib(), string(), token()}.
-type update_msg() :: {update, [url()], token(), proplists:proplist()}.
-type delayed_update_msg() :: {delayed_update, [url()], token(), proplists:proplist()}.
-type delete_attrib_msg()  :: {delete_attrib, attrib(), token()}.
-type delete_msg() :: {delete, token()}.

% Special msg for GC.
-type gc_get_msg() :: gc_get.

% Special messages for the +deleted tag.
-type has_tagname_msg() :: {has_tagname, tagname()}.
-type get_tagnames_msg() :: get_tagnames.
-type delete_tagname_msg() :: {delete_tagname, tagname()}.

% Notifications.
-type notify_msg() :: {notify, term()}.

-type call_msg() :: get_msg() | put_msg() | update_msg() | delayed_update_msg()
                  | delete_attrib_msg() | delete_msg()
                  | has_tagname_msg() | get_tagnames_msg() | delete_tagname_msg()
                  | gc_get_msg().
-type cast_msg() :: notify_msg().

-export_type([call_msg/0, cast_msg/0]).

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

-spec start(tagname(), boolean()) -> ignore | {error,_} | {ok,pid()}.
start(TagName, NotFound) ->
    gen_server:start(?MODULE, {TagName, NotFound}, []).

-spec init({tagname(), boolean()}) -> gs_init().
init({TagName, true}) ->
    init(TagName, {missing, notfound}, ?TAG_EXPIRES_ONERROR);
init({TagName, false}) ->
    init(TagName, none, ?TAG_EXPIRES).

-spec init(tagname(), none | {missing, notfound}, non_neg_integer()) -> gs_init().
init(TagName, Data, Timeout) ->
    put(min_tagk, list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS"))),
    put(tagk, list_to_integer(disco:get_setting("DDFS_TAG_REPLICAS"))),
    put(blobk, list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS"))),
    {ok, #state{tag = TagName,
                data = Data,
                delayed = false,
                replicas = false,
                url_cache = false,
                timeout = Timeout}}.

-type msg() :: call_msg()
               %% internal messages:
             | gc_get0.

-type notify() :: notify_msg()
                  %% internal messages:
                | {notify0, term()}.

%%% Note to the reader!
%%%
%%% handle_casts below form a state machine. The order in which the functions
%%% are specified is very significant. The easiest way to understand the state
%%% machine is to follow the logic from the top to the bottom.
%%%

-spec handle_cast({msg(), from()} | {die, none} | notify(), state())
                 -> gs_noreply() | gs_stop(normal).
% We don't want to cache requests made by garbage collector
handle_cast({gc_get, ReplyTo}, #state{data = none} = S) ->
    handle_cast({gc_get0, ReplyTo}, S#state{timeout = 100});
handle_cast({notify, M}, #state{data = none} = S) ->
    handle_cast({notify0, M}, S#state{timeout = 100});

% On the other hand, if tag is already cached, GC'ing should
% not trash it
handle_cast({gc_get, ReplyTo}, S) ->
    handle_cast({gc_get0, ReplyTo}, S);
handle_cast({notify, M}, S) ->
    handle_cast({notify0, M}, S);

% First request for this tag: No tag data loaded - load it
handle_cast(M, #state{data = none, tag = Tag, timeout = TO} = S) ->
    {Data, Replicas, Timeout} =
        case is_tag_deleted(Tag) of
            true ->
                {{missing, deleted}, false, ?TAG_EXPIRES_ONERROR};
            false ->
                case get_tagdata(Tag) of
                    {ok, TagData, Repl} ->
                        case ddfs_tag_util:decode_tagcontent(TagData) of
                            {ok, Content} ->
                                {{ok, Content}, Repl, TO};
                            {error, _} = E ->
                                {E, false, ?TAG_EXPIRES_ONERROR}
                        end;
                    {missing, notfound} = E ->
                        {E, false, ?TAG_EXPIRES_ONERROR};
                    {error, _} = E ->
                        {E, false, ?TAG_EXPIRES_ONERROR}
                end;
            {error, _} = E ->
                {E, false, ?TAG_EXPIRES_ONERROR}
        end,
    handle_cast(M, S#state{data = Data,
                           replicas = Replicas,
                           timeout = lists:min([Timeout, TO])});

% Delayed update with an empty buffer, initialize the buffer and a flush process
handle_cast({{delayed_update, _, _, _}, _} = M,
            #state{data = {ok, _}, delayed = false, tag = Tag} = S) ->
    spawn(fun() ->
        timer:sleep(?DELAYED_FLUSH_INTERVAL),
        ddfs:get_tag(ddfs_master, binary_to_list(Tag), all, internal)
    end),
    handle_cast(M, S#state{delayed = gb_trees:empty()});

% Normal delayed update, add request to the buffer, reply later
handle_cast({{delayed_update, Urls, Token, Opt}, ReplyTo},
            #state{data = {ok, _}, delayed = Buffer} = S) ->
    Do = fun(_TokenInfo) ->
             do_delayed_update(Urls, Opt, ReplyTo, Buffer, S)
         end,
    S1 = authorize(write, Token, ReplyTo, S, Do),
    {noreply, S1, S1#state.timeout};

% Delayed update but the tag doesn't exist yet, fall back to normal update
handle_cast({{delayed_update, Urls, Token, Opt}, ReplyTo}, S) ->
    handle_cast({{update, Urls, Token, Opt}, ReplyTo}, S);

% Before handling any other requests, flush pending delayed updates
handle_cast(M, #state{delayed = Buffer} = S0) when Buffer =/= false ->
    Fun =
        fun({Opt, {Waiters, Urls}}, S) ->
            % Note that we can supply bogus TokenInfo to do_update here,
            % as it is only used when creating a new tag. We can
            % guarantee that the tag exists at this point.
            do_update({write, null}, Urls, Opt, Waiters, S)
        end,
    NewState =
        lists:foldl(Fun, S0#state{delayed = false}, gb_trees:to_list(Buffer)),
    handle_cast(M, NewState);

handle_cast({{delete, Token}, ReplyTo}, S) ->
    Do = fun(_TokenInfo) -> do_delete(ReplyTo, S) end,
    S1 = authorize(write, Token, ReplyTo, S, Do),
    {noreply, S1, S1#state.timeout};

handle_cast({die, _}, S) ->
    {stop, normal, S};

handle_cast({{get, Attrib, Token}, ReplyTo}, #state{data = {ok, D}} = S) ->
    Do = fun(TokenInfo) ->
            gen_server:reply(ReplyTo, do_get(TokenInfo, Attrib, D)),
            S
         end,
    S1 = authorize(read, Token, ReplyTo, S, Do),
    {noreply, S1, S1#state.timeout};

handle_cast({{get, _, _}, ReplyTo}, #state{data = Data, timeout = TO} = S) ->
    gen_server:reply(ReplyTo, Data),
    {noreply, S, TO};

handle_cast({_, ReplyTo}, #state{data = {error, _} = Data, timeout = TO} = S) ->
    gen_server:reply(ReplyTo, Data),
    {noreply, S, TO};

handle_cast({gc_get0, ReplyTo},
            #state{data = {ok, D}, replicas = Replicas, timeout = TO} = S) ->
    R = {D#tagcontent.id, D#tagcontent.urls, Replicas},
    gen_server:reply(ReplyTo, R),
    {noreply, S, TO};

handle_cast({gc_get0, ReplyTo}, #state{data = Data,
                                       replicas = Replicas,
                                       timeout = TO} = S) ->
    gen_server:reply(ReplyTo, {Data, Replicas}),
    {noreply, S, TO};

handle_cast({{update, Urls, Token, Opt}, ReplyTo}, S) ->
    Do = fun(TokenInfo) -> do_update(TokenInfo, Urls, Opt, ReplyTo, S) end,
    S1 = authorize(write, Token, ReplyTo, S, Do),
    {noreply, S1, S1#state.timeout};

handle_cast({{put, Field, Value, Token}, ReplyTo}, #state{timeout = TO} = S) ->
    case ddfs_tag_util:validate_value(Field, Value) of
        true ->
            Do =
                fun (TokenInfo) ->
                    S1 = do_put(TokenInfo, Field, Value, ReplyTo, S),
                    S1#state{url_cache = false}
                end,
            S1 = authorize(write, Token, ReplyTo, S, Do),
            {noreply, S1, S1#state.timeout};
        false ->
            _ = send_replies(ReplyTo, {error, invalid_attribute_value}),
            {noreply, S, TO}
    end;

handle_cast({{delete_attrib, Field, Token}, ReplyTo},
             #state{data = {ok, _}} = S) ->
    Do = fun(_TokenInfo) -> do_delete_attrib(Field, ReplyTo, S) end,
    S1 = authorize(write, Token, ReplyTo, S, Do),
    {noreply, S1, S1#state.timeout};

handle_cast({{delete_attrib, _Field, _Token}, ReplyTo},
            #state{timeout = TO} = S) ->
    _ = send_replies(ReplyTo, {error, unknown_attribute}),
    {noreply, S, TO};

handle_cast({notify0, {gc_rr_update, Updates, Blacklist, UpdateId}}, S) ->
    S1 = do_gc_rr_update(S, Updates, Blacklist, UpdateId),
    {noreply, S1, S1#state.timeout};

% Special operations for the +deleted metatag

handle_cast(M, #state{url_cache = false, data = {missing, notfound}} = S) ->
    handle_cast(M, S#state{url_cache = gb_sets:empty()});

handle_cast(M, #state{url_cache = false, data = {ok, Data}} = S) ->
    Urls = Data#tagcontent.urls,
    handle_cast(M, S#state{url_cache = init_url_cache(Urls)});

handle_cast({{has_tagname, Name}, ReplyTo},
            #state{url_cache = Cache, timeout = TO} = S) ->
    gen_server:reply(ReplyTo, gb_sets:is_member(Name, Cache)),
    {noreply, S, TO};

handle_cast({get_tagnames, ReplyTo},
            #state{url_cache = Cache, timeout = TO} = S) ->
    gen_server:reply(ReplyTo, {ok, Cache}),
    {noreply, S, TO};

handle_cast({{delete_tagname, Name}, ReplyTo}, #state{url_cache = Cache} = S) ->
    NewDel = gb_sets:delete_any(Name, Cache),
    NewUrls = [[<<"tag://", Tag/binary>>] || Tag <- gb_sets:to_list(NewDel)],
    S1 = do_put({write, null},
                urls,
                NewUrls,
                ReplyTo,
                S#state{url_cache = NewDel}),
    {noreply, S1, S1#state.timeout}.

-spec handle_call(term(), from(), state()) -> gs_reply(ok | state()).
handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call(_, _, S) -> {reply, ok, S}.

-spec handle_info(timeout, state()) -> gs_noreply_t() | gs_stop(normal);
                 ({reference(), term()}, state()) -> gs_noreply().
handle_info(timeout, S) ->
    handle_cast({die, none}, S);
% handle late replies to "catch gen_server:call"
handle_info({Ref, _Msg}, S) when is_reference(Ref) ->
    {noreply, S}.

% callback stubs
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


-spec authorize(tokentype(), binary(), replyto(), state(),
                fun((tokentype()) -> state())) -> state().
authorize(TokenType,
          Token,
          ReplyTo,
          #state{data = {ok, #tagcontent{read_token = ReadToken,
                                         write_token = WriteToken}}} = S,
          Op) ->
    authorize(TokenType, Token, ReplyTo, ReadToken, WriteToken, S, Op);

authorize(TokenType, Token, ReplyTo, State, Op) ->
    authorize(TokenType, Token, ReplyTo, null, null, State, Op).

authorize(TokenType, Token, ReplyTo, ReadToken, WriteToken, S, Op) ->
    case ddfs_tag_util:check_token(TokenType, Token, ReadToken, WriteToken) of
        false ->
            _ = send_replies(ReplyTo, {error, unauthorized}),
            S;
        TokenPrivilege ->
            Op({TokenPrivilege, Token})
    end.

do_update(TokenInfo, Urls, Opt, ReplyTo, #state{data = {missing, _}} = S) ->
    do_update(TokenInfo, Urls, Opt, ReplyTo, [], S);

do_update(TokenInfo, Urls, Opt, ReplyTo, #state{data = {ok, D}} = S) ->
    OldUrls = D#tagcontent.urls,
    do_update(TokenInfo, Urls, Opt, ReplyTo, OldUrls, S).

do_update(TokenInfo, Urls, Opt, ReplyTo, OldUrls,
          #state{url_cache = OldCache} = S) ->
    case ddfs_tag_util:validate_urls(Urls) of
        true ->
            NoDup = proplists:is_defined(nodup, Opt),
            {Cache, Merged} = merge_urls(Urls,
                                         OldUrls,
                                         NoDup,
                                         OldCache),
            do_put(TokenInfo,
                   urls,
                   Merged,
                   ReplyTo,
                   S#state{url_cache = Cache});
        false ->
            _ = send_replies(ReplyTo, {error, invalid_url_object}),
            S
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
    Name = ddfs_util:url_to_name(Url),
    case {Name, gb_sets:is_member(Name, Seen)} of
        {false, _} ->
            find_unseen(Rest, Seen, [Repl|Urls]);
        {_, false} ->
            find_unseen(Rest, gb_sets:add(Name, Seen), [Repl|Urls]);
        {_, true} ->
            find_unseen(Rest, Seen, Urls)
    end.

init_url_cache(Urls) ->
    gb_sets:from_list([ddfs_util:url_to_name(Url) || [Url|_] <- Urls]).

-spec send_replies(replyto() | [replyto()],
                   {error, commit_failed | invalid_attribute_value |
                           invalid_url_object | unknown_attribute |
                           replication_failed | unauthorized} |
                   {ok, [binary(),...]} | ok) -> [any()].
send_replies(ReplyTo, Message) when is_tuple(ReplyTo) ->
    send_replies([ReplyTo], Message);
send_replies(ReplyToList, Message) ->
    [gen_server:reply(Re, Message) || Re <- ReplyToList].

-spec do_gc_rr_update(state(), [blob_update()], [node()], tagid()) -> state().
do_gc_rr_update(#state{data = {missing, _}} = S, _Updates, _Blacklist, _Id) ->
    % The tag has been deleted, or cannot be found; ignore the update.
    S;
do_gc_rr_update(#state{data = {error, _}} = S, _Updates, _Blacklist, _Id) ->
    % Error retrieving tag data; ignore the update.
    S;
do_gc_rr_update(#state{tag = TagName, data = {ok, D} = Tag} = S,
                Updates, Blacklist, UpdateId) ->
    OldUrls = D#tagcontent.urls,
    Id = D#tagcontent.id,
    Map = gb_trees:from_orddict(lists:sort(Updates)),
    NewUrls = gc_update_urls(Id, OldUrls, Map, Blacklist, UpdateId),
    {ok, NewTagContent} =
        ddfs_tag_util:update_tagcontent(TagName, urls, NewUrls, Tag, null),
    NewTagData = ddfs_tag_util:encode_tagcontent(NewTagContent),
    TagId = NewTagContent#tagcontent.id,
    case put_distribute({TagId, NewTagData}) of
        {ok, DestNodes, _} ->
            S#state{data = {ok, NewTagContent},
                    replicas = DestNodes,
                    url_cache = init_url_cache(NewUrls)};
        {error, _} = E ->
            lager:error("GC: unable to update tag ~p: ~p", [TagName, E]),
            S
    end.

-spec gc_update_urls(tagid(), [[url()]], gb_tree(), [node()], tagid())
                    -> [[url()]].
gc_update_urls(Id, OldUrls, Map, Blacklist, UpdateId) ->
    gc_update_urls(Id, OldUrls, Map, Blacklist, UpdateId, []).
gc_update_urls(_Id, [], _Map, _Blacklist, _UpdateId, Acc) ->
    lists:reverse(Acc);
gc_update_urls(Id, [[] | Rest], Map, Blacklist, UpdateId, Acc) ->
    gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, Acc);
gc_update_urls(Id, [[Url|_] = BlobSet | Rest], Map, Blacklist, UpdateId, Acc) ->
    case ddfs_util:parse_url(Url) of
        not_ddfs ->
            gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, [BlobSet|Acc]);
        {_, _, _, _, BlobName} ->
            case gb_trees:lookup(BlobName, Map) of
                none ->
                    gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, [BlobSet|Acc]);
                {value, filter}
                  when Id =:= UpdateId ->
                    % Only apply the blacklist filter provided we
                    % still retain at least the minimum number of
                    % replicas.  This is just a sanity check; the real
                    % safety check is done by GC/RR.
                    Filtered = filter_blacklist(BlobSet, Blacklist),
                    case length(Filtered) >= get(blobk) of
                        true ->
                            gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, [Filtered|Acc]);
                        false ->
                            gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, [BlobSet|Acc])
                    end;
                {value, filter} ->
                    % The tag was modified after the filter safety
                    % check was done; skip the update.
                    gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, [BlobSet|Acc]);
                {value, NewUrls_} ->
                    % Ensure that new locations use the same scheme as
                    % the existing locations.
                    NewUrls = rewrite_scheme(Url, NewUrls_),
                    NewBlobSet = lists:usort(NewUrls ++ BlobSet),
                    gc_update_urls(Id, Rest, Map, Blacklist, UpdateId, [NewBlobSet|Acc])
            end
    end.

-spec rewrite_scheme(url(), [url()]) -> [url()].
rewrite_scheme(OrigUrl, Urls) ->
    {S, _, _, _, _} = mochiweb_util:urlsplit(binary_to_list(OrigUrl)),
    [rewrite_url(U, S) || U <- Urls].

rewrite_url(Url, S) ->
    U1 = binary_to_list(Url),
    {_, H, P, Q, F} = mochiweb_util:urlsplit(U1),
    NewUrl = mochiweb_util:urlunsplit({S, H, P, Q, F}),
    list_to_binary(NewUrl).

-spec filter_blacklist([url()], [node()]) -> [url()].
filter_blacklist(BlobSet, Blacklist) ->
    lists:foldl(
      fun(Url, Acc) ->
              case ddfs_util:parse_url(Url) of
                  not_ddfs -> [Url | Acc];
                  {Host, _V, _T, _H, _B} ->
                      case disco:slave_safe(Host) of
                          false -> [Url | Acc];
                          Node -> case lists:member(Node, Blacklist) of
                                      true -> Acc;
                                      false -> [Url|Acc]
                                  end
                      end
              end
      end, [], BlobSet).

-spec get_tagdata(tagname()) -> {missing, notfound} | {error, _}
                             | {ok, binary(), [{replica(), node()}]}.
get_tagdata(TagName) ->
    {ok, ReadableNodes, RBSize} = ddfs_master:get_read_nodes(),
    TagMinK = get(min_tagk),
    case RBSize >= TagMinK of
        true ->
            {error, too_many_failed_nodes};
        false ->
            {Replies, Failed} =
                gen_server:multi_call(ReadableNodes,
                                      ddfs_node,
                                      {get_tag_timestamp, TagName},
                                      ?NODE_TIMEOUT),
            case [{TagNfo, Node} || {Node, {ok, TagNfo}} <- Replies] of
                _ when length(Failed) + RBSize >= TagMinK ->
                    {error, too_many_failed_nodes};
                [] ->
                    {missing, notfound};
                L ->
                    {{Time, _Vol}, _Node} = lists:max(L),
                    Replicas = [X || {{T, _}, _} = X <- L, T == Time],
                    TagID = ddfs_util:pack_objname(TagName, Time),
                    read_tagdata(TagID, Replicas, [], tagdata_failed)
            end
    end.

read_tagdata(_TagID, Replicas, Failed, Error)
  when length(Replicas) =:= length(Failed) ->
    {error, Error};

read_tagdata(TagID, Replicas, Failed, _Error) ->
    {TagNfo, SrcNode} = Chosen = disco_util:choose_random(Replicas -- Failed),
    GetData = try ddfs_node:get_tag_data(SrcNode, TagID, TagNfo)
              catch K:V -> {error, {K,V}}
              end,
    case GetData of
        {ok, Data} ->
            {_, DestNodes} = lists:unzip(Replicas),
            {ok, Data, DestNodes};
        E ->
            read_tagdata(TagID, Replicas, [Chosen|Failed], E)
    end.

-spec do_delayed_update([[binary()]], [term()], replyto(),
                        gb_tree(), state()) -> state().
do_delayed_update(Urls, Opt, ReplyTo, Buffer, S) ->
    % We must handle updates with different set of options separately.
    % Thus requests are indexed by the normalized set of options (OptKey)
    OptKey = lists:sort(proplists:compact(Opt)),
    case ddfs_tag_util:validate_urls(Urls) of
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
            S#state{delayed = NewBuffer};
        false ->
            gen_server:reply(ReplyTo, {error, invalid_url_object}),
            S
    end.

-spec jsonbin(_) -> binary().
jsonbin(X) ->
    iolist_to_binary(mochijson2:encode(X)).

-spec do_get({tokentype(), token()}, attrib() | all, tagcontent()) ->
             {ok, binary()} | {error, unauthorized | unknown_attribute}.
do_get(_TokenInfo, all, D) ->
    {ok, ddfs_tag_util:encode_tagcontent_secure(D)};

do_get(_TokenInfo, urls, D) ->
    {ok, jsonbin(D#tagcontent.urls)};

do_get(_TokenInfo, read_token, D) ->
    {ok, jsonbin(D#tagcontent.read_token)};

do_get({read, _}, write_token, _D) ->
    {error, unauthorized};

do_get({write, _}, write_token, D) ->
    {ok, jsonbin(D#tagcontent.write_token)};

do_get(_TokenInfo, {user, A}, D) ->
    case proplists:lookup(A, D#tagcontent.user) of
        {_, V} -> {ok, jsonbin(V)};
        _ -> {error, unknown_attribute}
    end.

-spec do_put({tokentype(), token()}, attrib(), string(), replyto(), state())
            -> state().
do_put({_, Token},
       Field,
       Value,
       ReplyTo,
       #state{tag = TagName, data = TagData} = S) ->
    case ddfs_tag_util:update_tagcontent(TagName, Field, Value, TagData, Token) of
        {ok, TagContent} ->
            NewTagData = ddfs_tag_util:encode_tagcontent(TagContent),
            TagID = TagContent#tagcontent.id,
            case put_distribute({TagID, NewTagData}) of
                {ok, DestNodes, DestUrls} ->
                    if TagData =:= {missing, deleted} ->
                            {ok, _} = remove_from_deleted(TagName),
                            ok;
                       true -> ok
                    end,
                    _ = send_replies(ReplyTo, {ok, DestUrls}),
                    S#state{data = {ok, TagContent}, replicas = DestNodes};
                {error, _} = E ->
                    _ = send_replies(ReplyTo, E),
                    S#state{url_cache = false}
            end;
        {error, _} = E ->
            _ = send_replies(ReplyTo, E),
            S#state{url_cache = false}
    end.

do_delete_attrib(Field, ReplyTo, #state{tag = TagName, data = {ok, D}} = S) ->
    TagContent = ddfs_tag_util:delete_tagattrib(TagName, Field, D),
    NewTagData = ddfs_tag_util:encode_tagcontent(TagContent),
    TagId = TagContent#tagcontent.id,
    case put_distribute({TagId, NewTagData}) of
        {ok, DestNodes, _DestUrls} ->
            _ = send_replies(ReplyTo, ok),
            S#state{data = {ok, TagContent},
                    replicas = DestNodes};
        {error, _} = E ->
            _ = send_replies(ReplyTo, E),
            S
    end.

% Put transaction:
% 1. choose nodes
% 2. multicall nodes, send TagData -> receive temporary file names
% 3. if failures -> retry with other nodes
% 4. if all nodes fail before tagk replicas are written, fail
% 5. multicall with temp file names, rename
% 6. if all fail, fail
% 7. if at least one multicall succeeds, return updated tagdata, desturls

-spec put_distribute({tagid(),binary()}) ->
    {error, commit_failed | replication_failed} | {ok, [node()], [binary(),...]}.
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
    {ok, Nodes} = ddfs_master:choose_write_nodes(K0, Exclude),
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
    {error, commit_failed} | {ok, [node()], [binary(), ...]}.
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

-spec do_delete(replyto(), state()) -> state().
do_delete(ReplyTo, #state{tag = Tag} = S) ->
    case add_to_deleted(Tag) of
        {ok, _} ->
            gen_server:reply(ReplyTo, ok),
            gen_server:cast(self(), {die, none});
        E ->
            gen_server:reply(ReplyTo, E)
    end,
    S.

-spec is_tag_deleted(tagname()) -> _.
is_tag_deleted(<<"+deleted">>) -> false;
is_tag_deleted(Tag) ->
    deleted_op({has_tagname, Tag}, ?NODEOP_TIMEOUT).

-spec add_to_deleted(tagname()) -> _.
add_to_deleted(Tag) ->
    Urls = [[<<"tag://", Tag/binary>>]],
    deleted_op({update, Urls, internal, [nodup]}, ?TAG_UPDATE_TIMEOUT).

-spec remove_from_deleted(tagname()) -> _.
remove_from_deleted(Tag) ->
    deleted_op({delete_tagname, Tag}, ?NODEOP_TIMEOUT).

deleted_op(Op, Timeout) ->
    ddfs_master:tag_operation(Op, <<"+deleted">>, Timeout).
