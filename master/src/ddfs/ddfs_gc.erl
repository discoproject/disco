-module(ddfs_gc).
-export([start_gc/1, gc_request/1, hosted_tags/1]).

% GC internal api.
-export([abort/2]).

-include("common_types.hrl").
-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_tag.hrl").
-include("ddfs_gc.hrl").

-define(CALL_TIMEOUT, 5 * ?SECOND).

-type status() :: init_wait | not_running | phase().

-spec gc_request(status) -> {ok, status()} | {error, term()};
                (start)  -> ok | {ok, init_wait} | {error, term()}.
gc_request(Request) ->
    ?MODULE ! {self(), Request},
    receive
        R -> R
    after ?CALL_TIMEOUT ->
            {error, timeout}
    end.


-spec abort(term(), atom()) -> no_return().
abort(Msg, Code) ->
    lager:warning("GC: aborted with ~p", [Msg]),
    exit(Code).


-spec start_gc(string()) -> no_return().
start_gc(Root) ->
    true = register(?MODULE, self()),
    % Wait some time for all nodes to start and stabilize.
    InitialWait =
        case disco:has_setting("DDFS_GC_INITIAL_WAIT") of
            true -> list_to_integer(disco:get_setting("DDFS_GC_INITIAL_WAIT")) * ?MINUTE;
            false -> ?GC_DEFAULT_INITIAL_WAIT
        end,
    initial_wait(InitialWait),
    process_flag(trap_exit, true),
    GCMaxDuration = lists:min([?GC_MAX_DURATION,
                               ?ORPHANED_BLOB_EXPIRES,
                               ?ORPHANED_TAG_EXPIRES]),
    start_gc(Root, ets:new(deleted_ages, [set, public]), GCMaxDuration).

-spec initial_wait(timeout()) -> ok.
initial_wait(InitialWait) ->
    Start = now(),
    receive
        {From, _Req} ->
            From ! {ok, init_wait},
            Wait = timer:now_diff(now(), Start) div 1000,
            initial_wait(InitialWait - Wait);
        Other ->
            lager:error("GC: got unexpected msg ~p", [Other]),
            Wait = timer:now_diff(now(), Start) div 1000,
            initial_wait(InitialWait - Wait)
    after InitialWait ->
            ok
    end.

-spec start_gc(string(), ets:tab(), non_neg_integer()) -> no_return().
start_gc(Root, DeletedAges, GCMaxDuration) ->
    Start = now(),
    case ddfs_gc_main:start_link(Root, DeletedAges) of
        {ok, Gc} ->
            start_gc_wait(Gc, GCMaxDuration),
            % timer:now_diff() returns microseconds.
            Wait = timer:now_diff(now(), Start) div 1000,
            % Wait until the next scheduled gc run slot.
            Idle = ?GC_INTERVAL - (Wait rem ?GC_INTERVAL),
            idle(Idle);
        E ->
            lager:error("GC: unable to start due to ~p", [E]),
            idle(?GC_INTERVAL)
    end,
    start_gc(Root, DeletedAges, GCMaxDuration).

-spec idle(timeout()) -> ok.
idle(Timeout) ->
    Start = now(),
    receive
        {From, status} ->
            From ! {ok, not_running},
            Wait = Timeout - timer:now_diff(now(), Start) div 1000,
            idle(Wait);
        {From, start} ->
            From ! ok;
        _Other ->
            Wait = Timeout - timer:now_diff(now(), Start) div 1000,
            idle(Wait)
    after Timeout ->
            ok
    end.

-spec start_gc_wait(pid(), timeout()) -> ok.
start_gc_wait(Pid, Interval) ->
    Start = now(),
    receive
        {'EXIT', Pid, Reason} ->
            lager:error("GC: exited with ~p", [Reason]);
        {'EXIT', Other, Reason} ->
            lager:error("GC: got unexpected exit of ~p: ~p", [Other, Reason]),
            start_gc_wait(Pid, Interval - (timer:now_diff(now(), Start) div 1000));
        {From, status} when is_pid(From) ->
            ddfs_gc_main:gc_status(Pid, From),
            start_gc_wait(Pid, Interval - (timer:now_diff(now(), Start) div 1000));
        {From, start} when is_pid(From) ->
            From ! ok,
            start_gc_wait(Pid, Interval - (timer:now_diff(now(), Start) div 1000));
        Other ->
            lager:error("GC: got unexpected msg ~p", [Other]),
            start_gc_wait(Pid, Interval - (timer:now_diff(now(), Start) div 1000))
    after Interval ->
            lager:error("GC: completion timed out"),
            exit(Pid, force_timeout)
    end.

-spec hosted_tags(host()) -> {ok, [tagname()]} | {error, term()}.
hosted_tags(Host) ->
    case disco:slave_safe(Host) of
        false ->
            {error, unknown_host};
        Node ->
            case hosted_tags(Host, Node) of
                {error, _} = E -> E;
                Tags -> {ok, Tags}
            end
    end.
-spec hosted_tags(host(), node()) -> [tagname()] | {error, term()}.
hosted_tags(Host, Node) ->
    try
        {ok, Tags} = ddfs_master:get_tags(safe),
        lists:foldl(
          fun (_T, {error, _} = E) ->
                  E;
              (T, HostedTags) ->
                  case tag_is_hosted(T, Host, Node, ?MAX_TAG_OP_RETRIES) of
                      true -> [T|HostedTags];
                      false -> HostedTags;
                      E -> E
                  end
          end, [], Tags)
    catch K:V ->
            {error, {K,V}}
    end.

-spec tag_is_hosted(tagname(), host(), node(), non_neg_integer()) ->
                           boolean() | {error, term()}.
tag_is_hosted(T, _Host, _Node, 0) ->
    {error, {get_tag, T}};
tag_is_hosted(T, Host, Node, Retries) ->
    try
        case ddfs_master:tag_operation(gc_get, T, ?GET_TAG_TIMEOUT) of
            {{missing, _}, false} ->
                false;
            {_Id, Urls, TagReplicas} ->
                lists:member(Node, TagReplicas) orelse urls_are_hosted(Urls, Host, Node);
            E ->
                E
        end
    catch _:_ ->
            tag_is_hosted(T, Host, Node, Retries - 1)
    end.

-spec urls_are_hosted([[url()]], host(), node())
                     -> boolean() | {error | term()}.
urls_are_hosted([], _Host, _Node) ->
    false;
urls_are_hosted([[]|Rest], Host, Node) ->
    urls_are_hosted(Rest, Host, Node);
urls_are_hosted([Urls|Rest], Host, Node) ->
    Hosted =
        lists:foldl(
          fun (<<"tag://", T/binary>>, false) ->
                  tag_is_hosted(T, Host, Node, ?MAX_TAG_OP_RETRIES);
              (Url, false) ->
                  case ddfs_util:parse_url(Url) of
                      not_ddfs -> false;
                      {H, _V, _T, _H, _B} -> H =:= Host
                  end;
              (_Url, TrueOrError) ->
                  TrueOrError
          end, false, Urls),
    case Hosted of
        false -> urls_are_hosted(Rest, Host, Node);
        TrueOrError -> TrueOrError
    end.
