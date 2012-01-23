-module(ddfs_gc).
-export([start_gc/1, abort/2, hosted_tags/1]).

-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_gc.hrl").

-spec abort(term(), atom()) -> no_return().
abort(Msg, Code) ->
    error_logger:warning_report({"GC: aborted", Msg}),
    exit(Code).

-spec start_gc(string()) -> no_return().
start_gc(Root) ->
    % Wait some time for all nodes to start and stabilize.
    InitialWait =
        case disco:has_setting("DDFS_GC_INITIAL_WAIT") of
            true -> list_to_integer(disco:get_setting("DDFS_GC_INITIAL_WAIT")) * ?MINUTE;
            false -> ?GC_DEFAULT_INITIAL_WAIT
        end,
    timer:sleep(InitialWait),
    process_flag(trap_exit, true),
    GCMaxDuration = lists:min([?GC_MAX_DURATION,
                               ?ORPHANED_BLOB_EXPIRES,
                               ?ORPHANED_TAG_EXPIRES]),
    start_gc(Root, ets:new(deleted_ages, [set, public]), GCMaxDuration).

-spec start_gc(string(), ets:tab(), non_neg_integer()) -> no_return().
start_gc(Root, DeletedAges, GCMaxDuration) ->
    case ddfs_gc_main:start_link(Root, DeletedAges) of
        {ok, Gc} ->
            Start = now(),
            start_gc_wait(Gc, GCMaxDuration),
            % timer:now_diff() returns microseconds.
            Wait = round(timer:now_diff(now(), Start) / 1000),
            % Wait until the next scheduled gc run slot.
            Idle = ?GC_INTERVAL - (Wait rem ?GC_INTERVAL),
            timer:sleep(Idle);
        E ->
            error_logger:error_report({"GC: error starting", E}),
            timer:sleep(?GC_INTERVAL)
    end,
    start_gc(Root, DeletedAges, GCMaxDuration).

-spec start_gc_wait(pid(), timeout()) -> ok.
start_gc_wait(Pid, Interval) ->
    Start = now(),
    receive
        {'EXIT', Pid, Reason} ->
            error_logger:error_report({"GC: exit", Pid, Reason});
        {'EXIT', Other, Reason} ->
            error_logger:error_report({"GC: unexpected exit", Other, Reason}),
            start_gc_wait(Pid, round(Interval - (timer:now_diff(now(), Start) / 1000)));
        Other ->
            error_logger:error_report({"GC: unexpected msg exit", Other}),
            start_gc_wait(Pid, round(Interval - (timer:now_diff(now(), Start) / 1000)))
    after Interval ->
            error_logger:error_report({"GC: timeout exit"}),
            exit(Pid, force_timeout)
    end.

-spec hosted_tags(nonempty_string()) -> [object_name()] | {'error', term()}.
hosted_tags(Host) ->
    case catch ddfs_master:get_tags(safe) of
        {ok, Tags} ->
            Node = disco:slave_node(Host),
            lists:foldl(
              fun (_T, {error, _} = E) ->
                      E;
                  (T, HostedTags) ->
                      case tag_is_hosted(T, Host, Node, ?MAX_TAG_OP_RETRIES) of
                          true -> [T|HostedTags];
                          false -> HostedTags;
                          E -> E
                      end
              end, [], Tags);
        E ->
            E
    end.

-spec tag_is_hosted(object_name(), nonempty_string(), node(), non_neg_integer()) ->
                           boolean() | {'error', term()}.
tag_is_hosted(T, _Host, _Node, 0) ->
    {error, {get_tag, T}};
tag_is_hosted(T, Host, Node, Retries) ->
    case catch ddfs_master:tag_operation(gc_get, T, ?GET_TAG_TIMEOUT) of
        {{missing, _}, false} ->
            false;
        {'EXIT', {timeout, _}} ->
            tag_is_hosted(T, Host, Node, Retries - 1);
        {_Id, Urls, TagReplicas} ->
            lists:member(Node, TagReplicas) orelse urls_are_hosted(Urls, Host, Node);
        E ->
            E
    end.

-spec urls_are_hosted([[url()]], nonempty_string(), node())
                     -> boolean() | {'error' | term()}.
urls_are_hosted([], _Host, _Node) ->
    false;
urls_are_hosted([[]|Rest], Host, Node) ->
    urls_are_hosted(Rest, Host, Node);
urls_are_hosted([Urls|Rest], Host, Node) ->
    Hosted =
        lists:foldl(
          fun (<<"tag://", _/binary>> = T, false) ->
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
