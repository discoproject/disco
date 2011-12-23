-module(ddfs_gc).
-export([start_gc/1, abort/2]).

-include("config.hrl").

-define(GET_TAG_TIMEOUT, 5 * ?MINUTE).

-spec abort(term(), atom()) -> no_return().
abort(Msg, Code) ->
    error_logger:warning_report({"GC: aborted", Msg}),
    exit(Code).

-spec start_gc(string()) -> no_return().
start_gc(Root) ->
    % Wait some time for all nodes to start and stabilize.
    timer:sleep(?GC_INITIAL_WAIT),
    process_flag(trap_exit, true),
    start_gc(Root, ets:new(deleted_ages, [set, public])).

-spec start_gc(string(), ets:tab()) -> no_return().
start_gc(Root, DeletedAges) ->
    case ddfs_gc_main:start_link(Root, DeletedAges) of
        {ok, Gc} ->
            Wait = start_gc_wait(Gc, ?GC_INTERVAL),
            case ?GC_INTERVAL > Wait of
                true ->
                    timer:sleep(?GC_INTERVAL - Wait);
                false ->
                    ok
            end;
        E ->
            error_logger:error_report({"GC: error starting", E}),
            timer:sleep(?GC_INTERVAL)
    end,
    start_gc(Root, DeletedAges).

-spec start_gc_wait(pid(), timer:time()) -> timer:time().
start_gc_wait(Pid, Interval) ->
    Start = now(),
    receive
	{'EXIT', Pid, Reason} ->
	    error_logger:error_report({"GC: exit", Pid, Reason});
	{'EXIT', Other, Reason} ->
	    error_logger:error_report({"GC: unexpected exit", Other, Reason});
	Other ->
	    error_logger:error_report({"GC: unexpected msg exit", Other})
    after Interval ->
	    error_logger:error_report({"GC: timeout exit"})
    end,
    % timer:now_diff() returns microseconds.
    round(timer:now_diff(now(), Start) / 1000).
