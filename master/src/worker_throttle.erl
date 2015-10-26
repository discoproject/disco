-module(worker_throttle).
-export([init/0, handle/1]).

-define(MAX_EVENTS_PER_SECOND, 5).
-define(MICROSECONDS_IN_SECOND, 1000000).

%% XXX opaque confuses Dialyzer because the opaque declaration expects to find some structure declaration in its right hand side, not another opaque term
-type state() :: queue:queue().
-type throttle() :: {ok, non_neg_integer(), state()} | {error, term()}.

-export_type([state/0]).

-spec init() -> state().
init() ->
    queue:new().

-spec handle(state()) -> throttle().
handle(Q) ->
    Now = erlang:timestamp(),
    Q1 = queue:in(Now, Q),
    Diff = timer:now_diff(Now, queue:get(Q1)),
    if Diff > ?MICROSECONDS_IN_SECOND ->
        Q2 = queue:drop(Q1),
        throttle(Q2, queue:len(Q2));
    true ->
        throttle(Q1, queue:len(Q1))
    end.

-spec throttle(queue:queue(), non_neg_integer()) -> throttle().
throttle(_Q, N) when N >= ?MAX_EVENTS_PER_SECOND * 2 ->
    {error, ["Worker is behaving badly: Sent ",
             integer_to_list(N), " events in a second, ignoring replies."]};

throttle(Q, N) when N > ?MAX_EVENTS_PER_SECOND ->
    {ok, 1000 div ?MAX_EVENTS_PER_SECOND, Q};

throttle(Q, _N) ->
    {ok, 0, Q}.
