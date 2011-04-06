-module(worker_throttle).
-export([init/0, handle/1]).

-define(MAX_EVENTS_PER_SECOND, 5).

init() ->
    queue:new().

handle(Q) ->
    Now = now(),
    Q1 = queue:in(Now, Q),
    Diff = timer:now_diff(Now, queue:get(Q1)) / 1000,
    if Diff > 1000 ->
        Q2 = queue:drop(Q1),
        throttle(Q2, queue:len(Q2));
    true ->
        throttle(Q1, queue:len(Q1))
    end.

throttle(_Q, N) when N > ?MAX_EVENTS_PER_SECOND * 3 ->
    {error, ["Worker is behaving badly: Sent ",
             integer_to_list(N), " events in a second, ignoring replies."]};

throttle(Q, N) when N > ?MAX_EVENTS_PER_SECOND ->
    {ok, 1000 div ?MAX_EVENTS_PER_SECOND, Q};

throttle(Q, _N) ->
    {ok, 0, Q}.

