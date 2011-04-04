-module(http_queue).
-export([new/2, add/2, remove/2]).

-record(q, {max_waiting :: non_neg_integer(),
            max_active :: non_neg_integer(),
            waiters :: [{T, fun(() -> _)}],
            active :: [T]}).

-type q() :: #q{}.
-export_type([q/0]).

-spec new(non_neg_integer(), non_neg_integer()) -> #q{}.
new(MaxActive, MaxWaiting) ->
    #q{max_waiting = MaxWaiting,
       max_active = MaxActive,
       waiters = [],
       active = []}.

-spec add({_, fun(() -> _)}, q()) -> {{'active', _} | 'wait', q()} | 'full'.

add({Id, Fun}, #q{max_active = MaxA, active = A} = Q) when length(A) < MaxA ->
    {{active, Fun()}, Q#q{active = [Id|A]}};

add(E, #q{max_waiting = MaxW, waiters = W} = Q) when length(W) < MaxW ->
    {wait, Q#q{waiters = [E|W]}};

add(_E, _Q) -> full.

-spec remove(_, #q{}) -> {'wait' | {'active', _}, #q{}}.
remove(Id, #q{active = A, waiters = W} = Q) ->
    case A -- [Id] of
        L when L =:= A ->
            {wait, Q#q{waiters = lists:keydelete(Id, 1, W)}};
        L ->
            wake(Q#q{active = L})
    end.

-spec wake(#q{}) -> {'wait' | {'active', _}, #q{}}.
wake(#q{waiters = []} = Q) ->
    {wait, Q};
wake(#q{waiters = W} = Q) ->
    [E|R] = lists:reverse(W),
    add(E, Q#q{waiters = lists:reverse(R)}).
