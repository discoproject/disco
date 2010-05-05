-module(http_queue).
-export([new/2, add/2, remove/2]).

-record(q, {max_waiting, max_active, waiters, active}).

new(MaxActive, MaxWaiting) ->
    #q{max_waiting = MaxWaiting,
       max_active = MaxActive,
       waiters = [],
       active = []}.

add({Id, Fun}, #q{max_active = MaxA, active = A} = Q) when length(A) < MaxA ->
    {{active, Fun()}, Q#q{active = [Id|A]}};

add(E, #q{max_waiting = MaxW, waiters = W} = Q) when length(W) < MaxW ->
    {wait, Q#q{waiters = [E|W]}};

add(_E, _Q) -> full.

remove(Id, #q{active = A, waiters = W} = Q) ->
    case A -- [Id] of
        L when L =:= A ->
            {wait, Q#q{waiters = lists:keydelete(Id, 1, W)}};
        L ->
            wake(Q#q{active = L})
    end.

wake(#q{waiters = []} = Q) ->
    {wait, Q};
wake(#q{waiters = W} = Q) ->
    [E|R] = lists:reverse(W),
    add(E, Q#q{waiters = lists:reverse(R)}).






