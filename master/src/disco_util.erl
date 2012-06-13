-module(disco_util).
-export([choose_random/1, choose_random/2, groupby/2,
         format_timestamp/1]).

-spec format_timestamp(erlang:timestamp()) -> binary().
format_timestamp(TimeStamp) ->
    {Date, Time} = calendar:now_to_local_time(TimeStamp),
    DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
    TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
    list_to_binary([DateStr, TimeStr]).

-spec groupby(pos_integer(), [T]) -> [[T]].
groupby(N, TupleList) ->
    groupby(N, TupleList, []).

groupby(_N, [], Groups) -> lists:reverse(Groups);
groupby(N, [H|_] = List, Groups) ->
    Key = element(N, H),
    {Group, Rest} = lists:splitwith(fun(X) -> Key =:= element(N, X) end, List),
    groupby(N, Rest, [Group|Groups]).

-spec choose_random([T,...]) -> T.
choose_random(L) ->
    lists:nth(random:uniform(length(L)), L).

-spec choose_random(list(T), non_neg_integer()) -> list(T).
choose_random(L, N) ->
    choose_random(L, [], N).

choose_random([], R, _) -> R;
choose_random(_, R, 0) -> R;
choose_random(L, R, N) ->
    C = choose_random(L),
    choose_random(L -- [C], [C|R], N - 1).
