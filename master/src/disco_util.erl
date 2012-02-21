-module(disco_util).
-export([groupby/2, join/2, format_timestamp/1]).

% Define locally since this is not exported from stdlib/timer.erl or erts/erlang.erl.
-type timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-export_type([timestamp/0]).

-spec format_timestamp(erlang:timestamp()) -> binary().
format_timestamp(TimeStamp) ->
    {Date, Time} = calendar:now_to_local_time(TimeStamp),
    DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
    TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
    list_to_binary([DateStr, TimeStr]).

groupby(N, TupleList) ->
    groupby(N, TupleList, []).

groupby(_N, [], Groups) -> lists:reverse(Groups);
groupby(N, [H|_] = List, Groups) ->
    Key = element(N, H),
    {Group, Rest} = lists:splitwith(fun(X) -> Key =:= element(N, X) end, List),
    groupby(N, Rest, [Group|Groups]).

join([], _Separator) -> [];
join([_] = List, _Separator) -> List;
join([F|List], Separator) ->
    lists:flatten([F, [[Separator, E] || E <- List]]).
