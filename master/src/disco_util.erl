-module(disco_util).
-export([groupby/2, format_timestamp/1]).

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
