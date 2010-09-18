-module(disco_util).
-export([groupby/2, join/2]).

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
