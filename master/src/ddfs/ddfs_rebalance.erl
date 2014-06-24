-module(ddfs_rebalance).
-export([
         utility/1,
         avg_disk_usage/1,
         threshold/0,
         is_balanced/3,
         weighted_select_from_nodes/2,
         less/4
     ]).

-include("config.hrl").

-type node_info() :: {node(), {non_neg_integer(), non_neg_integer()}}.

-spec utility(node_info()) -> non_neg_integer().
utility(Node) ->
    utility(Node, disco:has_setting("DDFS_ABSOLUTE_SPACE")).

utility({_N, {Free, Used}}, false) ->
    Sum = Free + Used,
    case Sum of
        0 -> 0;
        _ -> Used / Sum
    end;

utility({_N, {Free, _Used}}, true) ->
    -Free.

-spec avg_disk_usage([node_info()]) -> non_neg_integer().
avg_disk_usage(NodeStats) ->
    avg_disk_usage(NodeStats, disco:has_setting("DDFS_ABSOLUTE_SPACE")).

avg_disk_usage(NodeStats, false) ->
    {SumFree, SumUsed} = lists:foldl(
            fun({_N, {Free, Used}}, {SFree, SmUsed}) ->
                    {SFree + Free, SmUsed + Used}
            end, {0, 0}, NodeStats),
    NumNodes = length(NodeStats),
    case NumNodes of
        0 -> 0;
        _ -> case SumFree + SumUsed of
                0 -> 0;
                _ -> SumUsed / (NumNodes * (SumFree + SumUsed))
            end
    end;

avg_disk_usage(NodeStats, true) ->
    SumFree = lists:foldl(
            fun({_N, {Free, _}}, SFree) ->
                    SFree + Free
            end,
            0, NodeStats),
    NumNodes = length(NodeStats),
    case NumNodes of
        0 -> 0;
        _ -> -SumFree / NumNodes
    end.

-spec threshold() -> float().
threshold() ->
    case disco:has_setting("DDFS_GC_BALANCE_THRESHOLD") of
        true  -> element(1,
                string:to_float(disco:get_setting("DDFS_GC_BALANCE_THRESHOLD")));
        false -> ?GC_BALANCE_THRESHOLD
    end.

-spec is_balanced(non_neg_integer(), non_neg_integer(), float()) -> true | false.
is_balanced(Balanced, Threshold, DiskSpace) ->
    is_balanced(Balanced, Threshold, DiskSpace, disco:has_setting("DDFS_ABSOLUTE_SPACE")).

is_balanced(Balanced, Threshold, DiskSpace, false) ->
    (Balanced / DiskSpace) > Threshold;

is_balanced(Balanced, Threshold, _DiskSpace, true) ->
    Balanced > Threshold.

-spec less(non_neg_integer(), non_neg_integer(),
        non_neg_integer(), non_neg_integer()) -> true | false.

less(B1, DS1, B2, DS2) ->
    less(B1, DS1, B2, DS2, disco:has_setting("DDFS_ABSOLUTE_SPACE")).

less(B1, DS1, B2, DS2, false) ->
    B1 / DS1 =< B2 / DS2;

less(B1, _DS1, B2, _DS2, true) ->
    B1 =< B2.

-spec weighted_select_from_nodes(list(T), non_neg_integer()) -> list(T) | error.
weighted_select_from_nodes(List, K) ->
    Utilization = [{N, 1 - ddfs_rebalance:utility(Node)} || ({N, _} = Node) <- List],
    TotalSum = lists:foldl(fun({_, I}, S) -> S + I end, 0, Utilization),
    case TotalSum of
    0 -> error;
    _ ->
        NormalizedUtil = lists:foldl(fun({N, I}, L) -> [{N, I / TotalSum} | L] end, [],
            Utilization),
        weighted_select_items(NormalizedUtil, K)
    end.

-spec weighted_select_items(list(T), non_neg_integer()) -> list(T) | error.
weighted_select_items(L, K) when length(L) < K ->
    error;
weighted_select_items(L, K) ->
    weighted_select_items(L, K, [], 0).

weighted_select_items(_, 0, Items, _) ->
    Items;
weighted_select_items(L, K, Items, Consumed) ->
    P = random:uniform() * (1 - Consumed),
    {Item, Weight} = weighted_choose(L, P),
    Rest = lists:keydelete(Item, 1, L),
    weighted_select_items(Rest, K - 1, [Item|Items], Consumed + Weight).

weighted_choose([{Node, Weight}|_], P) when Weight > P ->
    {Node, Weight};
weighted_choose([{Node, Weight}|[]], _) ->
    {Node, Weight};
weighted_choose([{_, Weight}|Rest], P) ->
    weighted_choose(Rest, P - Weight).
