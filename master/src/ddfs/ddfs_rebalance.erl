-module(ddfs_rebalance).
-export([
         utility/1,
         avg_disk_usage/1,
         threshold/0
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
