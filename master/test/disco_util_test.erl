-module(disco_util_test).
-include_lib("eunit/include/eunit.hrl").

sorted_items(NodeList, K) ->
    lists:sort(disco_util:weighted_select_items(NodeList, K)).

weighted_random_test() ->
    Tests = [{[{n1, 1}], 1, [n1]},
             {[{n1, 1}, {n2, 2}], 2, [n1, n2]},
             {[{n1, 1/6}, {n2, 2/6}, {n3, 3/6}], 3, [n1, n2, n3]},
             {[{n1, 1}, {n2, 2/6}, {n3, 3/6}], 1, [n1]},
             {[{n1, 1}, {n2, 1}, {n3, 1}, {n4, 1}], 2, [n1, n2]}
            ],

    lists:foreach(fun({NodeList, K, Results}) ->
                    Results = sorted_items(NodeList, K)
                  end, Tests),

    ErrorTests = [{[{n1, 0.5}, {n2, 0.5}, {n3, 1}, {n4, 1}], 5},
                  {[], 1}
                 ],

    lists:foreach(fun({NodeList, K}) ->
                    error = disco_util:weighted_select_items(NodeList, K)
                  end, ErrorTests).
