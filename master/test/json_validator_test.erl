-module(json_validator_test).

-export([test/0]).

test() ->
    Tests = [{null, "null"},
             {integer, "1"},
             {integer, "-1"},
             {boolean, "true"},
             {boolean, "false"},
             {float, "0.1"},
             {float, "-0.1"},
             {string, "\"string\""},
             {{array, [null, boolean, string]},
              "[null, true, \"string\"]"},
             {{array, [{array, [integer, boolean]},
                       integer]},
              "[[1, true], 3]"},
             {{hom_array, boolean}, "[true, false]"},
             {{object, [{<<"intkey">>, integer},
                        {<<"arraykey">>, {hom_array, integer}}]},
              "{\"intkey\" : 1, \"arraykey\" : [1,2]}"},
             {{value, <<"string">>}, "\"string\""},
             {{opt, [integer, string, boolean]}, "true"}
            ],

    lists:foreach(fun({Type, Json}) ->
                          Payload = mochijson2:decode(Json),
                          ok = json_validator:validate(Type, Payload)
                  end, Tests).
