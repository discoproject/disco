-module(ddfs_util_test).
-export([to_hex/1, of_hex/1]).
-export([prop_test/0]).

-include_lib("proper/include/proper.hrl").
%-include("../src/ddfs/config.hrl").
%-include("../src/ddfs/ddfs_util.erl").

-spec to_hex_helper(non_neg_integer(), string()) -> string().
to_hex_helper(Int, L) ->
    case {Int, L} of
        {0, []} -> "00";
        {0, _ } -> L;
        _ ->
            D = Int div 16,
            R = Int rem 16,
            C = if R < 10 -> $0 + R;
                   true   -> $A + R - 10
                end,
            to_hex_helper(D, [C|L])
    end.

-spec to_hex(non_neg_integer()) -> string().
to_hex(Int) ->
    to_hex_helper(Int, []).

-spec of_hex_helper(string(), integer()) ->
                            integer() | {'error', string()}.
of_hex_helper(H, Int) ->
    case H of
        "" ->
            Int;
        [Hd|Tl] when (Hd >= $0) and (Hd =< $9) ->
            of_hex_helper(Tl, Int*16 + (Hd - $0));
        [Hd|Tl] when ((Hd >= $A) and (Hd =< $F)) ->
            of_hex_helper(Tl, Int*16 + 10 + (Hd - $A));
        [Hd|Tl] when ((Hd >= $a) and (Hd =< $f)) ->
            of_hex_helper(Tl, Int*16 + 10 + (Hd - $a));
        _ ->
            {'error', "invalid hex character"}
    end.

-spec of_hex(string()) -> integer() | {'error', string()}.
of_hex(H) ->
    of_hex_helper(H, 0).


prop_inttoint() ->
    ?FORALL(Val, non_neg_integer(), Val == of_hex(to_hex(Val))).

prop_test() ->
    proper:check(prop_inttoint()).
