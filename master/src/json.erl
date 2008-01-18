%% JSON - RFC 4627 - for Erlang
%%---------------------------------------------------------------------------
%% Copyright (c) 2007 Tony Garnock-Jones <tonyg@kcbbs.gen.nz>
%% Copyright (c) 2007 LShift Ltd. <query@lshift.net>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use, copy,
%% modify, merge, publish, distribute, sublicense, and/or sell copies
%% of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
%% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
%% BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
%% ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
%% CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%---------------------------------------------------------------------------
%%
%% encode(val()) -> str()
%% decode(str()) -> {ok, val(), str()} | {error, Reason}
%%                  where Reason is usually far too much information
%%                  and should be ignored.
%%
%% Data type mapping as per Joe Armstrong's message
%% http://www.erlang.org/ml-archive/erlang-questions/200511/msg00193.html:
%%
%%	JSON Obj    = type obj()   = {obj, [{key(), val()}]}
%%	JSON Array  = type array() = [val()]
%%	JSON Number = type num()   = int() | float() 
%%	JSON String = type str()   = bin()
%%	JSON true false null       = true, false null (atoms)
%%	With Type val() = obj() | array() | num() | str() | true | false | null
%%
%% and key() being a str(). (Or a binary or atom, during JSON encoding.)
%%
%% No unicode processing is done other than minimal \uXXXX parsing and generation.
%%
%% I'm lenient in the following ways during parsing:
%%  - repeated commas in arrays and objects collapse to a single comma
%%  - characters =<32 or >127 that somehow appear in the input stream
%%    inside a string are silently accepted unchanged
%%  - any character =<32 is considered whitespace
%%  - leading zeros for numbers are accepted

-module(json).

-export([mime_type/0, encode/1, encode/2, decode/1]).
-export([from_record/3, to_record/3]).
-export([hex_digit/1, digit_hex/1]).

mime_type() ->
    "application/json".

encode(X) ->
    lists:reverse(encode(X, [])).

encode(true, Acc) ->
    "eurt" ++ Acc;
encode(false, Acc) ->
    "eslaf" ++ Acc;
encode(null, Acc) ->
    "llun" ++ Acc;
encode(Str, Acc) when is_binary(Str) ->
    quote_and_encode_string(binary_to_list(Str), Acc);
encode(Str, Acc) when is_atom(Str) ->
    quote_and_encode_string(atom_to_list(Str), Acc);
encode(Num, Acc) when is_number(Num) ->
    encode_number(Num, Acc);
encode({obj, Fields}, Acc) ->
    "}" ++ encode_object(Fields, "{" ++ Acc);
% Tuple case added by Ville
encode(Str, Acc) when is_tuple(Str) ->
    encode(tuple_to_list(Str), Acc);
encode(Arr, Acc) when is_list(Arr) ->
    "]" ++ encode_array(Arr, "[" ++ Acc).

encode_object([], Acc) ->
    Acc;
encode_object([{Key, Value}], Acc) ->
    encode_field(Key, Value, Acc);
encode_object([{Key, Value} | Rest], Acc) ->
    encode_object(Rest, "," ++ encode_field(Key, Value, Acc)).

encode_field(Key, Value, Acc) when is_binary(Key) ->
    encode(Value, ":" ++ quote_and_encode_string(binary_to_list(Key), Acc));
encode_field(Key, Value, Acc) when is_atom(Key) ->
    encode(Value, ":" ++ quote_and_encode_string(atom_to_list(Key), Acc));
encode_field(Key, Value, Acc) when is_list(Key) ->
    encode(Value, ":" ++ quote_and_encode_string(Key, Acc)).

encode_array([], Acc) ->
    Acc;
encode_array([X], Acc) ->
    encode(X, Acc);
encode_array([X | Rest], Acc) ->
    encode_array(Rest, "," ++ encode(X, Acc)).

quote_and_encode_string(Str, Acc) ->
    "\"" ++ encode_string(Str, "\"" ++ Acc).

encode_string([], Acc) ->
    Acc;
encode_string([$" | Rest], Acc) ->
    encode_string(Rest, [$", $\\ | Acc]);
encode_string([$\\ | Rest], Acc) ->
    encode_string(Rest, [$\\, $\\ | Acc]);
encode_string([X | Rest], Acc) when X < 32 orelse X > 127 ->
    encode_string(Rest, encode_general_char(X, Acc));
encode_string([X | Rest], Acc) ->
    encode_string(Rest, [X | Acc]).

encode_general_char(8, Acc) -> [$b, $\\ | Acc];
encode_general_char(9, Acc) -> [$t, $\\ | Acc];
encode_general_char(10, Acc) -> [$n, $\\ | Acc];
encode_general_char(12, Acc) -> [$f, $\\ | Acc];
encode_general_char(13, Acc) -> [$r, $\\ | Acc];
encode_general_char(X, Acc) ->
    [hex_digit((X) band 16#F),
     hex_digit((X bsr 4) band 16#F),
     hex_digit((X bsr 8) band 16#F),
     hex_digit((X bsr 12) band 16#F),
     $u,
     $\\ | Acc].

hex_digit(0) -> $0;
hex_digit(1) -> $1;
hex_digit(2) -> $2;
hex_digit(3) -> $3;
hex_digit(4) -> $4;
hex_digit(5) -> $5;
hex_digit(6) -> $6;
hex_digit(7) -> $7;
hex_digit(8) -> $8;
hex_digit(9) -> $9;
hex_digit(10) -> $A;
hex_digit(11) -> $B;
hex_digit(12) -> $C;
hex_digit(13) -> $D;
hex_digit(14) -> $E;
hex_digit(15) -> $F.

encode_number(Num, Acc) when is_integer(Num) ->
    lists:reverse(integer_to_list(Num), Acc);
encode_number(Num, Acc) when is_float(Num) ->
    lists:reverse(float_to_list(Num), Acc).

decode(Bin) when is_binary(Bin) ->
    decode(binary_to_list(Bin));
decode(Chars) ->
    case catch parse(skipws(Chars)) of
	{'EXIT', Reason} ->
	    %% Reason is usually far too much information, but helps
	    %% if needing to debug this module.
	    {error, Reason};
	{Value, Remaining} ->
	    {ok, Value, skipws(Remaining)}
    end.

parse([$" | Rest]) -> %% " emacs balancing
    {Str, Rest1} = parse_string(Rest, []),
    {list_to_binary(Str), Rest1};
parse("true" ++ Rest) -> {true, Rest};
parse("false" ++ Rest) -> {false, Rest};
parse("null" ++ Rest) -> {null, Rest};
parse([${ | Rest]) -> parse_object(skipws(Rest), []);
parse([$[ | Rest]) -> parse_array(skipws(Rest), []);
parse(Chars) -> parse_number(Chars, []).

skipws([X | Rest]) when X =< 32 ->
    skipws(Rest);
skipws(Chars) ->
    Chars.

parse_string([$" | Rest], Acc) -> %% " emacs balancing
    {lists:reverse(Acc), Rest};
parse_string([$\\, Key | Rest], Acc) ->
    parse_general_char(Key, Rest, Acc);
parse_string([X | Rest], Acc) ->
    parse_string(Rest, [X | Acc]).

parse_general_char($b, Rest, Acc) -> parse_string(Rest, [8 | Acc]);
parse_general_char($t, Rest, Acc) -> parse_string(Rest, [9 | Acc]);
parse_general_char($n, Rest, Acc) -> parse_string(Rest, [10 | Acc]);
parse_general_char($f, Rest, Acc) -> parse_string(Rest, [12 | Acc]);
parse_general_char($r, Rest, Acc) -> parse_string(Rest, [13 | Acc]);
parse_general_char($/, Rest, Acc) -> parse_string(Rest, [$/ | Acc]);
parse_general_char($\\, Rest, Acc) -> parse_string(Rest, [$\\ | Acc]);
parse_general_char($", Rest, Acc) -> parse_string(Rest, [$" | Acc]);
parse_general_char($u, [D0, D1, D2, D3 | Rest], Acc) ->
    parse_string(Rest, [(digit_hex(D0) bsl 12) +
			(digit_hex(D1) bsl 8) +
			(digit_hex(D2) bsl 4) +
			(digit_hex(D3)) | Acc]).

digit_hex($0) -> 0;
digit_hex($1) -> 1;
digit_hex($2) -> 2;
digit_hex($3) -> 3;
digit_hex($4) -> 4;
digit_hex($5) -> 5;
digit_hex($6) -> 6;
digit_hex($7) -> 7;
digit_hex($8) -> 8;
digit_hex($9) -> 9;

digit_hex($A) -> 10;
digit_hex($B) -> 11;
digit_hex($C) -> 12;
digit_hex($D) -> 13;
digit_hex($E) -> 14;
digit_hex($F) -> 15;

digit_hex($a) -> 10;
digit_hex($b) -> 11;
digit_hex($c) -> 12;
digit_hex($d) -> 13;
digit_hex($e) -> 14;
digit_hex($f) -> 15.

finish_number(Acc, Rest) ->
    Str = lists:reverse(Acc),
    {case catch list_to_integer(Str) of
	 {'EXIT', _} -> list_to_float(Str);
	 Value -> Value
     end, Rest}.

parse_number([], _Acc) ->
    exit(syntax_error);
parse_number([$- | Rest], Acc) ->
    parse_number1(Rest, [$- | Acc]);
parse_number(Rest, Acc) ->
    parse_number1(Rest, Acc).

parse_number1(Rest, Acc) ->
    {Acc1, Rest1} = parse_int_part(Rest, Acc),
    case Rest1 of
	[] -> finish_number(Acc1, []);
	[$. | More] ->
            {Acc2, Rest2} = parse_int_part(More, [$. | Acc1]),
            parse_exp(Rest2, Acc2, false);
        _ ->
            parse_exp(Rest1, Acc1, true)
    end.

parse_int_part(Chars = [_Ch | _Rest], Acc) ->
    parse_int_part0(Chars, Acc).

parse_int_part0([], Acc) ->
    {Acc, []};
parse_int_part0([Ch | Rest], Acc) ->
    case is_digit(Ch) of
	true -> parse_int_part0(Rest, [Ch | Acc]);
	false -> {Acc, [Ch | Rest]}
    end.

parse_exp([$e | Rest], Acc, NeedFrac) ->
    parse_exp1(Rest, Acc, NeedFrac);
parse_exp([$E | Rest], Acc, NeedFrac) ->
    parse_exp1(Rest, Acc, NeedFrac);
parse_exp(Rest, Acc, _NeedFrac) ->
    finish_number(Acc, Rest).

parse_exp1(Rest, Acc, NeedFrac) ->
    {Acc1, Rest1} = parse_signed_int_part(Rest, if
						    NeedFrac -> [$e, $0, $. | Acc];
						    true -> [$e | Acc]
						end),
    finish_number(Acc1, Rest1).

parse_signed_int_part([$+ | Rest], Acc) ->
    parse_int_part(Rest, [$+ | Acc]);
parse_signed_int_part([$- | Rest], Acc) ->
    parse_int_part(Rest, [$- | Acc]);
parse_signed_int_part(Rest, Acc) ->
    parse_int_part(Rest, Acc).

is_digit($0) -> true;
is_digit($1) -> true;
is_digit($2) -> true;
is_digit($3) -> true;
is_digit($4) -> true;
is_digit($5) -> true;
is_digit($6) -> true;
is_digit($7) -> true;
is_digit($8) -> true;
is_digit($9) -> true;
is_digit(_) -> false.

parse_object([$} | Rest], Acc) ->
    {{obj, lists:reverse(Acc)}, Rest};
parse_object([$, | Rest], Acc) ->
    parse_object(skipws(Rest), Acc);
parse_object([$" | Rest], Acc) -> %% " emacs balancing
    {Key, Rest1} = parse_string(Rest, []),
    [$: | Rest2] = skipws(Rest1),
    {Value, Rest3} = parse(skipws(Rest2)),
    parse_object(skipws(Rest3), [{Key, Value} | Acc]).

parse_array([$] | Rest], Acc) ->
    {lists:reverse(Acc), Rest};
parse_array([$, | Rest], Acc) ->
    parse_array(skipws(Rest), Acc);
parse_array(Chars, Acc) ->
    {Value, Rest} = parse(Chars),
    parse_array(skipws(Rest), [Value | Acc]).

from_record(R, _RName, Fields) ->
    {obj, encode_record_fields(R, 2, Fields)}.

encode_record_fields(_R, _Index, []) ->
    [];
encode_record_fields(R, Index, [Field | Rest]) ->
    case element(Index, R) of
	undefined ->
	    encode_record_fields(R, Index + 1, Rest);
	Value ->
	    [{atom_to_list(Field), Value} | encode_record_fields(R, Index + 1, Rest)]
    end.

to_record({obj, Values}, Fallback, Fields) ->
    list_to_tuple([element(1, Fallback) | decode_record_fields(Values, Fallback, 2, Fields)]).

decode_record_fields(_Values, _Fallback, _Index, []) ->
    [];
decode_record_fields(Values, Fallback, Index, [Field | Rest]) ->
    [case lists:keysearch(atom_to_list(Field), 1, Values) of
	 {value, {_, Value}} ->
	     Value;
	 false ->
	     element(Index, Fallback)
     end | decode_record_fields(Values, Fallback, Index + 1, Rest)].
