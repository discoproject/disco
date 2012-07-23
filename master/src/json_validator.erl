-module(json_validator).
-export([validate/2, error_msg/1]).

-type prim() :: null | integer | boolean | float | string.
-type spec() :: prim()
              | {array, [spec()]}
              | {hom_array, spec()}
              | {object, [{binary(), spec()}]}
              | {value, term()}
              | {opt, [spec()]}.
-type not_error() :: {not_null | not_integer | not_boolean | not_float
                      | not_string | not_list | not_object}.
-type error() :: {not_error(), term()}
               | {incorrect_length, term(), non_neg_integer()}
               | {unexpected_value, term(), term()}
               | {all_options_failed, spec(), term()}
               | {missing_key, binary(), term()}.
-export_type([spec/0]).

-spec validate(spec(), term()) -> ok | {error, error()}.
% primitives
validate(null, null) -> ok;
validate(integer, Payload) when is_integer(Payload) -> ok;
validate(boolean, Payload) when is_boolean(Payload)-> ok;
validate(float, Payload) when is_float(Payload) -> ok;
validate(string, Payload) when is_binary(Payload) -> ok;
validate(Type, Payload) when is_atom(Type) ->
    {error, {list_to_atom("not_" ++ atom_to_list(Type)), Payload}};

% heterogenous lists
validate({array, _Types}, Payload) when not is_list(Payload) ->
    {error, {not_list, Payload}};
validate({array, Types}, Payload) when length(Types) =/= length(Payload) ->
    {error, {incorrect_length, Payload, length(Types)}};
validate({array, Types}, Payload) ->
    lists:foldl(fun({T, P}, Acc) ->
                        case Acc of
                            ok -> validate(T, P);
                            E -> E
                        end
                end, ok, lists:zip(Types, Payload));

% homogenous lists
validate({hom_array, _Type}, Payload) when not is_list(Payload)->
    {error, {not_list, Payload}};
validate({hom_array, Type}, Payload) ->
    lists:foldl(fun(P, Acc) ->
                        case Acc of
                            ok -> validate(Type, P);
                            E -> E
                        end
                end, ok, Payload);
% dictionaries
% FIXME: does not handle repeated keys in Payload
validate({object, Dict}, {struct, Payload}) ->
    lists:foldl(fun(KeyType, Acc) ->
                        case Acc of
                            ok -> validate_key(KeyType, Payload);
                            E -> E
                        end
                end, ok, Dict);
validate({object, _Dict}, Payload) ->
    {error, {not_object, Payload}};

% explicit values
validate({value, Value}, Payload) when Value =:= Payload -> ok;
validate({value, Value}, Payload) -> {error, {unexpected_value, Value, Payload}};

% union types
validate({opt, Types}, Payload) ->
    case lists:any(fun(T) -> validate(T, Payload) =:= ok end, Types) of
        true -> ok;
        false -> {error, {all_options_failed, Types, Payload}}
    end.

-spec validate_key({binary(), spec()}, term()) -> ok | {error, tuple()}.
validate_key({Key, Type}, Payload) ->
    case proplists:get_value(Key, Payload) of
        undefined -> {error, {missing_key, Key, Payload}};
        Value -> validate(Type, Value)
    end.

-spec error_msg(error()) -> iolist().
error_msg({not_null, Payload}) ->
    io_lib:format("null expected, '~p' received", [Payload]);
error_msg({not_integer, Payload}) ->
    io_lib:format("integer expected, '~p' received", [Payload]);
error_msg({not_boolean, Payload}) ->
    io_lib:format("boolean expected, '~p' received", [Payload]);
error_msg({not_float, Payload}) ->
    io_lib:format("float expected, '~p' received", [Payload]);
error_msg({not_string, Payload}) ->
    io_lib:format("string expected, '~p' received", [Payload]);
error_msg({not_list, Payload}) ->
    io_lib:format("list expected, '~p' received", [Payload]);
error_msg({not_object, Payload}) ->
    io_lib:format("object/dict expected, '~p' received", [Payload]);
error_msg({incorrect_length, Payload, Len}) ->
    io_lib:format("list of length ~B expected, '~p' received", [Len, Payload]);
error_msg({unexpected_value, Value, Payload}) ->
    io_lib:format("value '~p' expected, '~p' received", [Value, Payload]);
error_msg({all_options_failed, Spec, Payload}) ->
    io_lib:format("value of type '~p' expected, '~p' received", [Spec, Payload]);
error_msg({missing_key, Key, Payload}) ->
    io_lib:format("object with key '~s' expected, '~p' received", [Key, Payload]).
