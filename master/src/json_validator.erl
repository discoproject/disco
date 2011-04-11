-module(json_validator).
-export([validate/2]).
-export([test/0]).

-type prim() :: 'null' | 'integer' | 'boolean' | 'float' | 'string'.
-type spec() :: prim()
              | {'array', [spec()]}
              | {'hom_array', spec()}
              | {'object', [{binary(), spec()}]}
              | {'value', term()}
              | {'opt', [spec()]}.
-export_type([spec/0]).

-spec validate(spec(), term()) -> 'ok' | {'error', tuple()}.
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
    case lists:any(fun(V) -> V =:= ok end,
                   lists:map(fun(T) -> validate(T, Payload) end,
                             Types)) of
        true -> ok;
        false -> {error, {all_options_failed, Types, Payload}}
    end.

-spec validate_key({binary(), spec()}, term()) -> 'ok' | {'error', tuple()}.
validate_key({Key, Type}, Payload) ->
    case proplists:get_value(Key, Payload) of
        undefined -> {error, {missing_key, Key, Payload}};
        Value -> validate(Type, Value)
    end.


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
                          ok = validate(Type, Payload)
                  end, Tests).
