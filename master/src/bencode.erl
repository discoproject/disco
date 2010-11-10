-module(bencode).

-export([encode/1, decode/1]).

ascii(Int) when is_integer(Int) ->
    list_to_binary(integer_to_list(Int)).

decode(Binary) when is_binary(Binary) ->
    {Object, _Rest} = decode_(Binary),
    Object.

decode_(<<$i, Rest/binary>>) ->
    decode_int(Rest, []);
decode_(<<$d, Rest/binary>>) ->
    decode_dict(Rest, dict:new());
decode_(<<$l, Rest/binary>>) ->
    decode_list(Rest, []);
decode_(Bytes) ->
    decode_bytes(Bytes, []).

decode_int(<<$e, Rest/binary>>, Acc) ->
    {list_to_integer(lists:reverse(Acc)), Rest};
decode_int(<<Digit, Rest/binary>>, Acc) ->
    decode_int(Rest, [Digit|Acc]).

decode_bytes(<<$:, Rest0/binary>>, Acc) ->
    Size = list_to_integer(lists:reverse(Acc)),
    <<Bytes:Size/binary, Rest1/binary>> = Rest0,
    {Bytes, Rest1};
decode_bytes(<<Digit, Rest/binary>>, Acc) ->
    decode_bytes(Rest, [Digit|Acc]).

decode_dict(<<$e, Rest/binary>>, Acc) ->
    {Acc, Rest};
decode_dict(Data, Acc) ->
    {Key, Rest0} = decode_(Data),
    {Val, Rest1} = decode_(Rest0),
    decode_dict(Rest1, dict:store(Key, Val, Acc)).

decode_list(<<$e, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
decode_list(Data, Acc) ->
    {Item, Rest} = decode_(Data),
    decode_list(Rest, [Item|Acc]).

encode(Bytes) when is_binary(Bytes) ->
    <<(ascii(size(Bytes)))/binary, $:, Bytes/binary>>;
encode(Int) when is_integer(Int) ->
    <<$i, (ascii(Int))/binary, $e>>;
encode(List) when is_list(List) ->
    <<$l, (list_to_binary([encode(Item) || Item <- List]))/binary, $e>>;
encode(Dict) ->
    <<$d, (list_to_binary(dict:fold(fun(Key, Val, Acc) ->
                                            Acc ++ [encode(Key), encode(Val)]
                                    end, [], Dict)))/binary, $e>>.
