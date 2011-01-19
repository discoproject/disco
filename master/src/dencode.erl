-module(dencode).

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
decode_(<<$b, Rest/binary>>) ->
    decode_bytes(Rest, []).

decode_int(<<$\n, Rest/binary>>, Acc) ->
    {list_to_integer(lists:reverse(Acc)), Rest};
decode_int(<<Digit, Rest/binary>>, Acc) ->
    decode_int(Rest, [Digit|Acc]).

decode_bytes(<<$\n, Rest0/binary>>, Acc) ->
    Size = list_to_integer(lists:reverse(Acc)),
    <<Bytes:Size/binary, Rest1/binary>> = Rest0,
    {Bytes, Rest1};
decode_bytes(<<Digit, Rest/binary>>, Acc) ->
    decode_bytes(Rest, [Digit|Acc]).

decode_dict(<<$\n, Rest/binary>>, Acc) ->
    {Acc, Rest};
decode_dict(<<$,, Rest/binary>>, Acc) ->
    decode_dict(Rest, Acc);
decode_dict(Data, Acc) ->
    {Key, <<$,, Rest0/binary>>} = decode_(Data),
    {Val, Rest1} = decode_(Rest0),
    decode_dict(Rest1, dict:store(Key, Val, Acc)).

decode_list(<<$\n, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
decode_list(<<$,, Rest/binary>>, Acc) ->
    decode_list(Rest, Acc);
decode_list(Data, Acc) ->
    {Item, Rest} = decode_(Data),
    decode_list(Rest, [Item|Acc]).

encode(Bytes) when is_binary(Bytes) ->
    <<$b, (ascii(size(Bytes)))/binary, $\n, Bytes/binary>>;
encode(Int) when is_integer(Int) ->
    <<$i, (ascii(Int))/binary, $\n>>;
encode(List) when is_list(List) ->
    <<$l, (binary_join([encode(Item) || Item <- List], <<$,>>))/binary, $\n>>;
encode(Dict) ->
    <<$d, (binary_join(dict:fold(fun(Key, Val, Acc) ->
                                         Acc ++ [encode(Key), encode(Val)]
                                 end, [], Dict), <<$,>>))/binary, $\n>>.

binary_join([], <<_Sep/binary>>) ->
    <<"">>;
binary_join([<<Bin/binary>>|[]], <<_Sep/binary>>) ->
    Bin;
binary_join([<<Bin/binary>>|Tail], <<Sep/binary>>) ->
    <<Bin/binary, Sep/binary, (binary_join(Tail, Sep))/binary>>.
