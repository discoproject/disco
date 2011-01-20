-module(dencode).

-export([encode/1, decode/1]).

ascii(Int) when is_integer(Int) ->
    list_to_binary(integer_to_list(Int)).

read(<<Binary/binary>>, Num) ->
    <<Bin:Num/binary, Rest/binary>> = Binary,
    {Bin, Rest};
read(File, Num) ->
    {prim_file:read(File, Num), File}.

decode(Stream) ->
    {Object, _Rest} = decode_(read(Stream, 1)),
    Object.

decode_({<<$i>>, Stream}) ->
    decode_int(read(Stream, 1), []);
decode_({<<$b>>, Stream}) ->
    decode_bytes(read(Stream, 1), []);
decode_({<<$d>>, Stream}) ->
    decode_dict(read(Stream, 1), dict:new());
decode_({<<$l>>, Stream}) ->
    decode_list(read(Stream, 1), []).

decode_int({<<$\n>>, Stream}, Acc) ->
    {list_to_integer(lists:flatten(lists:reverse(Acc))), Stream};
decode_int({<<Digit/binary>>, Stream}, Acc) ->
    decode_int(read(Stream, 1), [binary_to_list(Digit)|Acc]).

decode_bytes({<<$\n>>, Stream}, Acc) ->
    Size = list_to_integer(lists:reverse(Acc)),
    read(Stream, Size);
decode_bytes({<<Digit>>, Stream}, Acc) ->
    decode_bytes(read(Stream, 1), [Digit|Acc]).

decode_dict({<<$\n>>, Stream}, Acc) ->
    {Acc, Stream};
decode_dict({<<$,>>, Stream}, Acc) ->
    decode_dict(read(Stream, 1), Acc);
decode_dict({Byte, Stream}, Acc) ->
    {Key, Stream1} = decode_({Byte, Stream}),
    {<<$,>>, Stream2} = read(Stream1, 1),
    {Val, Stream3} = decode_(read(Stream2, 1)),
    decode_dict(read(Stream3, 1), dict:store(Key, Val, Acc)).

decode_list({<<$\n>>, Stream}, Acc) ->
    {lists:reverse(Acc), Stream};
decode_list({<<$,>>, Stream}, Acc) ->
    decode_list(read(Stream, 1), Acc);
decode_list({Byte, Stream}, Acc) ->
    {Item, Stream1} = decode_({Byte, Stream}),
    decode_list(read(Stream1, 1), [Item|Acc]).

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
