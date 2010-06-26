-module(disco_config).
-export([get_config_table/0, save_config_table/1, expand_range/2]).

-spec expand_range(nonempty_string(), nonempty_string()) -> [nonempty_string()].
expand_range(FirstNode, Max) ->
    Len = string:len(FirstNode),
    FieldLen = string:len(Max),
    MaxNum = list_to_integer(Max),
    Name = string:sub_string(FirstNode, 1, Len - FieldLen),
    MinNum = list_to_integer(
        string:sub_string(FirstNode, Len - FieldLen + 1)),
    Format = lists:flatten(io_lib:format("~s~~.~w.0w", [Name, FieldLen])),
    [lists:flatten(io_lib:fwrite(Format, [I])) ||
        I <- lists:seq(MinNum, MaxNum)].

-spec add_nodes([nonempty_string(),...],integer()) ->
    [{[char()], integer()}] | {nonempty_string(), integer()}.
add_nodes([FirstNode, Max], Instances) ->
    [{N, Instances} || N <- expand_range(FirstNode, Max)];

add_nodes([Node], Instances) -> {Node, Instances}.

-spec parse_row([binary(),...]) ->
    [{[char()], integer()}] | {nonempty_string(), integer()}.
parse_row([NodeSpecB, InstancesB]) ->
    NodeSpec = string:strip(binary_to_list(NodeSpecB)),
    Instances = string:strip(binary_to_list(InstancesB)),
    add_nodes(string:tokens(NodeSpec, ":"), list_to_integer(Instances)).

-spec update_config_table([[binary(), ...]]) -> _.
update_config_table(Json) ->
    gen_server:cast(disco_server, {update_config_table,
        lists:flatten([parse_row(R) || R <- Json])}).

-spec get_config_table() -> {'ok', [[binary(), ...]]}.
get_config_table() ->
    case file:read_file(os:getenv("DISCO_MASTER_CONFIG")) of
        {ok, Config} -> ok;
        {error, enoent} ->
            Config = "[]"
    end,
    Json = mochijson2:decode(Config),
    update_config_table(Json),
    {ok, Json}.

-spec save_config_table([[binary(), ...]]) -> {'error' | 'ok', binary()}.
save_config_table(Json) ->
    {Nodes, _Cores} = lists:unzip(lists:flatten([parse_row(R) || R <- Json])),
    Sorted = lists:sort(Nodes),
    USorted = lists:usort(Nodes),
    if
        length(Sorted) == length(USorted) ->
            ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                                 mochijson2:encode(Json)),
            update_config_table(Json),
            {ok, <<"table saved!">>};
        true ->
            {error, <<"duplicate nodes">>}
    end.
