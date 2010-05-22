-module(disco_config).
-export([get_config_table/0, save_config_table/1, expand_range/2]).

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

add_nodes([FirstNode, Max], Instances) ->
    [{N, Instances} || N <- expand_range(FirstNode, Max)];

add_nodes([Node], Instances) -> {Node, Instances}.

parse_row([NodeSpecB, InstancesB]) ->
    NodeSpec = string:strip(binary_to_list(NodeSpecB)),
    Instances = string:strip(binary_to_list(InstancesB)),
    add_nodes(string:tokens(NodeSpec, ":"), list_to_integer(Instances)).

update_config_table(Json) ->
    gen_server:cast(disco_server, {update_config_table,
        lists:flatten([parse_row(R) || R <- Json])}).

get_config_table() ->
    case file:read_file(os:getenv("DISCO_MASTER_CONFIG")) of
        {ok, Config} -> ok;
        {error, enoent} ->
            Config = "[]"
    end,
    Json = mochijson2:decode(Config),
    update_config_table(Json),
    {ok, Json}.

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
