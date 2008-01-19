
-module(disco_config).
-export([get_config_table/0, save_config_table/1]).

config_file() ->
        {ok, Val} = application:get_env(disco_config),
        Val.

add_nodes([FirstNode, Max], Instances) ->
        Len = string:len(FirstNode),
        FieldLen = string:len(Max),
        MaxNum = list_to_integer(Max),
        Name = string:sub_string(FirstNode, 1, Len - FieldLen),
        MinNum = list_to_integer(
                string:sub_string(FirstNode, Len - FieldLen + 1)),
        Format = lists:flatten(io_lib:format("~s~~.~w.0w", [Name, FieldLen])),

        [{lists:flatten(io_lib:fwrite(Format, [I])), Instances} ||
                I <- lists:seq(MinNum, MaxNum)];

add_nodes([Node], Instances) -> {Node, Instances}.
        
parse_row([<<>>, <<>>]) -> none;
parse_row([NodeSpecB, InstancesB]) ->
        NodeSpec = string:strip(binary_to_list(NodeSpecB)),
        Instances = string:strip(binary_to_list(InstancesB)),
        add_nodes(string:tokens(NodeSpec, ":"), list_to_integer(Instances)).

update_config_table(Json) ->
        ok = gen_server:call(disco_server, {update_config_table, 
                lists:flatten([parse_row(R) || R <- Json])}).

get_config_table() ->
        error_logger:info_report([{"Opening config file"}]),
        {ok, Config} = file:read_file(config_file()),
        {ok, Json, _Rest} = json:decode(Config),
        update_config_table(Json),
        {ok, Json}.

save_config_table(Json) ->
        update_config_table(Json),
        ok = file:write_file(config_file(), json:encode(Json)),
        {ok, <<"table saved!">>}.


                        
