-module(disco_config).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([get_config_table/0, save_config_table/1, blacklist/1, whitelist/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ===================================================================
%% API functions

start_link() ->
    error_logger:info_report([{"Disco config starts"}]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

stop() ->
    gen_server:call(?MODULE, stop).

-spec get_config_table() -> {'ok', term()}.
get_config_table() ->
    gen_server:call(?MODULE, get_config_table).

-spec save_config_table(term()) -> {'ok' | 'error', binary()}.
save_config_table(Json) ->
    gen_server:call(?MODULE, {save_config_table, Json}).

-spec blacklist(nonempty_string()) -> 'ok'.
blacklist(Host) ->
    gen_server:call(?MODULE, {blacklist, Host}).

-spec whitelist(nonempty_string()) -> 'ok'.
whitelist(Host) ->
    gen_server:call(?MODULE, {whitelist, Host}).

%% ===================================================================
%% gen_server callbacks

init(_Args) ->
    {ok, undefined}.

handle_call(get_config_table, _, S) ->
    {reply, do_get_config_table(), S};

handle_call({save_config_table, Json}, _, S) ->
    {reply, do_save_config_table(Json), S};

handle_call({blacklist, Host}, _, S) ->
    {reply, do_blacklist(Host), S};

handle_call({whitelist, Host}, _, S) ->
    {reply, do_whitelist(Host), S}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) ->
    {noreply, S}.

terminate(Reason, _State) ->
    error_logger:warning_report({"Disco config dies", Reason}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% internal functions

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
    [{nonempty_string(), integer()}] | {nonempty_string(), integer()}.
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
    Config = lists:flatten([parse_row(R) || R <- Json]),
    disco_server:update_config_table(Config).

-spec do_get_config_table() -> {'ok', [[binary(), ...]]}.
do_get_config_table() ->
    case file:read_file(os:getenv("DISCO_MASTER_CONFIG")) of
        {ok, Config} -> ok;
        {error, enoent} ->
            Config = "[]"
    end,
    Json = mochijson2:decode(Config),
    update_config_table(Json),
    {ok, Json}.

-spec do_save_config_table([[binary(), ...]]) -> {'error' | 'ok', binary()}.
do_save_config_table(Json) ->
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

-spec do_blacklist(nonempty_string()) -> 'ok'.
do_blacklist(Host) ->
    disco_server:blacklist(Host, manual).

-spec do_whitelist(nonempty_string()) -> 'ok'.
do_whitelist(Host) ->
    disco_server:whitelist(Host, any).
