-module(disco_config).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([get_config_table/0, save_config_table/1, blacklist/1, whitelist/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type hostinfo_line() :: [binary(),...].
-type raw_hosts() :: [[hostinfo_line()]].
-type host_info() :: {nonempty_string(), non_neg_integer()}.
-type config() :: [{binary(), [binary(),...]}].

-export_type([host_info/0]).

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

-spec get_config_table() -> {'ok', raw_hosts()}.
get_config_table() ->
    gen_server:call(?MODULE, get_config_table).

-spec save_config_table(raw_hosts()) -> {'ok' | 'error', binary()}.
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

handle_call({save_config_table, RawHosts}, _, S) ->
    {reply, do_save_config_table(RawHosts), S};

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

-spec add_nodes([nonempty_string(),...], integer()) ->
    [host_info()] | host_info().
add_nodes([FirstNode, Max], Instances) ->
    [{N, Instances} || N <- expand_range(FirstNode, Max)];
add_nodes([Node], Instances) -> {Node, Instances}.

-spec parse_row(hostinfo_line()) -> [host_info()] | host_info().
parse_row([NodeSpecB, InstancesB]) ->
    NodeSpec = string:strip(binary_to_list(NodeSpecB)),
    Instances = string:strip(binary_to_list(InstancesB)),
    add_nodes(string:tokens(NodeSpec, ":"), list_to_integer(Instances)).

-spec update_config_table([host_info()], [nonempty_string()]) -> 'ok'.
update_config_table(HostInfo, Blacklist) ->
    disco_server:update_config_table(HostInfo, Blacklist).

-spec get_full_config() -> config().
get_full_config() ->
    case file:read_file(disco:get_setting("DISCO_MASTER_CONFIG")) of
        {ok, Json} ->
            ok;
        {error, enoent} ->
            Json = "[]"
    end,
    case mochijson2:decode(Json) of
        {struct, Body} -> Body;
        L when is_list(L) -> [{<<"hosts">>, L}, {<<"blacklist">>, []}]
    end.

-spec get_raw_hosts(config()) -> raw_hosts().
get_raw_hosts(Config) ->
    proplists:get_value(<<"hosts">>, Config).

-spec get_host_info(raw_hosts()) -> [host_info()].
get_host_info(RawHosts) ->
    lists:flatten([parse_row(R) || R <- RawHosts]).

-spec get_expanded_hosts(raw_hosts()) -> [nonempty_string()].
get_expanded_hosts(RawHosts) ->
    {Hosts, _Cores} = lists:unzip(get_host_info(RawHosts)),
    Hosts.

-spec get_blacklist(config()) -> [nonempty_string()].
get_blacklist(Config) ->
    BL = proplists:get_value(<<"blacklist">>, Config),
    lists:map(fun(B) -> binary_to_list(B) end, BL).

-spec make_config(raw_hosts(), [nonempty_string()]) -> config().
make_config(RawHosts, Blacklist) ->
    RawBlacklist = lists:map(fun(B) -> list_to_binary(B) end, Blacklist),
    [{<<"hosts">>, RawHosts}, {<<"blacklist">>, RawBlacklist}].

-spec make_blacklist([nonempty_string()], [nonempty_string()]) ->
    [nonempty_string()].
make_blacklist(Hosts, Prospects) ->
    lists:usort(lists:filter(fun(P) -> lists:member(P, Hosts) end, Prospects)).

-spec do_get_config_table() -> {'ok', raw_hosts()}.
do_get_config_table() ->
    Config = get_full_config(),
    RawHosts = get_raw_hosts(Config),
    Blacklist = get_blacklist(Config),
    update_config_table(get_host_info(RawHosts), Blacklist),
    {ok, RawHosts}.

-spec do_save_config_table(raw_hosts()) -> {'ok' | 'error', binary()}.
do_save_config_table(RawHosts) ->
    ParsedConfig =
        try
            {get_host_info(RawHosts), get_expanded_hosts(RawHosts)}
        catch
            _:_ ->
                error_logger:warning_report({"Disco config: parse error",
                                             RawHosts}),
                parse_error
        end,
    case ParsedConfig of
        {HostInfo, Hosts} ->
            {Hosts, _Cores} = lists:unzip(HostInfo),
            Sorted = lists:sort(Hosts),
            USorted = lists:usort(Hosts),
            if
                length(Sorted) == length(USorted) ->
                    % Retrieve and update old blacklist
                    OldBL = get_blacklist(get_full_config()),
                    NewBL = make_blacklist(Hosts, OldBL),
                    Config = make_config(RawHosts, NewBL),
                    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                                         mochijson2:encode({struct, Config})),
                    update_config_table(HostInfo, NewBL),
                    {ok, <<"table saved!">>};
                true ->
                    {error, <<"duplicate nodes">>}
            end;
        parse_error ->
            {error, <<"invalid config">>}
    end.

-spec do_blacklist(nonempty_string()) -> 'ok'.
do_blacklist(Host) ->
    OldConfig = get_full_config(),
    RawHosts = get_raw_hosts(OldConfig),
    NewBlacklist = make_blacklist(get_expanded_hosts(RawHosts),
                                  [Host | get_blacklist(OldConfig)]),
    NewConfig = make_config(RawHosts, NewBlacklist),
    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                         mochijson2:encode({struct, NewConfig})),
    disco_server:manual_blacklist(Host, true).

-spec do_whitelist(nonempty_string()) -> 'ok'.
do_whitelist(Host) ->
    OldConfig = get_full_config(),
    RawHosts = get_raw_hosts(OldConfig),
    NewBlacklist = make_blacklist(get_expanded_hosts(RawHosts),
                                  get_blacklist(OldConfig) -- [Host]),
    NewConfig = make_config(RawHosts, NewBlacklist),
    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                         mochijson2:encode({struct, NewConfig})),
    disco_server:manual_blacklist(Host, false).
