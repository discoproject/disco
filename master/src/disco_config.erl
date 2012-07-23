-module(disco_config).
-behaviour(gen_server).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").

-export([start_link/0]).
-export([get_config_table/0, save_config_table/1,
         blacklist/1, whitelist/1,
         gc_blacklist/1, gc_whitelist/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type hostinfo_line() :: [binary(),...].
-type raw_hosts() :: [[hostinfo_line()]].
-type host_info() :: {host(), non_neg_integer()}.
-type config() :: [{binary(), [binary(),...]}].

-export_type([host_info/0]).

% ===================================================================
%% API functions

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Disco config starts"),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

-spec get_config_table() -> {ok, raw_hosts()}.
get_config_table() ->
    gen_server:call(?MODULE, get_config_table).

-spec save_config_table(raw_hosts()) -> {ok | error, binary()}.
save_config_table(Json) ->
    gen_server:call(?MODULE, {save_config_table, Json}).

-spec blacklist(host()) -> ok.
blacklist(Host) ->
    gen_server:call(?MODULE, {blacklist, Host}).

-spec whitelist(host()) -> ok.
whitelist(Host) ->
    gen_server:call(?MODULE, {whitelist, Host}).

-spec gc_blacklist(host()) -> ok.
gc_blacklist(Host) ->
    gen_server:call(?MODULE, {gc_blacklist, Host}).

-spec gc_whitelist(host()) -> ok.
gc_whitelist(Host) ->
    gen_server:call(?MODULE, {gc_whitelist, Host}).

%% ===================================================================
%% gen_server callbacks

-type state() :: undefined.

-spec init(_) -> gs_init().
init(_Args) ->
    {ok, undefined}.

-type host_op() :: whitelist | blacklist | gc_whitelist | gc_blacklist.
-type host_op_msg() :: {host_op(), host()}.
-spec handle_call(host_op_msg(), from(), state()) -> gs_reply(ok);
                 (get_config_table, from(), state()) -> gs_reply({ok, raw_hosts()});
                 ({save_config_table, raw_hosts()}, from(), state()) ->
                         gs_reply({ok | error, binary()}).
handle_call(get_config_table, _, S) ->
    {reply, do_get_config_table(), S};

handle_call({save_config_table, RawHosts}, _, S) ->
    {reply, do_save_config_table(RawHosts), S};

handle_call({blacklist, Host}, _, S) ->
    {reply, do_blacklist(Host), S};

handle_call({whitelist, Host}, _, S) ->
    {reply, do_whitelist(Host), S};

handle_call({gc_blacklist, Host}, _, S) ->
    {reply, do_gc_blacklist(Host), S};

handle_call({gc_whitelist, Host}, _, S) ->
    {reply, do_gc_whitelist(Host), S}.

-spec handle_cast(term(), state()) -> gs_noreply().
handle_cast(_, S) ->
    {noreply, S}.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(_, S) ->
    {noreply, S}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, _State) ->
    lager:warning("Disco config dies: ~p", [Reason]).

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% internal functions

-spec expand_range(nonempty_string(), nonempty_string()) -> [host()].
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

-spec update_config_table([host_info()], [host()], [host()]) -> ok.
update_config_table(HostInfo, Blacklist, GCBlacklist) ->
    disco_server:update_config_table(HostInfo, Blacklist, GCBlacklist).

-spec get_full_config() -> config().
get_full_config() ->
    case file:read_file(disco:get_setting("DISCO_MASTER_CONFIG")) of
        {ok, Json} ->
            ok;
        {error, enoent} ->
            Json = "[]"
    end,
    % Backward compatibility for older configs, updated whenever a new
    % field gets added to the config.
    case mochijson2:decode(Json) of
        {struct, Body} ->
            case proplists:is_defined(<<"gc_blacklist">>, Body) of
                true -> Body;
                false -> [{<<"gc_blacklist">>, []}] ++ Body
            end;
        L when is_list(L) -> [{<<"hosts">>, L},
                              {<<"blacklist">>, []},
                              {<<"gc_blacklist">>, []}]
    end.

-spec get_raw_hosts(config()) -> raw_hosts().
get_raw_hosts(Config) ->
    proplists:get_value(<<"hosts">>, Config).

-spec get_host_info(raw_hosts()) -> [host_info()].
get_host_info(RawHosts) ->
    lists:flatten([parse_row(R) || R <- RawHosts]).

-spec get_expanded_hosts(raw_hosts()) -> [host()].
get_expanded_hosts(RawHosts) ->
    {Hosts, _Cores} = lists:unzip(get_host_info(RawHosts)),
    Hosts.

-spec get_blacklist(config(), blacklist | gc_blacklist) -> [host()].
get_blacklist(Config, blacklist) ->
    get_blacklist(proplists:get_value(<<"blacklist">>, Config));
get_blacklist(Config, gc_blacklist) ->
    get_blacklist(proplists:get_value(<<"gc_blacklist">>, Config)).

get_blacklist(Blacklist) ->
    [binary_to_list(B) || B <- Blacklist].

-spec make_config(raw_hosts(), [host()], [host()]) -> config().
make_config(RawHosts, Blacklist, GCBlacklist) ->
    BinarizeList = fun(L) -> [list_to_binary(B) || B <- L] end,
    RawBlacklist = BinarizeList(Blacklist),
    RawGCBlacklist = BinarizeList(GCBlacklist),
    [{<<"hosts">>, RawHosts},
     {<<"blacklist">>, RawBlacklist},
     {<<"gc_blacklist">>, RawGCBlacklist}].

-spec make_blacklist([host()], [host()]) -> [host()].
make_blacklist(Hosts, Prospects) ->
    lists:usort([P || P <- Prospects, lists:member(P, Hosts)]).

-spec do_get_config_table() -> {ok, raw_hosts()}.
do_get_config_table() ->
    Config = get_full_config(),
    RawHosts = get_raw_hosts(Config),
    Blacklist = get_blacklist(Config, blacklist),
    GCBlacklist = get_blacklist(Config, gc_blacklist),
    update_config_table(get_host_info(RawHosts), Blacklist, GCBlacklist),
    {ok, RawHosts}.

-spec do_save_config_table(raw_hosts()) -> {ok | error, binary()}.
do_save_config_table(RawHosts) ->
    ParsedConfig =
        try
            {get_host_info(RawHosts), get_expanded_hosts(RawHosts)}
        catch
            _:_ ->
                lager:warning("Disco config: error parsing ~p", [RawHosts]),
                parse_error
        end,
    case ParsedConfig of
        {HostInfo, Hosts} ->
            {Hosts, _Cores} = lists:unzip(HostInfo),
            Sorted = lists:sort(Hosts),
            USorted = lists:usort(Hosts),
            if
                length(Sorted) =:= length(USorted) ->
                    % Retrieve and update old blacklist
                    OldConfig = get_full_config(),
                    OldBL = get_blacklist(OldConfig, blacklist),
                    OldGCBL = get_blacklist(OldConfig, gc_blacklist),
                    BL = make_blacklist(Hosts, OldBL),
                    GCBL = make_blacklist(Hosts, OldGCBL),
                    Config = make_config(RawHosts, BL, GCBL),
                    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                                         mochijson2:encode({struct, Config})),
                    update_config_table(HostInfo, BL, GCBL),
                    {ok, <<"table saved!">>};
                true ->
                    {error, <<"duplicate nodes">>}
            end;
        parse_error ->
            {error, <<"invalid config">>}
    end.

-spec do_blacklist(host()) -> ok.
do_blacklist(Host) ->
    OldConfig = get_full_config(),
    RawHosts = get_raw_hosts(OldConfig),
    NewBlacklist = make_blacklist(get_expanded_hosts(RawHosts),
                                  [Host | get_blacklist(OldConfig, blacklist)]),
    GCBlacklist = get_blacklist(OldConfig, gc_blacklist),
    NewConfig = make_config(RawHosts, NewBlacklist, GCBlacklist),
    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                         mochijson2:encode({struct, NewConfig})),
    disco_server:manual_blacklist(Host, true).

-spec do_whitelist(host()) -> ok.
do_whitelist(Host) ->
    OldConfig = get_full_config(),
    RawHosts = get_raw_hosts(OldConfig),
    NewBlacklist = make_blacklist(get_expanded_hosts(RawHosts),
                                  get_blacklist(OldConfig, blacklist) -- [Host]),
    GCBlacklist = get_blacklist(OldConfig, gc_blacklist),
    NewConfig = make_config(RawHosts, NewBlacklist, GCBlacklist),
    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                         mochijson2:encode({struct, NewConfig})),
    disco_server:manual_blacklist(Host, false).

-spec do_gc_blacklist(host()) -> ok.
do_gc_blacklist(Host) ->
    OldConfig = get_full_config(),
    RawHosts = get_raw_hosts(OldConfig),
    Blacklist = get_blacklist(OldConfig, blacklist),
    NewGCBlacklist = make_blacklist(get_expanded_hosts(RawHosts),
                                    [Host | get_blacklist(OldConfig, gc_blacklist)]),
    NewConfig = make_config(RawHosts, Blacklist, NewGCBlacklist),
    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                         mochijson2:encode({struct, NewConfig})),
    disco_server:gc_blacklist(NewGCBlacklist).

-spec do_gc_whitelist(host()) -> ok.
do_gc_whitelist(Host) ->
    OldConfig = get_full_config(),
    RawHosts = get_raw_hosts(OldConfig),
    Blacklist = get_blacklist(OldConfig, blacklist),
    NewGCBlacklist = make_blacklist(get_expanded_hosts(RawHosts),
                                    get_blacklist(OldConfig, gc_blacklist) -- [Host]),

    NewConfig = make_config(RawHosts, Blacklist, NewGCBlacklist),
    ok = file:write_file(os:getenv("DISCO_MASTER_CONFIG"),
                         mochijson2:encode({struct, NewConfig})),
    disco_server:gc_blacklist(NewGCBlacklist).
