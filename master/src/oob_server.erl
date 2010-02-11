
-module(oob_server).
-behaviour(gen_server).

-define(CACHE_SIZE, 100).

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

start_link() ->
    error_logger:info_report([{"OOB server starts"}]),
    case gen_server:start_link({local, oob_server}, oob_server, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

init(_Args) ->
    {ok, []}.

handle_cast({store, JobName, Node, Keys}, Cache) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    FName = filename:join([Root, disco_server:jobhome(JobName), "oob"]),
    {ok, F} = file:open(FName, [raw, append]),
    file:write(F, [[Node, " ", Key, " ", Path, "\n"] ||
        {Key, Path} <- Keys]),
    file:close(F),
    % invalidate old cache entry
    case cache_find(list_to_binary(JobName), Cache, []) of
        not_found -> {noreply, Cache};
        {_, [_|C]} -> {noreply, C}
    end.

handle_call({fetch, JobName, Key}, _, Cache) ->
    {OobKeys, Cache1} = cache_find(JobName, Cache),
    if OobKeys == not_found ->
        {reply, error, Cache1};
    true ->
        {reply, dict:find(Key, OobKeys), Cache1}
    end;

handle_call({list, JobName}, _, Cache) ->
    {OobKeys, Cache1} = cache_find(JobName, Cache),
    if OobKeys == not_found ->
        {reply, error, Cache1};
    true ->
        {reply, {ok, dict:fetch_keys(OobKeys)}, Cache1}
    end.

cache_find(JobName, Cache) ->
    case cache_find(JobName, Cache, []) of
        {E, NCache} -> {E, NCache};
        not_found -> cache_add(JobName, Cache)
    end.

cache_find(_, [], _) -> not_found;
cache_find(JobName, [{Key, E}|Cache], C) when JobName == Key ->
    {E, [{Key, E}|C] ++ Cache};
cache_find(JobName, [X|Cache], C) ->
    cache_find(JobName, Cache, [X|C]).

% LRU cache
cache_add(JobName, Cache) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    FName = filename:join([Root, disco_server:jobhome(JobName), "oob"]),
    case file:read_file(FName) of
        {ok, Data} ->
            OobKeys = parse_file(binary_to_list(Data)),
            C = if length(Cache) == ?CACHE_SIZE ->
                lists:sublist(Cache, ?CACHE_SIZE - 1);
            true ->
                Cache
            end,
            {OobKeys, [{JobName, OobKeys}|C]};
        _ -> {not_found, Cache}
    end.

parse_file(Data) ->
    dict:from_list(lists:map(fun(L) ->
        [N, K, P] = [list_to_binary(X) || X <- string:tokens(L, " ")],
        {K, {N, P}}
    end, string:tokens(Data, "\n"))).

% callback stubs

handle_info(_, S) -> {noreply, S}.

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
