
-module(garbage_collect).
-export([remove_map_results/1, remove_job/1, remove_dir/1]).

spawn_remote([], _) -> ok;
spawn_remote([Url|Urls], F) ->
        {Node, S} = case string:tokens(Url, "/") of
                ["dir:", N, _, S0] -> {N, S0};
                ["disco:", N, S0] -> {N, S0}
        end,
        spawn(disco_worker:slave_name(Node), fun () -> F(S, Url) end),
        spawn_remote(Urls, F).

% Assuming that file:rename() is atomic, as it should be,
% this function should work ok. The first call is guaranteed to succeed.
remove_dir(Dir) ->
        spawn(fun() ->
                NDir = Dir ++ ".delete",
                ok = file:rename(Dir, NDir),
                {ok, Files} = file:list_dir(NDir),
                [file:delete(filename:join([NDir, F])) || F <- Files],
                file:del_dir(NDir)
        end).

% This function can NOT be safely executed in parallel.
remove_map_results(Urls) ->
        spawn_remote(lists:usort(Urls), fun (JobName, Url) ->
                Root = os:getenv("DISCO_ROOT"),
                error_logger:info_report({"Deleting map results at", Url}),
                Files = filelib:wildcard(filename:join(
                        [Root, "data", JobName, "map-*"])),
                [file:delete(F) || F <- Files]
        end).

% This function can be safely executed in parallel.
remove_job(Urls) ->
        spawn_remote(Urls, fun (JobName, Url) ->
                Root = os:getenv("DISCO_ROOT"),
                error_logger:info_report({"Deleting all job files at", Url}),
                remove_dir(filename:join([Root, "data", JobName]))
        end).
        
