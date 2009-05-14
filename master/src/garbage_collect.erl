
-module(garbage_collect).
-export([remove_map_results/1, remove_job/1, remove_dir/1]).

spawn_remote([], _) -> ok;
spawn_remote([Url|Urls], F) ->
        {Node, S} = case string:tokens(Url, "/") of
                ["dir:", N, _|S0] -> {N, lists:flatten(S0)};
                ["disco:", N, S0] -> {N, S0}
        end,
        SName = disco_worker:slave_name(Node),
        case net_adm:ping(SName) of 
                pong -> spawn(SName, fun () -> F(S, Url) end);
                _ -> ok
        end,
        spawn_remote(Urls, F).

% Functions below need to resort to os:cmd() instead of the build-in file
% module, since it is not usable on the slave nodes due to IO redirection.

remove_dir(Dir) ->
        spawn(fun() -> os:cmd("rm -Rf " ++ Dir) end).

remove_map_results(Urls) ->
        spawn_remote(Urls, fun (JobName, Url) ->
                Root = os:getenv("DISCO_ROOT"),
                error_logger:info_report({"Deleting map results at", Url}),
                os:cmd("rm -Rf " ++
                        filename:join([Root, "data", JobName, "map-*"]))
        end).

remove_job(Urls) ->
        spawn_remote(Urls, fun (JobName, Url) ->
                Root = os:getenv("DISCO_ROOT"),
                error_logger:info_report({"Deleting all job files at", Url}),
                remove_dir(filename:join([Root, "data", JobName]))
        end).
        
