
-module(garbage_collect).
-export([remove_map_results/1, remove_job/1, remove_dir/1, move_results/1]).

-define(RESULTFS_TIMEOUT, 300000). % 5min

spawn_remote([], _) -> ok;
spawn_remote([Url|Urls], F) when is_binary(Url) ->
    spawn_remote([binary_to_list(Url)|Urls], F);
spawn_remote([Url|Urls], F) when is_list(Url) ->
    ["dir:", Node, _, Pref, JobName, File] = string:tokens(Url, "/"),
    JobRoot = filename:join([Node, Pref, JobName]),
    SName = disco_worker:slave_name(Node),
    case net_adm:ping(SName) of 
        pong -> spawn(SName, fun () -> F(File, JobRoot, Node) end);
        _ -> ok
    end,
    spawn_remote(Urls, F).

% Functions below need to resort to os:cmd() instead of the build-in file
% module, since it is not usable on the slave nodes due to IO redirection.

remove_dir(Dir) ->
    spawn(fun() -> os:cmd("rm -Rf " ++ Dir) end).

remove_job(Urls) ->
    spawn_remote(Urls, fun (_, JobRoot, Node) ->
        Root = os:getenv("DISCO_ROOT"),
        error_logger:info_report({"Deleting all job files at", Node, JobRoot}),
        remove_dir(filename:join([Root, "data", JobRoot])),
        remove_dir(filename:join([Root, "temp", JobRoot]))
    end).

remove_map_results(Urls) ->
    spawn_remote(Urls, fun (File, JobRoot, _) ->
        Root = os:getenv("DISCO_ROOT"),
        lists:foreach(fun(F) ->
            C = "rm -Rf " ++
                filename:join([Root, "data", JobRoot, F]),
            os:cmd(C)
        end, parse_dir(File, JobRoot))
    end).

move_results(Urls) ->
    Parent = self(),
    spawn_remote(Urls, fun (File, JobRoot, Node) ->
        Root = os:getenv("DISCO_ROOT"),
        Src = filename:join([Root, "temp", JobRoot]),
        Dst = filename:join([Root, "data", JobRoot]),
        os:cmd("mkdir -p " ++ Dst ++ "/oob"),
        lists:foreach(fun(F) ->
            SrcF = filename:join(Src, F),
            X = os:cmd("mv " ++ SrcF ++ " " ++ Dst),
            if X =/= [] ->
                Parent ! {resultfs_error, Node, X};
            true -> ok
            end
        end, parse_dir(File, JobRoot)),
        os:cmd("mv " ++ Src ++ "/oob/* " ++ Dst ++ "/oob"),
        Parent ! {resultfs_node_ok, Node}
    end),
    wait_nodes(length(Urls)).

wait_nodes(0) -> ok;
wait_nodes(N) ->
    receive
        {resultfs_node_ok, _} ->
            wait_nodes(N - 1);
        {resultfs_error, Node, Error} ->
            {error, Node, Error};
        _ ->
            wait_nodes(N)
    after ?RESULTFS_TIMEOUT ->
        timeout
    end.

parse_dir(File, JobRoot) ->
    Root = os:getenv("DISCO_ROOT"),
    case string:tokens(File, ":") of
        [B, N] -> disco_config:expand_range(B, N);
        [Index] ->
            IFile = filename:join([Root, "temp", JobRoot, File]),
            [Index|string:tokens(os:cmd("cat " ++ IFile), "\n")]
    end.
