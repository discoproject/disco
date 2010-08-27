-module(disco).

-export([get_setting/1, has_setting/1,
         host/1, node/1, node_name/0, node_safe/1,
         oob_name/1, jobhome/1, debug_flags/1,
         format_time/1, settings/0, disco_url_path/1]).

-spec get_setting(string()) -> string().
get_setting(SettingName) ->
    case os:getenv(SettingName) of
        false ->
            error_logger:warning_report(
              {"Required setting", SettingName, "missing!"}),
            exit(["Must specify ", SettingName]);
        Val ->
            Val
    end.

-spec has_setting(string()) -> bool().
has_setting(SettingName) ->
    case os:getenv(SettingName) of
        false -> false;
        ""    -> false;
        _Val  -> true
    end.

-spec host(node()) -> string().
host(Node) ->
    string:sub_word(atom_to_list(Node), 2, $@).

-spec node(string()) -> node().
node(Host) ->
    list_to_atom(node_name() ++ "@" ++ Host).

-spec node_name() -> string().
node_name() ->
    get_setting("DISCO_NAME") ++ "_slave".

-spec node_safe(string()) -> 'false' | node().
node_safe(Host) ->
    case catch list_to_existing_atom(node_name() ++ "@" ++ Host) of
        {'EXIT', _} -> false;
        Node -> Node
    end.

-spec oob_name(string()) -> string().
oob_name(JobName) ->
    lists:flatten(["disco:job:oob:", JobName]).

-spec settings() -> [string()].
settings() ->
    lists:filter(fun has_setting/1,
                 string:tokens(get_setting("DISCO_SETTINGS"), ",")).

debug_flags(Server) ->
    case os:getenv("DISCO_DEBUG") of
        "trace" ->
            Root = disco:get_setting("DISCO_MASTER_ROOT"),
            [{debug, [{log_to_file,
                filename:join(Root, Server ++ "_trace.log")}]}];
        _ -> []
    end.

jobhome(JobName) when is_list(JobName) ->
    jobhome(list_to_binary(JobName));
jobhome(JobName) ->
    <<D0:8, _/binary>> = erlang:md5(JobName),
    [D1] = io_lib:format("~.16b", [D0]),
    Prefix = case D1 of [_] -> "0"; _ -> "" end,
    lists:flatten([Prefix, D1, "/", binary_to_list(JobName), "/"]).

format_time(T) ->
    MS = 1000,
    SEC = 1000 * MS,
    MIN = 60 * SEC,
    HOUR = 60 * MIN,
    D = timer:now_diff(now(), T),
    Ms = (D rem SEC) div MS,
    Sec = (D rem MIN) div SEC,
    Min = (D rem HOUR) div MIN,
    Hour = D div HOUR,
    lists:flatten(io_lib:format("~B:~2.10.0B:~2.10.0B.~3.10.0B",
        [Hour, Min, Sec, Ms])).

disco_url_path(Url) ->
    {match, [Path]} = re:run(Url,
                             ".*?://.*?/disco/(.*)",
                             [{capture, all_but_first, list}]),
    Path.

