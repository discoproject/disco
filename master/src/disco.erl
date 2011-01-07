-module(disco).

-export([get_setting/1,
         has_setting/1,
         settings/0,
         host/1,
         node/1,
         node_name/0,
         node_safe/1,
         oob_name/1,
         jobhome/1,
         jobhome/2,
         debug_flags/1,
         disco_url_path/1,
         format/2,
         format_time/1,
         format_time/4,
         format_time_since/1,
         make_dir/1]).

-define(MILLISECOND, 1000).
-define(SECOND, (1000 * ?MILLISECOND)).
-define(MINUTE, (60 * ?SECOND)).
-define(HOUR, (60 * ?MINUTE)).

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

-spec settings() -> [string()].
settings() ->
    lists:filter(fun has_setting/1,
                 string:tokens(get_setting("DISCO_SETTINGS"), ",")).

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

jobhome(JobName) ->
    jobhome(JobName, get_setting("DISCO_MASTER_ROOT")).

jobhome(JobName, Root) when is_list(JobName) ->
    jobhome(list_to_binary(JobName), Root);
jobhome(JobName, Root) ->
    <<Hash:8, _/binary>> = erlang:md5(JobName),
    filename:join([Root,
                   io_lib:format("~2.16.0b", [Hash]),
                   binary_to_list(JobName)]).

debug_flags(Server) ->
    case os:getenv("DISCO_DEBUG") of
        "trace" ->
            Root = disco:get_setting("DISCO_MASTER_ROOT"),
            [{debug, [{log_to_file,
                filename:join(Root, Server ++ "_trace.log")}]}];
        _ -> []
    end.

disco_url_path(Url) ->
    {match, [Path]} = re:run(Url,
                             ".*?://.*?/disco/(.*)",
                             [{capture, all_but_first, list}]),
    Path.

format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

format_time(Ms) when is_integer(Ms) ->
    format_time((Ms rem ?SECOND) div ?MILLISECOND,
                (Ms rem ?MINUTE) div ?SECOND,
                (Ms rem ?HOUR) div ?MINUTE,
                (Ms div ?HOUR)).

format_time(Ms, Second, Minute, Hour) ->
    format("~B:~2.10.0B:~2.10.0B.~3.10.0B", [Hour, Minute, Second, Ms]).

format_time_since(Time) ->
    format_time(timer:now_diff(now(), Time)).

make_dir(Dir) ->
    case filelib:ensure_dir(Dir) of
        ok ->
            case file:make_dir(Dir) of
                ok ->
                    {ok, Dir};
                {error, eexist} ->
                    {ok, Dir};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
