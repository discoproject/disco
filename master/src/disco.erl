-module(disco).

-export([get_setting/1,
         has_setting/1,
         settings/0,
         host/1,
         name/1,
         slave_name/0,
         slave_node/1,
         slave_safe/1,
         oob_name/1,
         hexhash/1,
         jobhome/1,
         jobhome/2,
         joburl/2,
         data_root/1,
         data_path/2,
         debug_flags/1,
         cluster_in_a_box_mode/0,
         disco_url_path/1,
         enum/1,
         format/2,
         format_time/1,
         format_time/4,
         format_time_since/1,
         make_dir/1,
         ensure_dir/1,
         is_file/1,
         is_dir/1]).

-include_lib("kernel/include/file.hrl").
-include("disco.hrl").

-define(MILLISECOND, 1000).
-define(SECOND, (1000 * ?MILLISECOND)).
-define(MINUTE, (60 * ?SECOND)).
-define(HOUR, (60 * ?MINUTE)).

-spec get_setting(nonempty_string()) -> string().
get_setting(SettingName) ->
    case os:getenv(SettingName) of
        false ->
            lager:warning("Required setting ~p missing!", [SettingName]),
            exit(["Must specify ", SettingName]);
        Val ->
            Val
    end.

-spec has_setting(nonempty_string()) -> boolean().
has_setting(SettingName) ->
    case os:getenv(SettingName) of
        false -> false;
        ""    -> false;
        _Val  -> true
    end.

-spec settings() -> [nonempty_string()].
settings() ->
    [T || T <- string:tokens(get_setting("DISCO_SETTINGS"), ","),
	  has_setting(T)].

-spec host(node()) -> host_name().
host(Node) ->
    string:sub_word(atom_to_list(Node), 2, $@).

-spec name(node()) -> host_name().
name(Node) ->
    string:sub_word(atom_to_list(Node), 1, $@).

-spec slave_name() -> nonempty_string().
slave_name() ->
    get_setting("DISCO_NAME") ++ "_slave".

-spec slave_node(host_name()) -> node().
slave_node(Host) ->
    list_to_atom(slave_name() ++ "@" ++ Host).

-spec slave_safe(host_name()) -> 'false' | node().
slave_safe(Host) ->
    try list_to_existing_atom(slave_name() ++ "@" ++ Host)
    catch _:_ -> false
    end.

-spec oob_name(jobname()) -> nonempty_string().
oob_name(JobName) ->
    lists:flatten(["disco:job:oob:", JobName]).

-spec hexhash(nonempty_string() | binary()) -> nonempty_string().
hexhash(Path) when is_list(Path) ->
    hexhash(list_to_binary(Path));
hexhash(Path) ->
    <<Hash:8, _/binary>> = erlang:md5(Path),
    lists:flatten(io_lib:format("~2.16.0b", [Hash])).

-spec jobhome(jobname()) -> nonempty_string().
jobhome(JobName) ->
    jobhome(JobName, get_setting("DISCO_MASTER_ROOT")).

-spec jobhome(jobname(), nonempty_string()) -> nonempty_string().
jobhome(JobName, Root) ->
    filename:join([Root, hexhash(JobName), JobName]).

-spec joburl(host_name(), jobname()) -> nonempty_string().
joburl(Host, JobName) ->
    filename:join(["disco", Host, hexhash(JobName), JobName]).

-spec data_root(node() | nonempty_string()) -> nonempty_string().
data_root(Node) when is_atom(Node) ->
    data_root(host(Node));
data_root(Host) ->
    filename:join(get_setting("DISCO_DATA"), Host).

-spec data_path(node() | host_name(), nonempty_string()) -> nonempty_string().
data_path(NodeOrHost, Path) ->
    filename:join(data_root(NodeOrHost), Path).

-spec debug_flags(nonempty_string()) -> [term()].
debug_flags(Server) ->
    case os:getenv("DISCO_DEBUG") of
        "trace" ->
            Root = disco:get_setting("DISCO_MASTER_ROOT"),
            [{debug, [{log_to_file,
                filename:join(Root, Server ++ "_trace.log")}]}];
        _ -> []
    end.

-spec cluster_in_a_box_mode() -> boolean().
cluster_in_a_box_mode() ->
    case os:getenv("DISCO_CLUSTER_IN_A_BOX") of
        false -> false;
        _ -> true
    end.

-spec disco_url_path(file:filename()) -> [nonempty_string()].
disco_url_path(Url) ->
    {match, [Path]} = re:run(Url,
                             ".*?://.*?/disco/(.*)",
                             [{capture, all_but_first, list}]),
    Path.

-spec enum([term()]) -> [{non_neg_integer(), term()}].
enum(List) ->
    lists:zip(lists:seq(0, length(List) - 1), List).

-spec format(nonempty_string(), [term()]) -> nonempty_string().
format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

-spec format_time(integer()) -> nonempty_string().
format_time(Ms) when is_integer(Ms) ->
    format_time((Ms rem ?SECOND) div ?MILLISECOND,
                (Ms rem ?MINUTE) div ?SECOND,
                (Ms rem ?HOUR) div ?MINUTE,
                (Ms div ?HOUR)).

-spec format_time(integer(), integer(), integer(), integer()) ->
                         nonempty_string().
format_time(Ms, Second, Minute, Hour) ->
    format("~B:~2.10.0B:~2.10.0B.~3.10.0B", [Hour, Minute, Second, Ms]).

-spec format_time_since(erlang:timestamp()) -> nonempty_string().
format_time_since(Time) ->
    format_time(timer:now_diff(now(), Time)).

-spec make_dir(file:filename()) -> {'ok', file:filename()} | {'error', _}.
make_dir(Dir) ->
    case ensure_dir(Dir) of
        ok ->
            case prim_file:make_dir(Dir) of
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

% based on ensure_dir() in /usr/lib/erlang/lib/stdlib-1.17/src/filelib.erl

-spec ensure_dir(file:filename()) -> 'ok' | {'error', file:posix()}.
ensure_dir("/") ->
    ok;
ensure_dir(F) ->
    Dir = filename:dirname(F),
    case is_dir(Dir) of
        true ->
            ok;
        false ->
            case ensure_dir(Dir) of
                ok ->
                    case prim_file:make_dir(Dir) of
                        {error,eexist}=EExist ->
                            case is_dir(Dir) of
                                true ->
                                    ok;
                                false ->
                                    EExist
                            end;
                        Err ->
                            Err
                    end;
                Err ->
                    Err
            end
    end.

-spec is_dir(file:filename()) -> boolean().
is_dir(Dir) ->
    case prim_file:read_file_info(Dir) of
        {ok, #file_info{type=directory}} ->
            true;
        _ ->
            false
    end.

-spec is_file(file:filename()) -> boolean().
is_file(Dir) ->
    case prim_file:read_file_info(Dir) of
        {ok, #file_info{type=regular}} ->
            true;
        {ok, #file_info{type=directory}} ->
            true;
        _ ->
            false
    end.
