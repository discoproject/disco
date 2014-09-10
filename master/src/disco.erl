-module(disco).

-export([get_setting/1, has_setting/1, settings/0]).
-export([host/1, name/1]).
-export([slave_name/1, slave_node/1, slave_safe/1, slave_for_url/1]).
-export([jobhome/1, jobhome/2, joburl/2, joburl_to_localpath/1]).
-export([data_root/1, data_path/2, ddfs_root/2]).
-export([local_cluster/0, preferred_host/1, dir_to_url/1]).
-export([enum/1, enum/2, hexhash/1, large_hexhash/1, oob_name/1, debug_flags/1]).
-export([format/2, format_time/1, format_time/4, format_time_since/1]).
-export([make_dir/1, ensure_dir/1, is_file/1, is_dir/1]).

-include_lib("kernel/include/file.hrl").

-include("common_types.hrl").
-include("disco.hrl").

-define(MILLISECOND, 1000).
-define(SECOND, (1000 * ?MILLISECOND)).
-define(MINUTE, (60 * ?SECOND)).
-define(HOUR, (60 * ?MINUTE)).

-define(MAX_FORMAT_LENGTH, 8192).

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

-spec name(node()) -> host().
name(Node) ->
    string:sub_word(atom_to_list(Node), 1, $@).

default_slave_name() ->
    get_setting("DISCO_NAME") ++ "_slave".

-define(DLMTR_STR, "_at_").

box_slave_name(Host) ->
    default_slave_name() ++ ?DLMTR_STR ++ Host.

-spec slave_name(host()) -> nonempty_string().
slave_name(Host) ->
    case local_cluster() of
        false -> default_slave_name();
        true  -> box_slave_name(Host)
    end.

-spec slave_node(host()) -> node().
slave_node(Host) ->
    case local_cluster() of
        false -> list_to_atom(default_slave_name() ++ "@" ++ Host);
        true  -> list_to_atom(box_slave_name(Host) ++ "@localhost")
    end.

-spec slave_safe(host()) -> false | node().
slave_safe(Host) ->
    case local_cluster() of
        false -> list_to_existing_atom(default_slave_name() ++ "@" ++ Host);
        true  -> list_to_existing_atom(box_slave_name(Host) ++ "@localhost")
    end.

-spec slave_for_url(url()) -> false | node().
slave_for_url(Url) ->
    {_S, Host, _P, _Q, _F} = mochiweb_util:urlsplit(binary_to_list(Url)),
    slave_safe(Host).

-spec host(node()) -> host().
host(Node) ->
    case local_cluster() of
        false ->
            string:sub_word(atom_to_list(Node), 2, $@);
        true  ->
            Prefix = string:sub_word(atom_to_list(Node), 1, $@),
            Regex = default_slave_name() ++ ?DLMTR_STR ++ "(.*)",
            Capture = [{capture, all_but_first, list}],
            {match, [Host]} = re:run(Prefix, Regex, Capture),
            Host
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


-spec large_hexhash(nonempty_string()) -> nonempty_string().
large_hexhash(Path) ->
    <<Hash:128>> = erlang:md5(Path),
    lists:flatten(io_lib:format("~32.16.0b", [Hash])).

-spec jobhome(jobname()) -> path().
jobhome(JobName) ->
    jobhome(JobName, get_setting("DISCO_MASTER_ROOT")).

-spec jobhome(jobname(), path()) -> path().
jobhome(JobName, Root) ->
    filename:join([Root, hexhash(JobName), JobName]).

-spec joburl(host(), jobname()) -> path().
joburl(Host, JobName) ->
    filename:join(["disco", Host, hexhash(JobName), JobName]).

-spec joburl_to_localpath(url()) -> path().
joburl_to_localpath(Url) ->
    {_S, _H, Path, _Q, _F} = mochiweb_util:urlsplit(binary_to_list(Url)),
    ["/", "disco" | P] = filename:split(Path),
    filename:join([get_setting("DISCO_DATA") | P]).

-spec data_root(node() | nonempty_string()) -> path().
data_root(Node) when is_atom(Node) ->
    data_root(host(Node));
data_root(Host) ->
    filename:join(get_setting("DISCO_DATA"), Host).

-spec data_path(node() | host(), path()) -> path().
data_path(NodeOrHost, Path) ->
    filename:join(data_root(NodeOrHost), Path).

-spec ddfs_root(path(), host()) -> path().
ddfs_root(DdfsRoot, Host) ->
    case local_cluster() of
        false -> DdfsRoot;
        true  -> filename:join(DdfsRoot, Host)
    end.

-spec debug_flags(nonempty_string()) -> [term()].
debug_flags(Server) ->
    case os:getenv("DISCO_DEBUG") of
        "trace" ->
            Root = disco:get_setting("DISCO_MASTER_ROOT"),
            [{debug, [{log_to_file,
                       filename:join(Root, Server ++ "_trace.log")}]}];
        _ -> []
    end.

-spec local_cluster() -> boolean().
local_cluster() ->
    case os:getenv("DISCO_LOCAL_CLUSTER") of
        false -> false;
        _     -> true
    end.

-spec dir_to_url(url()) -> url().
dir_to_url(<<"dir://", _/binary>> = Dir) ->
    {_S, Host, P, Q, F} = mochiweb_util:urlsplit(binary_to_list(Dir)),
    Url = {"http", Host ++ ":" ++ get_setting("DISCO_PORT"), P, Q, F},
    mochiweb_util:urlunsplit(Url);
dir_to_url(<<_/binary>> = Url) ->
    Url.

-spec preferred_host(url()) -> url_host().
preferred_host(Url) ->
    Opts = [{capture, all_but_first, binary}],
    case re:run(Url, "^[a-zA-Z0-9]+://([^/:]*)", Opts) of
        {match, [Match]} -> binary_to_list(Match);
        nomatch          -> none
    end.

-spec enum([T]) -> [{non_neg_integer(), T}].
enum(List) ->
    enum(List, 0).

-spec enum([T], non_neg_integer()) -> [{non_neg_integer(), T}].
enum(List, Start) ->
    lists:zip(lists:seq(Start, Start + length(List) - 1), List).


-spec format(nonempty_string(), [term()]) -> nonempty_string().
format(Format, Args) ->
    lists:flatten(lager:safe_format(Format, Args, ?MAX_FORMAT_LENGTH)).

-spec format_time(integer()) -> nonempty_string().
format_time(Ms) when is_integer(Ms) ->
    format_time((Ms rem ?SECOND) div ?MILLISECOND,
                (Ms rem ?MINUTE) div ?SECOND,
                (Ms rem ?HOUR) div ?MINUTE,
                (Ms div ?HOUR)).

-spec format_time(integer(), integer(), integer(), integer())
                 -> nonempty_string().
format_time(Ms, Second, Minute, Hour) ->
    lists:flatten(io_lib:format("~B:~2.10.0B:~2.10.0B.~3.10.0B",
                                [Hour, Minute, Second, Ms])).

-spec format_time_since(erlang:timestamp()) -> nonempty_string().
format_time_since(Time) ->
    format_time(timer:now_diff(now(), Time)).

-spec make_dir(file:filename()) -> {ok, file:filename()} | {error, _}.
make_dir(Dir) ->
    case ensure_dir(Dir) of
        ok ->
            case prim_file:make_dir(Dir) of
                ok                   -> {ok, Dir};
                {error, eexist}      -> {ok, Dir};
                {error, _Reason} = E -> E
            end;
        {error, _Reason} = E ->
            E
    end.

% based on ensure_dir() in /usr/lib/erlang/lib/stdlib-1.17/src/filelib.erl

-spec ensure_dir(file:filename()) -> ok | {error, file:posix()}.
ensure_dir("/") -> ok;
ensure_dir(F) ->
    Dir = filename:dirname(F),
    case is_dir(Dir) of
        true  ->
            ok;
        false ->
            case ensure_dir(Dir) of
                ok -> case prim_file:make_dir(Dir) of
                          {error, eexist} = EExist ->
                              case is_dir(Dir) of
                                  true  -> ok;
                                  false -> EExist
                              end;
                          Err -> Err
                      end;
                Err -> Err
            end
    end.

-spec is_dir(file:filename()) -> boolean().
is_dir(Dir) ->
    case prim_file:read_file_info(Dir) of
        {ok, #file_info{type=directory}} -> true;
        _                                -> false
    end.

-spec is_file(file:filename()) -> boolean().
is_file(Dir) ->
    case prim_file:read_file_info(Dir) of
        {ok, #file_info{type=regular}}   -> true;
        {ok, #file_info{type=directory}} -> true;
        _                                -> false
    end.
