-module(ddfs_util).
-export([is_valid_name/1, timestamp/0, timestamp/1, timestamp_to_time/1,
         ensure_dir/1, hashdir/5, safe_rename/2, format_timestamp/0,
         diskspace/1, fold_files/3, pack_objname/2, unpack_objname/1,
         choose_random/1, replace/3, startswith/2]).

-include_lib("kernel/include/file.hrl").

-include("config.hrl").

is_valid_name([]) -> false;
is_valid_name(Name) when length(Name) > ?NAME_MAX -> false;
is_valid_name(Name) ->
    Ok = ":@-_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    not lists:any(fun(C) -> string:chr(Ok, C) == 0 end, Name).

replace(Str, A, B) ->
    replace(lists:flatten(Str), A, B, []).
replace([], _, _, L) ->
    lists:reverse(L);
replace([C|R], A, B, L) when C == A ->
    replace(R, A, B, [B|L]);
replace([C|R], A, B, L) ->
    replace(R, A, B, [C|L]).

startswith(B, Prefix) when size(B) < size(Prefix) ->
    false;
startswith(B, Prefix) ->
    {Head, _} = split_binary(B, size(Prefix)),
    Head =:= Prefix.

timestamp() -> timestamp(now()).
timestamp({X0, X1, X2}) ->
    io_lib:format("~.16b-~.16b-~.16b", [X0, X1, X2]).

timestamp_to_time(T) ->
    list_to_tuple([erlang:list_to_integer(X, 16) ||
        X <- string:tokens(lists:flatten(T), "-")]).

pack_objname(Name, T) ->
    list_to_binary([Name, "$", timestamp(T)]).

unpack_objname(Obj) when is_binary(Obj) ->
    unpack_objname(binary_to_list(Obj));
unpack_objname(Obj) ->
    [Name, Tstamp] = string:tokens(Obj, "$"),
    {list_to_binary(Name), timestamp_to_time(Tstamp)}.

ensure_dir(Dir) ->
    case prim_file:make_dir(Dir) of
        ok -> ok;
        {error, eexist} -> ok;
        E -> E
    end.

format_timestamp() ->
    {Date, Time} = calendar:now_to_local_time(now()),
    DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
    TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
    list_to_binary([DateStr, TimeStr]).

hashdir(Name, Node, Mode, Root, Vol) ->
    <<D0:8, _/binary>> = erlang:md5(Name),
    [D1] = io_lib:format("~.16b", [D0]),
    Dir = [if length(D1) == 1 -> "0"; true -> "" end, D1],
    Path = filename:join([Vol, Mode, Dir]),
    Url = list_to_binary(["disco://", Node, "/ddfs/", Path, "/", Name]),
    Local = filename:join(Root, Path),
    {ok, Local, Url}.

safe_rename(Src, Dst) ->
    case prim_file:read_file_info(Dst) of
        {error, enoent} ->
            case prim_file:write_file_info(Src,
                    #file_info{mode = ?FILE_MODE}) of
                ok ->
                    case prim_file:rename(Src, Dst) of
                        ok -> ok;
                        {error, E} -> {error, {rename_failed, E}}
                    end;
                {error, E} -> {error, {chmod_failed, E}}
            end;
        _ -> {error, file_exists}
    end.

diskspace(Path) ->
    case lists:reverse(string:tokens(os:cmd(["df -k ", Path]), "\n\t ")) of
        [_, _, Free|_] ->
            case catch list_to_integer(Free) of
                X when is_integer(X) -> {ok, X};
                _ -> {error, invalid_path}
            end;
        _ ->
            {error, invalid_output}
    end.

fold_files(Dir, Fun, Acc0) ->
    {ok, L} = prim_file:list_dir(Dir),
    lists:foldl(fun(F, Acc) ->
        Path = filename:join(Dir, F),
        case prim_file:read_file_info(Path) of
            {ok, #file_info{type = directory}} ->
                fold_files(Path, Fun, Acc);
            _ ->
                Fun(F, Dir, Acc)
        end
    end, Acc0, L).

choose_random(L) ->
    lists:nth(random:uniform(length(L)), L).

    
