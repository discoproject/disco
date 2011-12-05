-module(jobpack).

-export([valid/1,
         jobfile/1,
         exists/1,
         extract/2,
         extracted/1,
         read/1,
         save/2,
         copy/2,
         jobinfo/1]).

-include_lib("kernel/include/file.hrl").
-include("disco.hrl").

-define(MAGIC, 16#d5c0).
-define(VERSION, 16#0001).
-define(HEADER_SIZE, 128).
-define(COPY_BUFFER_SIZE, 1048576).

-spec dict({'struct', [{term(), term()}]}) -> dict().
dict({struct, List}) ->
    dict:from_list(List).

-spec find(binary(), dict()) -> term().
find(Key, Dict) ->
    case catch dict:find(Key, Dict) of
        {ok, Field} ->
            Field;
        error ->
            throw({error, disco:format("jobpack missing key '~s'", [Key])})
    end.

-spec find(binary(), dict(), T) -> T.
find(Key, Dict, Default) ->
    case catch dict:find(Key, Dict) of
        {ok, Field} ->
            Field;
        error ->
            Default
    end.

-spec jobfile(nonempty_string()) -> nonempty_string().
jobfile(JobHome) ->
    filename:join(JobHome, "jobfile").

-spec exists(nonempty_string()) -> boolean().
exists(JobHome) ->
    disco:is_file(jobfile(JobHome)).

-spec extract(binary(), nonempty_string()) -> [file:name()].
extract(JobPack, JobHome) ->
    JobZip = jobzip(JobPack),
    case discozip:extract(JobZip, [{cwd, JobHome}]) of
        {ok, Files} ->
            ok = prim_file:write_file(filename:join(JobHome, ".jobhome"), <<"">>),
            ok = ensure_executable_worker(JobPack, JobHome),
            Files;
        {error, Reason} ->
            exit({"Couldn't extract jobhome", JobHome, Reason})
    end.

-spec extracted(nonempty_string()) -> boolean().
extracted(JobHome) ->
    disco:is_file(filename:join(JobHome, ".jobhome")).

ensure_executable_worker(JobPack, JobHome) ->
    Worker = find(<<"worker">>, jobdict(JobPack)),
    Path = filename:join(JobHome, binary_to_list(Worker)),
    prim_file:write_file_info(Path, #file_info{mode = 8#755}).

-spec read(nonempty_string()) -> binary().
read(JobHome) ->
    JobFile = jobfile(JobHome),
    case prim_file:read_file(JobFile) of
        {ok, JobPack} ->
            JobPack;
        {error, Reason} ->
            throw({"Couldn't read jobfile", JobFile, Reason})
    end.

-spec save(binary(), nonempty_string()) -> nonempty_string().
save(JobPack, JobHome) ->
    TmpFile = tempname(JobHome),
    JobFile = jobfile(JobHome),
    case prim_file:write_file(TmpFile, JobPack) of
        ok ->
            case prim_file:rename(TmpFile, JobFile) of
                ok ->
                    JobFile;
                {error, Reason} ->
                    throw({"Couldn't rename jobpack", TmpFile, Reason})
            end;
        {error, Reason} ->
            throw({"Couldn't save jobpack", TmpFile, Reason})
    end.

-spec copy({file:io_device(), non_neg_integer()}, nonempty_string()) ->
                  {'ok', nonempty_string()}.
copy({Src, Size}, JobHome) ->
    TmpFile = tempname(JobHome),
    JobFile = jobfile(JobHome),
    {ok, Dst} = prim_file:open(TmpFile, [raw, binary, write]),
    ok = copy(Src, Dst, Size),
    ok = prim_file:close(Dst),
    _ = file:close(Src),
    ok = prim_file:rename(TmpFile, JobFile),
    {ok, JobFile}.

copy(_Src, _Dst, 0) -> ok;
copy(Src, Dst, Left) ->
    {ok, Buf} = file:read(Src, ?COPY_BUFFER_SIZE),
    ok = prim_file:write(Dst, Buf),
    copy(Src, Dst, Left - size(Buf)).

tempname(JobHome) ->
    {MegaSecs, Secs, MicroSecs} = now(),
    filename:join(JobHome, disco:format("jobfile@~.16b:~.16b:~.16b",
                                        [MegaSecs, Secs, MicroSecs])).

-spec jobinfo(binary()) -> {nonempty_string(), jobinfo()}.
jobinfo(JobPack) ->
    JobDict = jobdict(JobPack),
    Scheduler = dict(find(<<"scheduler">>, JobDict)),
    {validate_prefix(find(<<"prefix">>, JobDict)),
     #jobinfo{jobenvs = jobenvs(JobPack),
              inputs = find(<<"input">>, JobDict),
              worker = find(<<"worker">>, JobDict),
              owner = find(<<"owner">>, JobDict),
              map = find(<<"map?">>, JobDict, false),
              reduce = find(<<"reduce?">>, JobDict, false),
              nr_reduce = find(<<"nr_reduces">>, JobDict),
              max_cores = find(<<"max_cores">>, Scheduler, 1 bsl 31),
              force_local = find(<<"force_local">>, Scheduler, false),
              force_remote = find(<<"force_remote">>, Scheduler, false)}}.

-spec jobdict(binary()) -> dict().
jobdict(<<?MAGIC:16/big,
         _Version:16/big,
         JobDictOffset:32/big,
         JobEnvsOffset:32/big,
         _/binary>> = JobPack) ->
    JobDictLength = JobEnvsOffset - JobDictOffset,
    <<_:JobDictOffset/bytes, JobDict:JobDictLength/bytes, _/binary>> = JobPack,
    dict(mochijson2:decode(JobDict)).

-spec jobenvs(binary()) -> [{nonempty_string(), string()}].
jobenvs(<<?MAGIC:16/big,
         _Version:16/big,
         _JobDictOffset:32/big,
         JobEnvsOffset:32/big,
         JobHomeOffset:32/big,
         _/binary>> = JobPack) ->
    JobEnvsLength = JobHomeOffset - JobEnvsOffset,
    <<_:JobEnvsOffset/bytes, JobEnvs:JobEnvsLength/bytes, _/binary>> = JobPack,
    {struct, Envs} = mochijson2:decode(JobEnvs),
    Envs.

-spec jobzip(binary()) -> binary().
jobzip(<<?MAGIC:16/big,
        _Version:16/big,
        _JobDictOffset:32/big,
        _JobEnvsOffset:32/big,
        JobHomeOffset:32/big,
        JobDataOffset:32/big,
        _/binary>> = JobPack) ->
    JobHomeLength = JobDataOffset - JobHomeOffset,
    <<_:JobHomeOffset/bytes, JobZip:JobHomeLength/bytes, _/binary>> = JobPack,
    JobZip.

-spec valid(binary()) -> 'ok' | {'error', term()}.
valid(<<?MAGIC:16/big,
        ?VERSION:16/big,
        JobDictOffset:32/big,
        JobEnvsOffset:32/big,
        JobHomeOffset:32/big,
        JobDataOffset:32/big,
        _/binary>> = JobPack)
  when JobDictOffset =:= ?HEADER_SIZE,
       JobEnvsOffset > JobDictOffset,
       JobHomeOffset >= JobEnvsOffset,
       JobDataOffset >= JobHomeOffset,
       size(JobPack) >= JobDataOffset ->
    case catch jobenvs(JobPack) of
        {'EXIT', _} -> {error, invalid_dicts_or_envs};
        {error, E} -> {error, E};
        _ -> ok
    end;
valid(_JobPack) -> {error, invalid_header}.

-spec validate_prefix(binary() | list()) -> nonempty_string().
validate_prefix(Prefix) when is_binary(Prefix)->
    validate_prefix(binary_to_list(Prefix));
validate_prefix(Prefix) ->
    case string:chr(Prefix, $/) + string:chr(Prefix, $.) of
        0 ->
            Prefix;
        _ ->
            throw({error, "invalid prefix"})
    end.
