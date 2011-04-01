-module(jobpack).

-export([jobfile/1,
         exists/1,
         extract/2,
         extracted/1,
         read/1,
         save/2,
         copy/2,
         jobinfo/1,
         jobenvs/1]).

-include("disco.hrl").

-define(MAGIC, 16#d5c0).
-define(VERSION, 16#0001).
-define(COPY_BUFFER_SIZE, 1048576).

dict({struct, List}) ->
    dict:from_list(List).

find(Key, Dict) ->
    case catch dict:find(Key, Dict) of
        {ok, Field} ->
            Field;
        error ->
            throw(disco:format("jobpack missing key '~s'", [Key]))
    end.

find(Key, Dict, Default) ->
    case catch dict:find(Key, Dict) of
        {ok, Field} ->
            Field;
        error ->
            Default
    end.

jobfile(JobHome) ->
    filename:join(JobHome, "jobfile").

exists(JobHome) ->
    filelib:is_file(jobfile(JobHome)).

extract(JobPack, JobHome) ->
    JobZip = jobzip(JobPack),
    case discozip:extract(JobZip, [{cwd, JobHome}]) of
        {ok, Files} ->
            ok = prim_file:write_file(filename:join(JobHome, ".jobhome"), <<"">>),
            Files;
        {error, Reason} ->
            throw({"Couldn't extract jobhome", JobHome, Reason})
    end.

extracted(JobHome) ->
    filelib:is_file(filename:join(JobHome, ".jobhome")).

read(JobHome) ->
    JobFile = jobfile(JobHome),
    case prim_file:read_file(JobFile) of
        {ok, JobPack} ->
            JobPack;
        {error, Reason} ->
            throw({"Couldn't read jobfile", JobFile, Reason})
    end.

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

copy({Src, Size}, JobHome) ->
    TmpFile = tempname(JobHome),
    JobFile = jobfile(JobHome),
    {ok, Dst} = prim_file:open(TmpFile, [raw, binary, write]),
    ok = copy(Src, Dst, Size),
    ok = prim_file:close(Dst),
    file:close(Src),
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

jobinfo(JobPack) when is_binary(JobPack) ->
    jobinfo(jobdict(JobPack));
jobinfo(JobDict) ->
    Scheduler = dict(find(<<"scheduler">>, JobDict)),
    {validate_prefix(find(<<"prefix">>, JobDict)),
     #jobinfo{inputs = find(<<"input">>, JobDict),
              worker = find(<<"worker">>, JobDict),
              owner = find(<<"owner">>, JobDict),
              map = find(<<"map?">>, JobDict, false),
              reduce = find(<<"reduce?">>, JobDict, false),
              nr_reduce = find(<<"nr_reduces">>, JobDict),
              max_cores = find(<<"max_cores">>, Scheduler, 1 bsl 31),
              force_local = find(<<"force_local">>, Scheduler, false),
              force_remote = find(<<"force_remote">>, Scheduler, false)}}.

jobdict(<<?MAGIC:16/big,
         _Version:16/big,
         JobDictOffset:32/big,
         JobEnvsOffset:32/big,
         _/binary>> = JobPack) ->
    JobDictLength = JobEnvsOffset - JobDictOffset,
    <<_:JobDictOffset/bytes, JobDict:JobDictLength/bytes, _/binary>> = JobPack,
    dict(mochijson2:decode(JobDict)).

jobenvs(<<?MAGIC:16/big,
         _Version:16/big,
         _JobDictOffset:32/big,
         JobEnvsOffset:32/big,
         JobHomeOffset:32/big,
         _/binary>> = JobPack) ->
    JobEnvsLength = JobHomeOffset - JobEnvsOffset,
    <<_:JobEnvsOffset/bytes, JobEnvs:JobEnvsLength/bytes, _/binary>> = JobPack,
    dict(mochijson2:decode(JobEnvs)).

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

validate_prefix(Prefix) when is_binary(Prefix)->
    validate_prefix(binary_to_list(Prefix));
validate_prefix(Prefix) ->
    case string:chr(Prefix, $/) + string:chr(Prefix, $.) of
        0 ->
            Prefix;
        _ ->
            throw("invalid prefix")
    end.
