-module(jobpack).

% Metadata extraction.
-export([jobinfo/1, valid/1]).
% Jobpack file management.
-export([jobfile/1, exists/1, extract/2, extracted/1, read/1, save/2, copy/2]).

-export_type([jobpack/0]).

-include_lib("kernel/include/file.hrl").

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-define(MAGIC, 16#d5c0).
-define(VERSION_1, 16#0001).
-define(VERSION_2, 16#0002).
-define(HEADER_SIZE, 128).
-define(COPY_BUFFER_SIZE, 1048576).
-define(DEFAULT_MAX_CORE, 1 bsl 31).

-type jobpack() :: binary().

% Metadata utilities.

-spec grouping(binary())         -> label_grouping().
grouping(<<"split">>)            -> split;
grouping(<<"group_node_label">>) -> group_node_label;
grouping(<<"group_node">>)       -> group_node;
grouping(<<"group_label">>)      -> group_label;
grouping(<<"group_all">>)        -> group_all;
grouping(G) ->
    throw({error, disco:format("invalid grouping '~p'", [G])}).

% Lookup utilities.
-type job_dict() :: disco_dict(binary(), term()).

-spec dict({struct, [{term(), term()}]}) -> job_dict().
dict({struct, List}) ->
    dict:from_list(List).

-spec find(binary(), job_dict()) -> term().
find(Key, Dict) ->
    case dict:find(Key, Dict) of
        {ok, Field} -> Field;
        error -> throw({error, disco:format("jobpack missing key '~s'", [Key])})
    end.

-spec find(binary(), job_dict(), T) -> T.
find(Key, Dict, Default) ->
    case dict:find(Key, Dict) of
        {ok, Field} -> Field;
        error       -> Default
    end.

% Metadata extraction.

-spec jobinfo(jobpack()) -> {jobname(), jobinfo()}.
jobinfo(JobPack) ->
    {Version, JobDict} = jobdict(JobPack),
    {Prefix, JobInfo} = core_jobinfo(JobPack, JobDict),
    VersionInfo = version_info(Version, JobDict),
    {Prefix, fixup_versions(JobInfo, VersionInfo)}.

-spec jobdict(jobpack()) -> {non_neg_integer(), job_dict()}.
jobdict(<<?MAGIC:16/big,
          Version:16/big,
          JobDictOffset:32/big,
          JobEnvsOffset:32/big,
          _/binary>> = JobPack) ->
    JobDictLength = JobEnvsOffset - JobDictOffset,
    <<_:JobDictOffset/bytes, JobDict:JobDictLength/bytes, _/binary>> = JobPack,
    {Version, dict(mochijson2:decode(JobDict))}.

-spec core_jobinfo(jobpack(), job_dict()) -> {jobname(), jobinfo()}.
core_jobinfo(JobPack, JobDict) ->
    Prefix  = find(<<"prefix">>, JobDict),
    SaveResults = find(<<"save_results">>, JobDict, false),
    SaveInfo = find(<<"save_info">>, JobDict, "ddfs"),
    JobInfo = #jobinfo{jobenvs = jobenvs(JobPack),
                       worker  = find(<<"worker">>, JobDict),
                       owner   = find(<<"owner">>, JobDict),
                       save_info = validate_save_info(SaveInfo),
                       save_results = validate_save_results(SaveResults)},
    {validate_prefix(Prefix), JobInfo}.

-spec jobenvs(jobpack()) -> [{nonempty_string(), string()}].
jobenvs(<<?MAGIC:16/big,
          _Version:16/big,
          _JobDictOffset:32/big,
          JobEnvsOffset:32/big,
          JobHomeOffset:32/big,
          _/binary>> = JobPack) ->
    JobEnvsLength = JobHomeOffset - JobEnvsOffset,
    <<_:JobEnvsOffset/bytes, JobEnvs:JobEnvsLength/bytes, _/binary>> = JobPack,
    {struct, Envs} = mochijson2:decode(JobEnvs),
    [{binary_to_list(K), binary_to_list(V)} || {K, V} <- Envs].

-spec jobzip(jobpack()) -> binary().
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

% Backward compatibility management.
-type job_inputs1() :: [url() | [url()]].
-record(ver1_info, {force_local  :: boolean(),
                    force_remote :: boolean(),
                    inputs :: job_inputs1(),
                    max_cores :: cores(),
                    nr_reduce :: non_neg_integer(),
                    map    :: boolean(),
                    reduce_shuffle :: boolean(),
                    reduce :: boolean()}).
-record(ver2_info, {pipeline     = []                :: pipeline(),
                    schedule     = #task_schedule{}  :: task_schedule(),
                    inputs       = []                :: [data_input()]}).

-spec version_info(non_neg_integer(), job_dict()) -> #ver1_info{} | #ver2_info{}.
version_info(?VERSION_1, JobDict) ->
    Scheduler = dict(find(<<"scheduler">>, JobDict)),
    #ver1_info{inputs = find(<<"input">>, JobDict),
               map    = find(<<"map?">>, JobDict, false),
               reduce = find(<<"reduce?">>, JobDict, false),
               reduce_shuffle = find(<<"reduce_shuffle?">>, JobDict, false),
               nr_reduce = find(<<"nr_reduces">>, JobDict),
               max_cores = find(<<"max_cores">>, Scheduler, ?DEFAULT_MAX_CORE),
               force_local  = find(<<"force_local">>, Scheduler, false),
               force_remote = find(<<"force_remote">>, Scheduler, false)};
version_info(?VERSION_2, JobDict) ->
    Pipeline = find(<<"pipeline">>, JobDict),
    Inputs   = find(<<"inputs">>, JobDict),
    Scheduler = dict(find(<<"scheduler">>, JobDict)),
    Max = find(<<"max_cores">>, Scheduler, ?DEFAULT_MAX_CORE),
    #ver2_info{schedule = #task_schedule{max_cores = Max},
               pipeline = validate_pipeline(Pipeline),
               inputs   = validate_inputs(Inputs)};
version_info(V, _JobDict) ->
    throw({error, disco:format("unsupported version '~p'", [V])}).

-spec fixup_versions(jobinfo(), #ver1_info{} | #ver2_info{}) -> jobinfo().
fixup_versions(JobInfo, #ver1_info{} = V1) ->
    {I, P} = job_from_ver1(V1),
    S = schedule_option1(V1),
    OI = disco:enum(I),
    JobInfo#jobinfo{inputs = OI, pipeline = P, schedule = S};
fixup_versions(JobInfo, #ver2_info{inputs = I, pipeline = P, schedule = S}) ->
    OI = disco:enum(I),
    JobInfo#jobinfo{inputs = OI, pipeline = P, schedule = S}.

% Validation

-spec valid(jobpack()) -> ok | {error, term()}.
valid(<<?MAGIC:16/big,
        _Version:16/big,
        JobDictOffset:32/big,
        JobEnvsOffset:32/big,
        JobHomeOffset:32/big,
        JobDataOffset:32/big,
        _/binary>> = JobPack)
  when JobDictOffset =:= ?HEADER_SIZE,
       JobEnvsOffset > JobDictOffset,
       JobHomeOffset >= JobEnvsOffset,
       JobDataOffset >= JobHomeOffset,
       byte_size(JobPack) >= JobDataOffset ->
    try  _ = jobinfo(JobPack),
         _ = jobenvs(JobPack),
         ok
    catch
        {error, _E} = Err -> Err;
        K:E -> {error, disco:format("invalid payload: ~p:~p", [K, E])}
    end;
valid(_JobPack) -> {error, "invalid header"}.

-spec validate_prefix(binary() | list()) -> nonempty_string().
validate_prefix(Prefix) when is_binary(Prefix)->
    validate_prefix(binary_to_list(Prefix));
validate_prefix(Prefix) ->
    case string:chr(Prefix, $/) + string:chr(Prefix, $.) of
        0 -> Prefix;
        _ -> throw({error, "invalid prefix"})
    end.

-spec validate_pipeline(list()) -> pipeline().
validate_pipeline(P) ->
    Spec = {hom_array, {array, [string, string, boolean]}},
    case json_validator:validate(Spec, P) of
        {error, E} ->
            Msg = disco:format("Invalid job pipeline: ~s",
                               json_validator:error_msg(E)),
            throw({error, Msg});
        ok ->
            % Ensure stages are unique.
            Stages = [S || [S | _] <- P],
            StageSet = gb_sets:from_list(Stages),
            case gb_sets:size(StageSet) =/= length(Stages) of
                true  -> throw({error, repeated_pipeline_stages});
                false -> ok
            end,
            [{S, grouping(G), Concurrent} || [S | [G | [Concurrent]]] <- P]
    end.

-spec validate_inputs(list()) -> [data_input()].
validate_inputs(Inputs) ->
    Spec = {hom_array, {array, [integer, integer, {hom_array, string}]}},
    case json_validator:validate(Spec, Inputs) of
        {error, E} ->
            lager:warning("Invalid inputs in jobpack: ~s",
                          json_validator:error_msg(E)),
            throw({error, invalid_job_inputs});
        _I ->
            [{data, {L, Sz, [{U, disco:preferred_host(U)} || U <- Urls]}}
             || [L, Sz, Urls] <- Inputs]
    end.

-spec validate_save_info(binary() | list()) -> string().
validate_save_info(S) when is_binary(S)->
    validate_save_info(binary_to_list(S));

validate_save_info(S) ->
    case lists:flatlength(S) < 4 of
        true ->
            lager:warning("Invalid save_info in jobpack: ~s is too short", S),
            throw({error, invalid_job_save_info});
        false ->
            case string:substr(S, 1, 4) of
                "hdfs" -> S;
                "ddfs" -> S;
                _ -> lager:warning("Invalid save_info in jobpack: ~s", S),
                     throw({error, invalid_job_save_info})
            end
    end.

-spec validate_save_results(term()) -> boolean().
validate_save_results(S) ->
    case json_validator:validate(boolean, S) of
        {error, E} ->
            lager:warning("Invalid save_results in jobpack: ~s",
                          json_validator:error_msg(E)),
            throw({error, invalid_job_save_results});
        _ ->
            S
    end.

% Jobpack file management.

-spec jobfile(path()) -> path().
jobfile(JobHome) ->
    filename:join(JobHome, "jobfile").

-spec exists(path()) -> boolean().
exists(JobHome) ->
    disco:is_file(jobfile(JobHome)).

-spec extract(jobpack(), path()) -> [file:name()].
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

-spec extracted(path()) -> boolean().
extracted(JobHome) ->
    disco:is_file(filename:join(JobHome, ".jobhome")).

ensure_executable_worker(JobPack, JobHome) ->
    {_Version, JobDict} = jobdict(JobPack),
    Worker = find(<<"worker">>, JobDict),
    Path = filename:join(JobHome, binary_to_list(Worker)),
    prim_file:write_file_info(Path, #file_info{mode = 8#755}).

-spec read(path()) -> binary().
read(JobHome) ->
    JobFile = jobfile(JobHome),
    case prim_file:read_file(JobFile) of
        {ok, JobPack} ->
            JobPack;
        {error, Reason} ->
            throw({"Couldn't read jobfile", JobFile, Reason})
    end.

-spec save(jobpack(), path()) -> {ok, path()} | {error, term()}.
save(JobPack, JobHome) ->
    TmpFile = tempname(JobHome),
    JobFile = jobfile(JobHome),
    case prim_file:write_file(TmpFile, JobPack) of
        ok ->
            case prim_file:rename(TmpFile, JobFile) of
                ok ->
                    {ok, JobFile};
                {error, Reason} ->
                    {error, disco:format("Couldn't rename jobpack to ~p: ~p",
                                         [JobFile, Reason])}
            end;
        {error, Reason} ->
            {error, disco:format("Couldn't save jobpack ~p: ~p",
                                 [TmpFile, Reason])}
    end.

-spec copy({file:io_device(), non_neg_integer(), pid()}, path()) -> {ok, path()}.
copy({Src, Size, Sender}, JobHome) ->
    CopyResult = (catch try_copy(Src, Size, JobHome)),
    _ = file:close(Src),
    Sender ! done,
    CopyResult.
try_copy(Src, Size, JobHome) ->
    TmpFile = tempname(JobHome),
    JobFile = jobfile(JobHome),
    {ok, Dst} = prim_file:open(TmpFile, [raw, binary, write]),
    ok = copy_bytes(Src, Dst, Size),
    ok = prim_file:close(Dst),
    ok = prim_file:rename(TmpFile, JobFile),
    {ok, JobFile}.

copy_bytes(_Src, _Dst, 0) -> ok;
copy_bytes(Src, Dst, Left) ->
    {ok, Buf} = file:read(Src, ?COPY_BUFFER_SIZE),
    ok = prim_file:write(Dst, Buf),
    copy_bytes(Src, Dst, Left - byte_size(Buf)).

tempname(JobHome) ->
    {MegaSecs, Secs, MicroSecs} = now(),
    filename:join(JobHome, disco:format("jobfile@~.16b:~.16b:~.16b",
                                        [MegaSecs, Secs, MicroSecs])).


% Compatibility utility: construct a pipeline from a job packet of
% Disco 0.4.2 or earlier.

-spec job_from_ver1(#ver1_info{}) -> {[data_input()],
                                      pipeline() | unsupported_job}.
job_from_ver1(#ver1_info{inputs = JI, map = M, reduce = R, nr_reduce = NR, reduce_shuffle = RS}) ->
    job_from_ver1(JI, M, R, RS, NR).

job_from_ver1(JobInputs, Map, Reduce, RS, Nr_reduce) ->
    Inputs = task_inputs1(JobInputs),
    Pipeline = pipeline1(Map, Reduce, RS, Nr_reduce),
    {Inputs, Pipeline}.

% Parse job input urls into pipeline format.
-spec task_inputs1([url() | [url()]]) -> [data_input()].
task_inputs1(Inputs) ->
    [task_input(I) || I <- Inputs].
-spec task_input(url() | [url()]) -> data_input().
task_input(<<"dir://", _/binary>> = Url) ->
    {dir, {disco:preferred_host(Url), Url, []}};
task_input(<<_/binary>> = Url) ->
    {data, {0, 0, input_replicas1(Url)}};
task_input([<<"dir://", _/binary>> = Url]) ->
    {dir, {disco:preferred_host(Url), Url, []}};
task_input([<<"dir://", _/binary>> = Url | _] = Inputs) ->
    % We do not output replicated dir:// urls, so we don't handle them
    % as input.
    lager:warning("Skipping redundant dir:// inputs, taking only first of ~p",
                  [Inputs]),
    task_input(Url);
task_input(Urls) ->
    {data, {0, 0, input_replicas1(Urls)}}.

-spec input_replicas1(url() | [url()]) -> [data_replica()].
input_replicas1(Input) when is_binary(Input) ->
    % Single replica
    [{Input, disco:preferred_host(Input)}];
input_replicas1(Reps) when is_list(Reps) ->
    % Replica set
    [{R, disco:preferred_host(R)} || R <- Reps].

-spec pipeline1(boolean(), boolean(), boolean(), non_neg_integer())
              -> pipeline() | unsupported_job.
pipeline1(false, false, _RS, _NR) ->
    [];
pipeline1(false, true, _RS, 1) ->
    [{?REDUCE, group_all, false}];
pipeline1(false, true, true, _NR) ->
    [{?REDUCE, group_label, false}, {?REDUCE_SHUFFLE, group_node, false}];
pipeline1(false, true, false, _NR) ->
    [{?REDUCE, group_label, false}];
pipeline1(true, false, _RS, _NR) ->
    [{?MAP, split, false}, {?MAP_SHUFFLE, group_node, false}];
pipeline1(true, true, _RS, 1) ->
    [{?MAP, split, false},
     {?MAP_SHUFFLE, group_node, false},
     {?REDUCE, group_all, false}];
pipeline1(true, true, true, _NR) ->
    [{?MAP, split, false}, {?MAP_SHUFFLE, group_node, false},
     {?REDUCE, group_label, false}, {?REDUCE_SHUFFLE, group_node, false}];
pipeline1(true, true, false, _NR) ->
    [{?MAP, split, false}, {?MAP_SHUFFLE, group_node, false},
     {?REDUCE, group_label, false}].

-spec schedule_option1(#ver1_info{}) -> task_schedule().
schedule_option1(#ver1_info{force_local = Local, force_remote = Remote, max_cores = Max}) ->
    S = schedule_option1(Local, Remote),
    S#task_schedule{max_cores = Max}.
% Prefer Local if both Local and Remote are set.
schedule_option1(true, _) -> #task_schedule{locality = local};
schedule_option1(_, true) -> #task_schedule{locality = remote};
schedule_option1(_, _)    -> #task_schedule{}.
