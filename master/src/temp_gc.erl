-module(temp_gc).

-include_lib("kernel/include/file.hrl").

-export([start_link/6]).

-define(GC_INTERVAL, 600000).

-spec start_link(pid(), pid(), pid(), nonempty_string(), nonempty_string(),
    non_neg_integer()) -> no_return().
start_link(Master, EventServer, DdfsMaster, DataRoot, Host, GCAfter) ->
    case catch register(temp_gc, self()) of
        {'EXIT', {badarg, _}} ->
            exit(already_started);
        _ -> ok
    end,
    put(master, Master),
    put(events, EventServer),
    put(ddfs, DdfsMaster),
    put(root, filename:join(DataRoot, Host)),
    put(gcafter, GCAfter),
    loop().

-spec loop() -> no_return().
loop() ->
    case catch {gen_server:call(get(master), get_purged),
                gen_server:call(get(events), get_jobs)} of
        {{ok, Purged}, {ok, Jobs}} ->
            case prim_file:list_dir(get(root)) of
                {ok, Dirs} ->
                    Active = gb_sets:from_list(
                        [Name || {Name, active, _Start, _Pid} <- Jobs]),
                    process_dir(Dirs, gb_sets:from_ordset(Purged), Active);
                _ ->
                    % fresh install, try again after GC_INTERVAL
                    ok
            end;
        _ ->
            % master busy, try again after GC_INTERVAL
            ok
    end,
    timer:sleep(?GC_INTERVAL),
    loop().

-spec process_dir([string()], gb_set(), gb_set()) -> 'ok'.
process_dir([], _Purged, _Active) -> ok;
process_dir([Dir|R], Purged, Active) ->
    Path = filename:join(get(root), Dir),
    {ok, Jobs} = prim_file:list_dir(Path),
    [process_job(filename:join(Path, Job), Purged) ||
        Job <- Jobs, ifdead(Job, Active)],
    process_dir(R, Purged, Active).

-spec ifdead(string(), gb_set()) -> bool().
ifdead(Job, Active) ->
    not gb_sets:is_member(list_to_binary(Job), Active).

-spec process_job(string(), gb_set()) -> 'ok' | string().
process_job(JobPath, Purged) ->
    case prim_file:read_file_info(JobPath) of
        {ok, #file_info{type = directory, mtime = TStamp}} ->
            T = calendar:datetime_to_gregorian_seconds(TStamp),
            Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            Job = filename:basename(JobPath),
            IsPurged = gb_sets:is_member(list_to_binary(Job), Purged),
            GCAfter = get(gcafter),
            if IsPurged; Now - T > GCAfter ->
                ddfs:delete(get(ddfs), disco:oob_name(Job), internal),
                os:cmd("rm -Rf " ++ JobPath);
            true ->
                ok
            end;
        _ ->
            ok
    end.
