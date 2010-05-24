-module(temp_gc).

-include_lib("kernel/include/file.hrl").

-export([start_link/6]).

-define(GC_INTERVAL, 600000).

start_link(Master, EventServer, DdfsMaster, DataRoot, Node, GCAfter) ->
    put(master, Master),
    put(events, EventServer),
    put(ddfs, DdfsMaster),
    put(root, filename:join(DataRoot, Node)),
    put(gcafter, GCAfter),
    loop().

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

process_dir([], _Purged, _Active) -> ok;
process_dir([Dir|R], Purged, Active) ->
    Path = filename:join(get(root), Dir),
    {ok, Jobs} = prim_file:list_dir(Path),
    [process_job(filename:join(Path, Job), Purged) ||
        Job <- Jobs, ifdead(Job, Active)],
    process_dir(R, Purged, Active).

ifdead(Job, Active) ->
    not gb_sets:is_member(list_to_binary(Job), Active).

process_job(JobPath, Purged) ->
    case prim_file:read_file_info(JobPath) of
        {ok, #file_info{type = directory, mtime = TStamp}} ->
            T = calendar:datetime_to_gregorian_seconds(TStamp),
            Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            Job = filename:basename(JobPath),
            IsPurged = gb_sets:is_member(list_to_binary(Job), Purged),
            GCAfter = get(gcafter),
            if IsPurged; Now - T > GCAfter ->
                ddfs:delete(get(ddfs), disco:oob_name(Job)),
                os:cmd("rm -Rf " ++ Job);
            true ->
                ok
            end;
        _ ->
            ok
    end.

