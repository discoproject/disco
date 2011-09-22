-module(temp_gc).

-include_lib("kernel/include/file.hrl").

-export([start_link/1]).

-define(GC_INTERVAL, 600000).

-spec start_link(pid()) -> no_return().
start_link(Master) ->
    case catch register(temp_gc, self()) of
        {'EXIT', {badarg, _}} ->
            exit(already_started);
        _ -> ok
    end,
    put(master, Master),
    loop().

-spec loop() -> no_return().
loop() ->
    case catch {get_purged(), get_jobs()} of
        {{ok, Purged}, {ok, Jobs}} ->
            case prim_file:list_dir(disco:data_root(node())) of
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
    flush(),
    loop().

% gen_server calls below may timeout, so we need to purge late replies
flush() ->
    receive
        _ ->
            flush()
    after 0 ->
        ok
    end.

ddfs_delete(Tag) ->
    ddfs:delete({ddfs_master, get(master)}, Tag, internal).

get_purged() ->
    gen_server:call({disco_server, get(master)}, get_purged).

get_jobs() ->
    gen_server:call({event_server, get(master)}, get_jobs).

-spec process_dir([string()], gb_set(), gb_set()) -> 'ok'.
process_dir([], _Purged, _Active) -> ok;
process_dir([Dir|R], Purged, Active) ->
    Path = disco:data_path(node(), Dir),
    {ok, Jobs} = prim_file:list_dir(Path),
    _ = [process_job(filename:join(Path, Job), Purged) ||
            Job <- Jobs, ifdead(Job, Active)],
    process_dir(R, Purged, Active).

-spec ifdead(string(), gb_set()) -> bool().
ifdead(Job, Active) ->
    not gb_sets:is_member(list_to_binary(Job), Active).

-spec process_job(string(), gb_set()) -> 'ok'.
process_job(JobPath, Purged) ->
    case prim_file:read_file_info(JobPath) of
        {ok, #file_info{type = directory, mtime = TStamp}} ->
            T = calendar:datetime_to_gregorian_seconds(TStamp),
            Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            Job = filename:basename(JobPath),
            IsPurged = gb_sets:is_member(list_to_binary(Job), Purged),
            GCAfter = list_to_integer(disco:get_setting("DISCO_GC_AFTER")),
            if IsPurged; Now - T > GCAfter ->
                ddfs_delete(disco:oob_name(Job)),
                _ = os:cmd("rm -Rf " ++ JobPath),
                ok;
            true ->
                ok
            end;
        _ ->
            ok
    end.
