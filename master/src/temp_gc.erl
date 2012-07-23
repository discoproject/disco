-module(temp_gc).

-include_lib("kernel/include/file.hrl").

-include("common_types.hrl").
-include("disco.hrl").
-include("config.hrl").

-export([start_link/2]).

-define(GC_INTERVAL, 2 * ?DAY).

-spec start_link(node(), path()) -> no_return().
start_link(Master, DataRoot) ->
    try register(temp_gc, self())
    catch _:_ -> exit(already_started)
    end,
    put(master, Master),
    loop(DataRoot).

-spec loop(path()) -> no_return().
loop(DataRoot) ->
    try
        {{ok, Purged}, {ok, Jobs}} = {get_purged(), event_server:get_jobs(get(master))},
        case prim_file:list_dir(DataRoot) of
            {ok, Dirs} ->
                Active = gb_sets:from_list(
                           [Name || {Name, active, _Start} <- Jobs]),
                process_dir(DataRoot, Dirs, gb_sets:from_ordset(Purged), Active);
            E ->
                % fresh install, try again after GC_INTERVAL
                error_logger:info_msg("Tempgc: error listing ~p: ~p",
                                      [DataRoot, E]),
                ok
        end
    catch K:V ->
            error_logger:info_msg("Tempgc: error contacting master from ~p: ~p:~p",
                                  [node(), K,V])
            % master busy, try again after GC_INTERVAL
    end,
    error_logger:info_msg("Tempgc: one pass completed on ~p", [node()]),
    timer:sleep(?GC_INTERVAL),
    flush(),
    loop(DataRoot).

% gen_server calls below may timeout, so we need to purge late replies
flush() ->
    receive _ -> flush()
    after 0 -> ok
    end.

ddfs_delete(Tag) ->
    ddfs:delete({ddfs_master, get(master)}, Tag, internal).

get_purged() ->
    disco_server:get_purged(get(master)).

-spec process_dir(path(), [path()], gb_set(), gb_set()) -> ok.
process_dir(_DataRoot, [], _Purged, _Active) -> ok;
process_dir(DataRoot, [Dir|R], Purged, Active) ->
    Path = filename:join(DataRoot, Dir),
    {ok, Jobs} = prim_file:list_dir(Path),
    _ = [process_job(filename:join(Path, Job), Purged)
         || Job <- Jobs, ifdead(Job, Active)],
    process_dir(DataRoot, R, Purged, Active).

-spec ifdead(jobname(), gb_set()) -> boolean().
ifdead(Job, Active) ->
    not gb_sets:is_member(Job, Active).

% Perform purge in one function so that gen_server errors can be
% caught by callers.
-spec purge_job(jobname(), nonempty_string()) -> ok.
purge_job(Job, JobPath) ->
    % Perform ddfs_delete before removing JobPath, so that in case
    % there are errors in deleting oob_name, we fail fast and leave
    % JobPath around for the next scan to find and retry.
    ddfs_delete(disco:oob_name(Job)),
    _ = os:cmd("rm -Rf " ++ JobPath),
    ok.

-spec process_job(path(), gb_set()) -> ok.
process_job(JobPath, Purged) ->
    case prim_file:read_file_info(JobPath) of
        {ok, #file_info{type = directory, mtime = TStamp}} ->
            T = calendar:datetime_to_gregorian_seconds(TStamp),
            Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            Job = filename:basename(JobPath),
            IsPurged = gb_sets:is_member(list_to_binary(Job), Purged),
            GCAfter = list_to_integer(disco:get_setting("DISCO_GC_AFTER")),
            if IsPurged; Now - T > GCAfter ->
                error_logger:info_msg("Tempgc: purging ~p", [JobPath]),
                catch purge_job(Job, JobPath);
            true ->
                ok
            end,
            % Sleep here to prevent master and disk being DDOS'ed
            timer:sleep(100);
        E ->
            error_logger:info_msg("Tempgc: error processing ~p: ~p",
                                  [JobPath, E])
    end.
