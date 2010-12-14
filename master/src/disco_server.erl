
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([update_config_table/2, get_active/1, get_nodeinfo/1,
         new_job/3, kill_job/1, kill_job/2, purge_job/1, clean_job/1,
         new_task/2, connection_status/2, manual_blacklist/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("disco.hrl").

-type connection_status() :: 'undefined' | 'up' | timer:timestamp().

-record(dnode, {name :: nonempty_string(),
                node_mon :: pid(),
                manual_blacklist :: bool(),
                connection_status :: connection_status(),
                slots :: non_neg_integer(),
                num_running :: non_neg_integer(),
                stats_ok :: non_neg_integer(),
                stats_failed :: non_neg_integer(),
                stats_crashed :: non_neg_integer()}).
-type dnode() :: #dnode{}.

-record(state, {workers :: gb_tree(),
                nodes :: gb_tree(),
                purged :: gb_tree()}).

-define(PURGE_TIMEOUT, 86400000). % 24h

%% ===================================================================
%% API functions

start_link() ->
    error_logger:info_report([{"DISCO SERVER STARTS"}]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE,
                               [], disco:debug_flags("disco_server")) of
        {ok, Server} ->
            case catch disco_config:get_config_table() of
                {ok, _Config} ->
                    {ok, Server};
                E ->
                    error_logger:warning_report({"Parsing config failed", E})
            end;
        {error, {already_started, Server}} ->
            {ok, Server}
    end.

stop() ->
    gen_server:call(?MODULE, stop).

-spec update_config_table([disco_config:host_info()], [nonempty_string()]) -> 'ok'.
update_config_table(Config, ManualBlacklist) ->
    gen_server:cast(?MODULE, {update_config_table, Config, ManualBlacklist}).

-spec get_active(nonempty_string() | 'all') ->
    {'ok', [{nonempty_string(), task()}]}.
get_active(JobName) ->
    gen_server:call(?MODULE, {get_active, JobName}).

-spec get_nodeinfo('all') -> {'ok', [nodeinfo()]}.
get_nodeinfo(Spec) ->
    gen_server:call(?MODULE, {get_nodeinfo, Spec}).

-spec new_job(nonempty_string(), pid(), non_neg_integer()) -> 'ok'.
new_job(JobName, JobCoord, Timeout) ->
    gen_server:call(?MODULE, {new_job, JobName, JobCoord}, Timeout).

-spec kill_job(nonempty_string()) -> 'ok'.
kill_job(JobName) ->
    gen_server:call(?MODULE, {kill_job, JobName}).
-spec kill_job(nonempty_string(), non_neg_integer()) -> 'ok'.
kill_job(JobName, Timeout) ->
    gen_server:call(?MODULE, {kill_job, JobName}, Timeout).

-spec purge_job(nonempty_string()) -> 'ok'.
purge_job(JobName) ->
    gen_server:cast(?MODULE, {purge_job, JobName}).

-spec clean_job(nonempty_string()) -> 'ok'.
clean_job(JobName) ->
    gen_server:call(?MODULE, {clean_job, JobName}).

-spec new_task(task(), non_neg_integer()) -> 'ok' | 'failed'.
new_task(Task, Timeout) ->
    gen_server:call(?MODULE, {new_task, Task}, Timeout).

-spec connection_status(nonempty_string(), 'up' | 'down') -> 'ok'.
connection_status(Node, Status) ->
    gen_server:call(?MODULE, {connection_status, Node, Status}).

-spec manual_blacklist(nonempty_string(), bool()) -> 'ok'.
manual_blacklist(Node, True) ->
    gen_server:call(?MODULE, {manual_blacklist, Node, True}).

%% ===================================================================
%% gen_server callbacks

init(_Args) ->
    process_flag(trap_exit, true),
    {ok, _} = fair_scheduler:start_link(),
    {ok, #state{workers = gb_trees:empty(),
                nodes = gb_trees:empty(),
                purged = gb_trees:empty()}}.

handle_cast({update_config_table, Config, ManualBlacklist}, S) ->
    {noreply, do_update_config_table(Config, ManualBlacklist, S)};

handle_cast(schedule_next, S) ->
    {noreply, do_schedule_next(S)};

handle_cast({purge_job, JobName}, S) ->
    {noreply, do_purge_job(JobName, S)};

handle_cast({exit_worker, Pid, Res}, S) ->
    {noreply, do_exit_worker(Pid, Res, S)}.

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call({new_job, JobName, JobCoord}, _, S) ->
    {reply, do_new_job(JobName, JobCoord, S), S};

handle_call({new_task, Task}, _, S) ->
    {reply, do_new_task(Task, S), S};

handle_call({get_active, JobName}, _From, S) ->
    {reply, do_get_active(JobName, S), S};

handle_call({get_nodeinfo, all}, _From, S) ->
    {reply, do_get_nodeinfo(S), S};

handle_call(get_purged, _, S) ->
    {Result, S1} = do_get_purged(S),
    {reply, Result, S1};

handle_call(get_num_cores, _, S) ->
    {reply, do_get_num_cores(S), S};

handle_call({kill_job, JobName}, _From, S) ->
    {reply, do_kill_job(JobName), S};

handle_call({clean_job, JobName}, _From, S) ->
    {reply, do_clean_job(JobName), S};

handle_call({connection_status, Node, Status}, _From, S) ->
    {reply, ok, do_connection_status(Node, Status, S)};

handle_call({manual_blacklist, Node, True}, _From, S) ->
    {reply, ok, do_manual_blacklist(Node, True, S)}.

handle_info({'EXIT', Pid, normal}, S) ->
    case gb_trees:lookup(Pid, S#state.workers) of
        none -> {noreply, S};
        _ -> error_logger:warning_report({"Task failed to call exit_worker",
                                          Pid}),
             process_exit(Pid, "Died unexpectedly without a reason",
            "unexpected", S)
    end;

handle_info({'EXIT', Pid, {worker_dies, {Msg, Args}}}, S) ->
    process_exit(Pid, io_lib:fwrite(Msg, Args), "worker_dies", S);

handle_info({'EXIT', Pid, noconnection}, S) ->
    process_exit(Pid, "Connection lost to the node (network busy?)",
                 "noconnection", S);

handle_info({'EXIT', Pid, Reason}, S) when Pid == self() ->
    error_logger:warning_report(["Disco server dies on error!", Reason]),
    {stop, stop_requested, S};

handle_info({'EXIT', Pid, Reason}, S) ->
    process_exit(Pid, io_lib:fwrite("Worked died unexpectedly: ~p", [Reason]),
                 "unexpected", S).

%% ===================================================================
%% gen_server callback stubs

terminate(Reason, _State) ->
    error_logger:warning_report({"Disco server dies", Reason}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% internal functions

-spec allow_write(#dnode{}) -> bool().
allow_write(#dnode{connection_status = up,
                   manual_blacklist = false}) ->
    true;
allow_write(#dnode{}) ->
    false.

-spec allow_read(#dnode{}) -> bool().
allow_read(#dnode{connection_status = up}) ->
    true;
allow_read(#dnode{}) ->
    false.

-spec allow_task(#dnode{}) -> bool().
allow_task(#dnode{} = N) -> allow_write(N).

-spec update_nodes(gb_tree()) -> 'ok'.
update_nodes(Nodes) ->
    WhiteNodes = [{N#dnode.name, N#dnode.slots}
                  || N <- gb_trees:values(Nodes), allow_task(N)],
    DDFSNodes = [{disco:node(N#dnode.name), allow_write(N), allow_read(N)}
                 || N <- gb_trees:values(Nodes)],
    gen_server:cast(ddfs_master, {update_nodes, DDFSNodes}),
    gen_server:cast(scheduler, {update_nodes, WhiteNodes}),
    schedule_next().

-spec update_stats(nonempty_string(), 'none'|{'value', dnode()}, _,
                   #state{}) -> #state{}.
update_stats(_Node, none, _ReplyType, S) -> S;
update_stats(Node, {value, N}, ReplyType, S) ->
    M = N#dnode{num_running = N#dnode.num_running - 1},
    M0 = case ReplyType of
             job_ok ->
                 M#dnode{stats_ok = M#dnode.stats_ok + 1};
             data_error ->
                 M#dnode{stats_failed = M#dnode.stats_failed + 1};
             job_error ->
                 M#dnode{stats_crashed = M#dnode.stats_crashed + 1};
             _ ->
                 M#dnode{stats_crashed = M#dnode.stats_crashed + 1}
         end,
    S#state{nodes = gb_trees:update(Node, M0, S#state.nodes)}.

-spec do_connection_status(nonempty_string(), 'up'|'down', #state{}) -> #state{}.
do_connection_status(Node, Status, #state{nodes = Nodes} = S) ->
    UpdatedNodes =
        case gb_trees:lookup(Node, Nodes) of
            {value, N} when Status =:= up ->
                N1 = N#dnode{connection_status = up},
                gb_trees:update(Node, N1, Nodes);
            {value, N} when Status =:= down ->
                N1 = N#dnode{connection_status = now()},
                gb_trees:update(Node, N1, Nodes);
            _ -> Nodes
        end,
    update_nodes(UpdatedNodes),
    S#state{nodes = UpdatedNodes}.

-spec do_manual_blacklist(nonempty_string(), bool(), #state{}) -> #state{}.
do_manual_blacklist(Node, True, #state{nodes = Nodes} = S) ->
    UpdatedNodes =
        case gb_trees:lookup(Node, Nodes) of
            {value, N} ->
                N1 = N#dnode{manual_blacklist = True},
                gb_trees:update(Node, N1, Nodes);
            _ -> Nodes
        end,
    update_nodes(UpdatedNodes),
    S#state{nodes = UpdatedNodes}.

start_worker(Node, NodeMon, T) ->
    event_server:event(T#task.jobname, "~s:~B assigned to ~s",
                       [T#task.mode, T#task.taskid, Node], []),
    spawn_link(disco_worker, start_link_remote,
               [self(), whereis(event_server), Node, NodeMon, T]).

process_exit(Pid, Msg, Code, S) ->
    process_exit1(gb_trees:lookup(Pid, S#state.workers), Pid, Msg, Code, S).

process_exit1(none, _, _, _, S) -> {noreply, S};
process_exit1({_, {Node, T}}, Pid, Msg, Code, S) ->
    P = io_lib:fwrite("WARN: [~s:~B] ", [T#task.mode, T#task.taskid]),
    event_server:event(Node, T#task.jobname, lists:flatten(P, Msg), [],
                       {task_failed, T#task.mode}),
    gen_server:cast(self(), {exit_worker, Pid, {data_error, Code}}),
    {noreply, S}.

-spec do_update_config_table([disco_config:host_info()], [nonempty_string()],
                             #state{}) -> #state{}.
do_update_config_table(Config, Blacklist, S) ->
    error_logger:info_report([{"Config table update"}]),
    NewNodes =
        lists:foldl(fun({Host, Slots}, NewNodes) ->
            NewNode =
                case gb_trees:lookup(Host, S#state.nodes) of
                    none ->
                        #dnode{name = Host,
                               node_mon = node_mon:start_link(Host),
                               manual_blacklist = lists:member(Host, Blacklist),
                               connection_status = undefined,
                               slots = Slots,
                               num_running = 0,
                               stats_ok = 0,
                               stats_failed = 0,
                               stats_crashed = 0};
                    {value, N} ->
                        N#dnode{slots = Slots,
                                manual_blacklist = lists:member(Host, Blacklist)}
                end,
            gb_trees:insert(Host, NewNode, NewNodes)
        end, gb_trees:empty(), Config),
    lists:foreach(
        fun(OldNode) ->
            case gb_trees:lookup(OldNode#dnode.name, NewNodes) of
                none ->
                    unlink(OldNode#dnode.node_mon),
                    exit(OldNode#dnode.node_mon, kill);
                _ -> ok
            end
        end, gb_trees:values(S#state.nodes)),
    disco_proxy:update_nodes(gb_trees:keys(NewNodes)),
    update_nodes(NewNodes),
    S#state{nodes = NewNodes}.

-spec schedule_next() -> 'ok'.
schedule_next() ->
    gen_server:cast(?MODULE, schedule_next).

-spec do_schedule_next(#state{}) -> #state{}.
do_schedule_next(#state{nodes = Nodes, workers = Workers} = S) ->
    Running = [{Y, N} || #dnode{slots = X, num_running = Y, name = N} = Node
                             <- gb_trees:values(Nodes), X > Y, allow_task(Node)],
    {_, AvailableNodes} = lists:unzip(lists:keysort(1, Running)),
    if AvailableNodes =/= [] ->
        case gen_server:call(scheduler, {next_task, AvailableNodes}) of
            {ok, {JobSchedPid, {Node, Task}}} ->
                M = gb_trees:get(Node, Nodes),
                WorkerPid = start_worker(Node, M#dnode.node_mon, Task),
                UWorkers = gb_trees:insert(WorkerPid, {Node, Task}, Workers),
                gen_server:cast(JobSchedPid, {task_started, Node, WorkerPid}),

                M1 = M#dnode{num_running = M#dnode.num_running + 1},
                UNodes = gb_trees:update(Node, M1, Nodes),
                S1 = S#state{nodes = UNodes, workers = UWorkers},
                do_schedule_next(S1);
            nojobs ->
                S
        end;
       true -> S
    end.

-spec do_purge_job(nonempty_string(), #state{}) -> #state{}.
do_purge_job(JobName, #state{purged = Purged} = S) ->
    handle_call({clean_job, JobName}, none, S),
    % NB! next line disabled for 0.3.1, ISSUE #227
    %ddfs:delete(ddfs_master, disco:oob_name(JobName), internal),
    Key = list_to_binary(JobName),
    NPurged =
        case gb_trees:is_defined(Key, Purged) of
            true ->
                Purged;
            false ->
                gb_trees:insert(Key, now(), Purged)
        end,
    S#state{purged = NPurged}.

-spec do_exit_worker(pid(), _, #state{}) -> #state{}.
do_exit_worker(Pid, {Type, _} = Res, S) ->
    V = gb_trees:lookup(Pid, S#state.workers),
    if V == none ->
            S;
       true ->
            {_, {Node, Task}} = V,
            UWorkers = gb_trees:delete(Pid, S#state.workers),
            Task#task.from ! {Res, Task, Node},
            schedule_next(),
            update_stats(Node, gb_trees:lookup(Node, S#state.nodes),
                         Type, S#state{workers = UWorkers})
    end.

-spec do_new_job(nonempty_string(), pid(), #state{}) -> 'ok'.
do_new_job(JobName, JobCoord, _S) ->
    catch gen_server:call(scheduler, {new_job, JobName, JobCoord}).

-spec do_new_task(task(), #state{}) -> 'ok' | 'failed'.
do_new_task(Task, S) ->
    NodeStats = [case gb_trees:lookup(Node, S#state.nodes) of
                     none -> {false, Input};
                     {value, N} -> {N#dnode.num_running, Input}
                 end || {_Url, Node} = Input <- Task#task.input],
    case catch gen_server:call(scheduler, {new_task, Task, NodeStats}) of
        ok ->
            schedule_next(),
            ok;
        Error ->
            error_logger:warning_report({"Scheduling task failed",
                                         Task, Error}),
            failed
    end.

-spec do_get_active(nonempty_string() | 'all', #state{}) ->
    {'ok', [{nonempty_string(), task()}]}.
do_get_active(all, #state{workers = Workers}) ->
    {ok, gb_trees:values(Workers)};
do_get_active(JobName, #state{workers = Workers}) ->
    Active = [{Host, Task} || {Host, #task{jobname = N} = Task}
                                  <- gb_trees:values(Workers), N == JobName],
    {ok, Active}.

-spec do_get_nodeinfo(#state{}) -> {'ok', [nodeinfo()]}.
do_get_nodeinfo(#state{nodes = Nodes}) ->
    Info = [#nodeinfo{name = N#dnode.name,
                      slots = N#dnode.slots,
                      num_running = N#dnode.num_running,
                      stats_ok = N#dnode.stats_ok,
                      stats_failed = N#dnode.stats_failed,
                      stats_crashed = N#dnode.stats_crashed,
                      connected = N#dnode.connection_status =:= up,
                      blacklisted = N#dnode.manual_blacklist}
            || N <- gb_trees:values(Nodes)],
    {ok, Info}.

-spec do_get_purged(#state{}) -> {{'ok', [binary()]}, #state{}}.
do_get_purged(#state{purged = Purged} = S) ->
    Now = now(),
    NPurgedList =
        [{Job, TStamp} || {Job, TStamp} <- gb_trees:to_list(Purged),
                          timer:now_diff(Now, TStamp) < ?PURGE_TIMEOUT * 1000],
    NPurged = gb_trees:from_orddict(NPurgedList),
    {{ok, gb_trees:keys(NPurged)}, S#state{purged = NPurged}}.

-spec do_get_num_cores(#state{}) -> {'ok', non_neg_integer()}.
do_get_num_cores(#state{nodes = Nodes}) ->
    {ok, lists:sum([N#dnode.slots || N <- gb_trees:values(Nodes)])}.

-spec do_kill_job(nonempty_string()) -> 'ok'.
do_kill_job(JobName) ->
    event_server:event(JobName, "WARN: Job killed", [], []),
    % Make sure that scheduler don't accept new tasks from this job
    gen_server:cast(scheduler, {job_done, JobName}),
    ok.

-spec do_clean_job(nonempty_string()) -> 'ok'.
do_clean_job(JobName) ->
    do_kill_job(JobName),
    gen_server:cast(event_server, {clean_job, JobName}),
    ok.
