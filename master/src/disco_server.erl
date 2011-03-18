
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([update_config_table/2,
         get_active/1,
         get_nodeinfo/1,
         new_job/2,
         kill_job/1,
         kill_job/2,
         purge_job/1,
         clean_job/1,
         get_worker_jobpack/2,
         new_task/2,
         connection_status/2,
         blacklist/2]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("disco.hrl").

-type connection_status() :: 'undefined' | 'up' | timer:timestamp().

-record(dnode, {node_monitor :: pid(),
                host :: nonempty_string(),
                blacklisted :: bool(),
                connection_status :: connection_status(),
                slots :: non_neg_integer(),
                num_running :: non_neg_integer(),
                stats_ok :: non_neg_integer(),
                stats_failed :: non_neg_integer(),
                stats_crashed :: non_neg_integer()}).
-type dnode() :: #dnode{}.

-record(state, {managers :: gb_tree(),
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
update_config_table(Config, Blacklist) ->
    gen_server:cast(?MODULE, {update_config_table, Config, Blacklist}).

-spec get_active(nonempty_string() | 'all') ->
    {'ok', [{nonempty_string(), task()}]}.
get_active(JobName) ->
    gen_server:call(?MODULE, {get_active, JobName}).

-spec get_nodeinfo('all') -> {'ok', [nodeinfo()]}.
get_nodeinfo(Spec) ->
    gen_server:call(?MODULE, {get_nodeinfo, Spec}).

-spec new_job(nonempty_string(), pid()) -> 'ok'.
new_job(JobName, JobCoordinator) ->
    gen_server:call(scheduler, {new_job, JobName, JobCoordinator}, 30000).

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

-spec blacklist(nonempty_string(), bool()) -> 'ok'.
blacklist(Node, True) ->
    gen_server:call(?MODULE, {blacklist, Node, True}).

% called from remote nodes
-spec get_worker_jobpack(pid(), nonempty_string()) -> binary().
get_worker_jobpack(Master, JobName) ->
    gen_server:call({?MODULE, Master}, {jobpack, JobName}).

%% ===================================================================
%% gen_server callbacks

init(_Args) ->
    process_flag(trap_exit, true),
    {ok, _} = fair_scheduler:start_link(),
    {ok, #state{managers = gb_trees:empty(),
                nodes = gb_trees:empty(),
                purged = gb_trees:empty()}}.

handle_cast({update_config_table, Config, Blacklist}, S) ->
    {noreply, do_update_config_table(Config, Blacklist, S)};

handle_cast(schedule_next, S) ->
    {noreply, do_schedule_next(S)};

handle_cast({purge_job, JobName}, S) ->
    {noreply, do_purge_job(JobName, S)}.

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

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

handle_call({jobpack, JobName}, From, State) ->
    spawn(fun () ->
                  gen_server:reply(From, jobpack:read(disco:jobhome(JobName)))
          end),
    {noreply, State};

handle_call({kill_job, JobName}, _From, S) ->
    {reply, do_kill_job(JobName), S};

handle_call({clean_job, JobName}, _From, S) ->
    {reply, do_clean_job(JobName), S};

handle_call({connection_status, Host, Status}, _From, S) ->
    {reply, ok, do_connection_status(Host, Status, S)};

handle_call({blacklist, Host, True}, _From, S) ->
    {reply, ok, do_blacklist(Host, True, S)}.

update_stats(Node) ->
    Node#dnode{num_running = Node#dnode.num_running - 1}.
update_stats(Node, {done, _Results}) ->
    update_stats(Node#dnode{stats_ok = Node#dnode.stats_ok + 1});
update_stats(Node, {error, _Error}) ->
    update_stats(Node#dnode{stats_failed = Node#dnode.stats_failed + 1});
update_stats(Node, _Reason) ->
    update_stats(Node#dnode{stats_crashed = Node#dnode.stats_crashed + 1}).

handle_exit(normal, {_Host, _Task}, State) ->
    {noreply, State};
handle_exit({shutdown, Reason}, {Host, Task}, State) ->
    handle_exit(Reason, {Host, Task}, State);
handle_exit(Reason, {Host, Task}, #state{nodes = Nodes} = State) ->
    Task#task.from ! {Reason, Task, Host},
    schedule_next(),
    {noreply,
     case gb_trees:lookup(Host, Nodes) of
         none ->
             State;
         {value, Node} ->
             State#state{nodes = gb_trees:update(Host,
                                                 update_stats(Node, Reason),
                                                 Nodes)}
     end}.

handle_info({'EXIT', From, noconnection}, State) ->
    error_logger:info_report("Connection lost to node (network busy?)", From),
    {noreply, State};
handle_info({'EXIT', From, Reason}, State) when From == self() ->
    error_logger:error_report(["Disco server died!", Reason]),
    {stop, stop_requested, State};
handle_info({'EXIT', From, Reason}, #state{managers = Managers} = State) ->
    case gb_trees:lookup(From, Managers) of
        none ->
            error_logger:info_report({'EXIT', From, Reason}),
            {noreply, State};
        {value, {Host, Task}} ->
            handle_exit(Reason,
                        {Host, Task},
                        State#state{managers = gb_trees:delete(From, Managers)})
    end.

%% ===================================================================
%% gen_server callback stubs

terminate(Reason, _State) ->
    error_logger:warning_report({"Disco server dies", Reason}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% internal functions

-spec allow_write(#dnode{}) -> bool().
allow_write(#dnode{connection_status = up,
                   blacklisted = false}) ->
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
    WhiteNodes = [{N#dnode.host, N#dnode.slots}
                  || N <- gb_trees:values(Nodes), allow_task(N)],
    DDFSNodes = [{disco:slave_node(N#dnode.host), allow_write(N), allow_read(N)}
                 || N <- gb_trees:values(Nodes)],
    gen_server:cast(ddfs_master, {update_nodes, DDFSNodes}),
    gen_server:cast(scheduler, {update_nodes, WhiteNodes}),
    schedule_next().

-spec do_connection_status(nonempty_string(), 'up'|'down', #state{}) -> #state{}.
do_connection_status(Host, down, State) ->
    do_connection_status(Host, now(), State);
do_connection_status(Host, Status, #state{nodes = Nodes} = State) ->
    UpdatedNodes =
        case gb_trees:lookup(Host, Nodes) of
            {value, Node} ->
                gb_trees:update(Host,
                                Node#dnode{connection_status = Status},
                                Nodes);
            none ->
                Nodes
        end,
    update_nodes(UpdatedNodes),
    State#state{nodes = UpdatedNodes}.

-spec do_blacklist(nonempty_string(), bool(), #state{}) -> #state{}.
do_blacklist(Host, True, #state{nodes = Nodes} = State) ->
    UpdatedNodes =
        case gb_trees:lookup(Host, Nodes) of
            {value, Node} ->
                gb_trees:update(Host,
                                Node#dnode{blacklisted = True},
                                Nodes);
            none ->
                Nodes
        end,
    update_nodes(UpdatedNodes),
    State#state{nodes = UpdatedNodes}.

spawn_manager(Host, NodeMonitor, Task) ->
    event_server:task_event(Task, disco:format("assigned to ~s", [Host])),
    node_mon:spawn_manager(NodeMonitor, Task).

start_node(Host, OldNodes) ->
    case gb_trees:lookup(Host, OldNodes) of
        none ->
            #dnode{node_monitor = node_mon:start_monitor(Host),
                   host = Host,
                   connection_status = undefined,
                   num_running = 0,
                   stats_ok = 0,
                   stats_failed = 0,
                   stats_crashed = 0};
        {value, Node} ->
            Node
    end.

-spec do_update_config_table([disco_config:host_info()], [nonempty_string()],
                             #state{}) -> #state{}.
do_update_config_table(Config, Blacklist, State) ->
    error_logger:info_report([{"Config table update"}]),
    Nodes = lists:foldl(
              fun ({Host, Slots}, Nodes) ->
                      case catch start_node(Host, State#state.nodes) of
                          {'EXIT', Reason} ->
                              error_logger:error_report(Reason),
                              Nodes;
                          Node ->
                              Blacklisted = lists:member(Host, Blacklist),
                              gb_trees:insert(Host,
                                              Node#dnode{slots = Slots,
                                                         blacklisted = Blacklisted},
                                              Nodes)
                      end
              end, gb_trees:empty(), Config),
    lists:foreach(
      fun(OldNode) ->
              case gb_trees:lookup(OldNode#dnode.host, Nodes) of
                  none ->
                      unlink(OldNode#dnode.node_monitor),
                      exit(OldNode#dnode.node_monitor, kill);
                  Node ->
                      Node
              end
      end, gb_trees:values(State#state.nodes)),
    disco_proxy:update_nodes(gb_trees:keys(Nodes)),
    update_nodes(Nodes),
    State#state{nodes = Nodes}.

-spec schedule_next() -> 'ok'.
schedule_next() ->
    gen_server:cast(?MODULE, schedule_next).

-spec do_schedule_next(#state{}) -> #state{}.
do_schedule_next(#state{nodes = Nodes, managers = Managers} = State) ->
    Available = [{NumRunning, Name}
                 || #dnode{slots = Slots,
                           num_running = NumRunning,
                           host = Name} = Node
                        <- gb_trees:values(Nodes),
                    Slots > NumRunning,
                    allow_task(Node)],

    case lists:unzip(lists:keysort(1, Available)) of
        {_, []} ->
            State;
        {_, Preferred} ->
            case gen_server:call(scheduler, {next_task, Preferred}) of
                {ok, {JobSchedPid, {Host, Task}}} ->
                    Node = gb_trees:get(Host, Nodes),
                    Manager = spawn_manager(Host, Node#dnode.node_monitor, Task),
                    NewManagers = gb_trees:insert(Manager, {Host, Task}, Managers),
                    NewNodes = gb_trees:update(Host,
                                               Node#dnode{num_running = Node#dnode.num_running + 1},
                                               Nodes),
                    gen_server:cast(JobSchedPid, {task_started, Host, Manager}),
                    do_schedule_next(State#state{nodes = NewNodes,
                                                 managers = NewManagers});
                nojobs ->
                    State
            end
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

-spec do_new_task(task(), #state{}) -> 'ok' | 'failed'.
do_new_task(Task, S) ->
    NodeStats = [case gb_trees:lookup(Host, S#state.nodes) of
                     {value, Node} ->
                         {Node#dnode.num_running, Input};
                     none ->
                         {false, Input}
                 end
                 || {_Url, Host} = Input <- Task#task.input],
    case catch gen_server:call(scheduler, {new_task, Task, NodeStats}) of
        ok ->
            schedule_next(),
            ok;
        Error ->
            error_logger:warning_report({"Scheduling task failed", Task, Error}),
            failed
    end.

-spec do_get_active(nonempty_string() | 'all', #state{}) ->
    {'ok', [{nonempty_string(), task()}]}.
do_get_active(all, #state{managers = Managers}) ->
    {ok, gb_trees:values(Managers)};
do_get_active(JobName, #state{managers = Managers}) ->
    Active = [{Host, Task} || {Host, #task{jobname = N} = Task}
                                  <- gb_trees:values(Managers), N == JobName],
    {ok, Active}.

-spec do_get_nodeinfo(#state{}) -> {'ok', [nodeinfo()]}.
do_get_nodeinfo(#state{nodes = Nodes}) ->
    Info = [#nodeinfo{name = N#dnode.host,
                      slots = N#dnode.slots,
                      num_running = N#dnode.num_running,
                      stats_ok = N#dnode.stats_ok,
                      stats_failed = N#dnode.stats_failed,
                      stats_crashed = N#dnode.stats_crashed,
                      connected = N#dnode.connection_status =:= up,
                      blacklisted = N#dnode.blacklisted}
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
    event_server:event(JobName, "WARNING: Job killed", [], []),
    % Make sure that scheduler don't accept new tasks from this job
    gen_server:cast(scheduler, {job_done, JobName}),
    ok.

-spec do_clean_job(nonempty_string()) -> 'ok'.
do_clean_job(JobName) ->
    do_kill_job(JobName),
    gen_server:cast(event_server, {clean_job, JobName}),
    ok.
