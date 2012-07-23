% This module implements the global task scheduler (GTS).  It is
% called by disco_server on any event that indicates a new task could
% perhaps be scheduled on the cluster.  The selection of this task is
% split into two phases:
%
% . First, a candidate job is selected according to the scheduler
%   policy (omitting from consideration any jobs who do not have any
%   tasks that can be scheduled currently on the available cluster
%   nodes).
%
% . Next, the job's task scheduler (JTS) in fair_scheduler_job.erl is
%   called with the list of cluster nodes which have available
%   computing slots.  The task returned by the JTS is then returned by
%   the GTS.  If the job's JTS does not have a candidate task, we
%   re-run the previous step with this job added to the omit list.

-module(fair_scheduler).
-behaviour(gen_server).

-export([start_link/0, new_job/2, job_done/1,
         next_task/1, new_task/2, update_nodes/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("fair_scheduler.hrl").

-type state() :: [node()].

%% ===================================================================
%% API functions

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Fair scheduler starts"),
    case gen_server:start_link({local, ?MODULE}, fair_scheduler, [],
                               disco:debug_flags("fair_scheduler"))
    of  {ok, _Server} = Ret -> Ret;
        {error, {already_started, Server}} -> {ok, Server}
    end.

-spec update_nodes([node_update()]) -> ok.
update_nodes(NewNodes) ->
    gen_server:cast(?MODULE, {update_nodes, NewNodes}).

-spec job_done(jobname()) -> ok.
job_done(JobName) ->
    gen_server:cast(?MODULE, {job_done, JobName}).

-spec next_task([host()]) -> nojobs | {ok, {pid(), {node(), task()}}}.
next_task(AvailableNodes) ->
    gen_server:call(?MODULE, {next_task, AvailableNodes}).

-spec new_job(jobname(), pid()) -> ok.
new_job(JobName, JobCoord) ->
    gen_server:call(?MODULE, {new_job, JobName, JobCoord}).

-spec new_task(task(), [nodestat()]) -> unknown_job | ok.
new_task(Task, NodeStat) ->
    gen_server:call(?MODULE, {new_task, Task, NodeStat}).

%% ===================================================================
%% gen_server callbacks

-spec init([]) -> gs_init().
init([]) ->
    {ok, _ } =
        case application:get_env(scheduler_opt) of
            {ok, "fifo"} ->
                lager:info("Scheduler uses fifo policy"),
                fair_scheduler_fifo_policy:start_link();
            _ ->
                lager:info("Scheduler uses fair policy"),
                fair_scheduler_fair_policy:start_link()
        end,
    _ = ets:new(jobs, [private, named_table]),
    {ok, []}.

-spec handle_cast(update_nodes_msg() | {job_done, jobname()}, state())
                 -> gs_noreply().
handle_cast({update_nodes, NewNodes}, _) ->
    gen_server:cast(sched_policy, {update_nodes, NewNodes}),
    NNodes = [Name || {Name, _NumCores} <- NewNodes],
    Msg = {update_nodes, NNodes},
    _ = [gen_server:cast(JobPid, Msg) || {_, {JobPid,_}} <- ets:tab2list(jobs)],
    {noreply, NNodes};

handle_cast({job_done, JobName}, Nodes) ->
    % We absolutely don't want to have the job coordinator alive after the
    % job has been removed from the scheduler. Make sure that doesn't
    % happen.
    case ets:lookup(jobs, JobName) of
        [] ->
            {noreply, Nodes};
        [{_, {_, JobCoord}}] ->
            ets:delete(jobs, JobName),
            exit(JobCoord, kill_worker),
            {noreply, Nodes}
    end.

-spec handle_call({new_job, jobname(), pid()}, from(), state()) -> gs_reply(ok);
                 (new_task_msg(), from(), state()) -> gs_reply(unknown_job | ok);
                 (dbg_state_msg(), from(), state()) -> gs_reply(state());
                 ({next_task, [host()]}, from(), state()) ->
                         gs_reply(nojobs | {ok, {pid(), {node(), task()}}}).

handle_call({new_job, JobName, JobCoord}, _, Nodes) ->
    {ok, JobPid} = fair_scheduler_job:start(JobName, JobCoord),
    gen_server:cast(JobPid, {update_nodes, Nodes}),
    gen_server:cast(sched_policy, {new_job, JobPid, JobName}),
    ets:insert(jobs, {JobName, {JobPid, JobCoord}}),
    {reply, ok, Nodes};

% This is not a handle_cast function, since we don't want to race against
% disco_server. We need to send the new_job and new_task messaged before
% disco_server sends its task_started and next_task messages.
handle_call({new_task, Task, NodeStats}, _, Nodes) ->
    JobName = Task#task.jobname,
    case ets:lookup(jobs, JobName) of
        [] ->
            {reply, unknown_job, Nodes};
        [{_, {JobPid, _}}] ->
            gen_server:cast(JobPid, {new_task, Task, NodeStats}),
            {reply, ok, Nodes}
    end;

handle_call(dbg_get_state, _, S) ->
    {reply, {S, ets:tab2list(jobs)}, S};

handle_call({next_task, AvailableNodes}, _From, Nodes) ->
    Jobs = [JobPid || {_, {JobPid, _}} <- ets:tab2list(jobs)],
    {reply, next_task(AvailableNodes, Jobs, []), Nodes}.

next_task(AvailableNodes, Jobs, NotJobs) ->
    case gen_server:call(sched_policy, {next_job, NotJobs}) of
        {ok, JobPid} ->
            case fair_scheduler_job:next_task(JobPid, Jobs, AvailableNodes) of
                {ok, Task} -> {ok, {JobPid, Task}};
                none       -> next_task(AvailableNodes, Jobs, [JobPid|NotJobs])
            end;
        nojobs -> nojobs
    end.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(_Msg, State) -> {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    _ = [exit(JobPid, kill) || {_, {JobPid, _}} <- ets:tab2list(jobs)],
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
