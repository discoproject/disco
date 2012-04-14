
-module(fair_scheduler_fair_policy).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

-define(FAIRY_INTERVAL, 1000).
-define(FF_ALPHA_DEFAULT, 0.001).

-record(job, {name :: nonempty_string(),
              prio :: float(),
              cputime :: non_neg_integer(),
              bias :: float(),
              pid :: pid()}).

start_link() ->
    lager:info("Fair scheduler: Fair policy"),
    case gen_server:start_link({local, sched_policy},
                   fair_scheduler_fair_policy, [],
                   disco:debug_flags("fair_scheduler_fair_policy")) of
        {ok, Server} ->
            {ok, Server};
        {error, {already_started, Server}} ->
            {ok, Server}
    end.

init(_) ->
    register(fairy, spawn_link(fun() -> fairness_fairy(0) end)),
    {ok, {gb_trees:empty(), [], 0}}.

% messages starting with 'priv' are not part of the public policy api

handle_cast({priv_update_priorities, Priorities}, {Jobs, _, NC}) ->
    % The Jobs tree may have changed while fairy was working.
    % Update only the elements that fairy knew about.
    NewJobs = lists:foldl(fun({JobPid, NewJob}, NJobs) ->
        case gb_trees:lookup(JobPid, NJobs) of
            none ->
                NJobs;
            {value, _} ->
                gb_trees:update(JobPid, NewJob, NJobs)
        end
    end, Jobs, Priorities),
    % Include all known jobs in the priority queue.
    NewPrioQ = [{Prio, Pid, N} || #job{name = N, pid = Pid, prio = Prio}
            <- gb_trees:values(NewJobs)],
    {noreply, {NewJobs, lists:keysort(1, NewPrioQ), NC}};

% Cluster topology has changed. Inform the fairy about the new total
% number of cores available.
handle_cast({update_nodes, Nodes}, {Jobs, PrioQ, _}) ->
    NumCores = lists:sum([C || {_, C} <- Nodes]),
    fairy ! {update, NumCores},
    {noreply, {Jobs, PrioQ, NumCores}};

handle_cast({new_job, JobPid, JobName}, {Jobs, PrioQ, NC}) ->
    Job = #job{name = JobName, cputime = 0, prio = 0.0,
        bias = 0.0, pid = JobPid},

    NewJobs = gb_trees:insert(JobPid, Job, Jobs),
    erlang:monitor(process, JobPid),
    {noreply, {NewJobs, prioq_insert(
        {0.0, JobPid, JobName}, PrioQ), NC}}.

% Return current priorities for the ui
handle_call(current_priorities, _, {_, PrioQ, _} = S) ->
    {reply, {ok, [{N, Prio} || {Prio, _, N} <- PrioQ]}, S};

handle_call({next_job, _}, _, {{0, _}, _, _} = S) ->
    {reply, nojobs, S};

handle_call({next_job, NotJobs}, _, {{N, _}, _, _} = S)
        when length(NotJobs) >= N ->
    {reply, nojobs, S};

% NotJobs lists all jobs that got 'none' reply from the fair_scheduler_job task
% scheduler. We want to skip them.
handle_call({next_job, NotJobs}, _, {Jobs, PrioQ, NC}) ->
    {NextJob, RPrioQ} = dropwhile(PrioQ, [], NotJobs),
    {UJobs, UPrioQ} = bias_priority(gb_trees:get(NextJob, Jobs),
        RPrioQ, Jobs, NC),
    {reply, {ok, NextJob}, {UJobs, UPrioQ, NC}};

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call(priv_get_jobs, _, {Jobs, _, _} = S) ->
    {reply, {ok, Jobs}, S}.

handle_info({'DOWN', _, _, JobPid, _}, {Jobs, PrioQ, NC}) ->
    Job = gb_trees:get(JobPid, Jobs),
    gen_server:cast(scheduler, {job_done, Job#job.name}),
    {noreply, {gb_trees:delete(JobPid, Jobs),
        lists:keydelete(JobPid, 2, PrioQ), NC}}.

dropwhile([{_, JobPid, _} = E|R], H, NotJobs) ->
    case lists:member(JobPid, NotJobs) of
        false -> {JobPid, lists:reverse(H) ++ R};
        true -> dropwhile(R, [E|H], NotJobs)
    end.

-type prioq_item() :: {float(), _, _}.
-type prioq() :: [prioq_item()].

% Bias priority is a cheap trick to estimate a new priority for a job that
% has been just scheduled for running. It is based on the assumption that
% the job actually starts a new task (1 / NumCores increase in its share)
% which might not be always true. Fairness fairy will eventually fix the
% bias.
-spec bias_priority(#job{}, prioq(), gb_tree(), non_neg_integer()) ->
    {gb_tree(), prioq()}.
bias_priority(Job, PrioQ, Jobs, NumCores) ->
    JobPid = Job#job.pid,
    Bias = Job#job.bias + 1 / NumCores,
    Prio = Job#job.prio + Bias,
    NPrioQ = prioq_insert({Prio, JobPid, Job#job.name}, PrioQ),
    {gb_trees:update(JobPid, Job#job{bias = Bias}, Jobs), NPrioQ}.

% Insert an item to an already sorted list
-spec prioq_insert(prioq_item(), prioq()) -> prioq().
prioq_insert(Item, R) -> prioq_insert(Item, R, []).
-spec prioq_insert(prioq_item(), prioq(), prioq()) -> prioq().
prioq_insert(Item, [], H) -> lists:reverse([Item|H]);
prioq_insert({Prio, _, _} = Item, [{P, _, _} = E|R], H) when Prio > P ->
    prioq_insert(Item, R, [E|H]);
prioq_insert(Item, L, H) ->
    lists:reverse(H) ++ [Item|L].

% Fairness Fairy assigns priorities to jobs in real time based on
% the ideal share of resources they should get, and the reality of
% much resources they are occupying in practice.

-spec fairness_fairy(non_neg_integer()) -> no_return().
fairness_fairy(NumCores) ->
    receive
        {update, NewNumCores} ->
            fairness_fairy(NewNumCores);
        _ ->
            fairness_fairy(NumCores)
    after ?FAIRY_INTERVAL ->
        case application:get_env(fair_scheduler_alpha) of
            {ok, Alpha} ->
                update_priorities(Alpha, NumCores);
            undefined ->
                update_priorities(?FF_ALPHA_DEFAULT, NumCores)
        end,
        fairness_fairy(NumCores)
    end.

-spec update_priorities(float(), non_neg_integer()) -> 'ok'.
update_priorities(_, 0) -> ok;
update_priorities(Alpha, NumCores) ->
    {ok, Jobs} = gen_server:call(sched_policy, priv_get_jobs),
    NumJobs = gb_trees:size(Jobs),

    % Get the status of each running job
    Stats = [{Job, X} || {Job, {ok, X}} <-
           [{Job, catch gen_server:call(Job#job.pid, get_stats, 100)} ||
            Job <- gb_trees:values(Jobs)]],

    % Each job gets a 1/Nth share of resources by default
    Share = NumCores / lists:max([1, NumJobs]),
    % NB: Two things are not accounted in the deficit calculation
    % 1) If NumTasks < Share, job will accumulate deficit.
    % 2) If max_cores < Share, job will accumulate deficit.

    gen_server:cast(sched_policy, {priv_update_priorities, lists:map(fun
        ({Job, {_NumTasks, NumRunning}}) ->
            % Compute the difference between the ideal fair share
            % and how much resources the job has actually reserved
            Deficit = NumRunning / NumCores - Share / NumCores,
            % Job's priority is the exponential moving average
            % of its deficits over time
            Prio = Alpha * Deficit + (1 - Alpha) * Job#job.prio,
            {Job#job.pid, Job#job{prio = Prio, bias = 0.0,
                cputime = Job#job.cputime + NumRunning}}
    end, Stats)}).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
