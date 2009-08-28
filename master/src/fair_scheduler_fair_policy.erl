
-module(fair_scheduler_fair_policy).
-behaviour(gen_server).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, 
        handle_info/2, terminate/2, code_change/3]).

-record(job, {name, prio, cputime, bias, pid}).

start_link(Nodes) ->
        error_logger:info_report([{"Fair scheduler: Fair policy"}]),
        case gen_server:start_link({local, fair_scheduler_policy}, 
                        fair_scheduler_fair_policy, Nodes, []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init(Nodes) ->
        register(fairy, spawn_link(fun() -> fairness_fairy(Nodes) end)),
        NumCores = lists:sum([C || {_, C} <- Nodes]),
        {ok, {gb_trees:empty(), [], NumCores}}.

% messages starting with _ are not part of the public policy api

handle_cast({_update_priorities, Priorities}, _, {Jobs, _, NC}) ->
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
        NewPrioQ = [{Prio, Pid} || #job{pid = Pid, prio = Prio} <- 
                gb_trees:values(NewJobs)],
        {noreply, {NewJobs, lists:keysort(1, NewPrioQ), NC}};

handle_cast({update_nodes, Nodes}, _, {Jobs, PrioQ, _}) ->
        NumCores = lists:sum([C || {_, C} <- Nodes]),
        fairy ! {update, NumCores},
        {noreply, {Jobs, PrioQ, NumCores}};

handle_cast({new_job, JobPid, JobName}, _, {Jobs, PrioQ, NC}) ->
        Job = #job{name = JobName, cputime = 0, prio = 0.0,
                bias = 0.0, pid = JobPid},
        NewJobs = gb_trees:insert(JobPid, Job, Jobs),
        {Pos, Neg} = lists:partition(fun({X, _}) -> X > 0.0 end, PrioQ),
        erlang:monitor(process, Job),
        {noreply, {NewJobs, prioq_insert({0.0, JobPid}, PrioQ), NC}}.

handle_call(next_job, _, {{0, _}, _, _} = S) ->
        {reply, nojobs, S};

handle_call(next_job, _, {Jobs, [{_, NextJob}|R], NC}) ->
        {UJobs, UPrioQ} = bias_priority(gb_trees:get(NextJob), R, Jobs, NC),
        {reply, {ok, NextJob}, {UJobs, UPrioQ, NC}};

handle_call(_get_jobs, _, {Jobs, _, _} = S) ->
        {reply, Jobs, S}.

handle_info({'DOWN', _, _, JobPid, _}, {Jobs, PrioQ, NC}) ->
        {noreply, {gb_trees:delete(JobPid, Jobs),
                lists:keydelete(JobPid, 2, PrioQ), NC}}.

bias_priority(Job, Rest, Jobs, NumCores) ->
        JobPid = Job#job.pid,
        Bias = Job#job.bias + 1 / NumCores,
        Prio = Job#job.prio + Bias,
        NPrioQ = prioq_insert({Prio, JobPid}, Rest),
        {gb_trees:update(JobPid, Job#job{bias = Bias}, Jobs), NPrioQ}.

prioq_insert(Item, R) -> prioq_insert(Item, R, []).
prioq_insert({Prio, _} = Item, [], H) ->
        lists:reverse([Item|H]);
prioq_insert({Prio, _} = Item, [{P, _} = E|R], H) when Prio > P ->
        prioq_insert(Item, R, [E|H]);
prioq_insert(Item, L, H) ->
        lists:reverse(H) ++ [Item|L].

        
        
        
        

fairness_fairy() ->


new_priorities(Jobs) ->
        
