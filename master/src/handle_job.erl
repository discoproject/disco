
-module(handle_job).
-export([handle/2, set_disco_url/2]).

-define(OK_HEADER, "HTTP/1.1 200 OK\n"
                   "Status: 200 OK\n"
                   "Content-type: text/plain\n\n").


-record(failinfo, {inputs, taskblack}).

% In theory we could keep the HTTP connection pending until the job
% finishes but in practice long-living HTTP connections are a bad idea.
% Thus, the HTTP request spawns a new process, job_coordinator, that 
% takes care of coordinating the whole map-reduce show, including
% fault-tolerance. The HTTP request returns immediately. It may poll
% the job status e.g. by using handle_ctrl's get_results.
new_coordinator(PostData) ->
        TMsg = "couldn't start a new job coordinator in 10s (master busy?)",
        S = self(),
        P = spawn(fun() -> init_job_coordinator(S, PostData) end),
        receive
                {P, ok} -> ok;
                {P, eexist} -> throw("job already exists");
                {P, env_failed} -> throw("creating job directories failed "
                                         "(disk full?)");
                {P, invalid_jobdesc} -> throw("invalid job description");
                {P, timeout} -> throw(TMsg);
                _ -> throw("job coordinator failed")
        after 20000 ->
                throw(TMsg)
        end.     

% job_coordinator() orchestrates map/reduce tasks for a job
init_job_coordinator(Parent, PostData) ->
        Msg = netstring:decode_netstring_fd(PostData),
        case catch find_values(Msg) of
                {'EXIT', _} ->
                        Parent ! {self(), invalid_jobdesc};
                Params ->
                        init_job_coordinator(Parent, Params, PostData)
        end.

init_job_coordinator(Parent, {Name, _, _, _, _} = Params, PostData) ->
        EnvStatus = (catch prepare_environment(Name)),
        case gen_server:call(event_server, {new_job, Name, self()}, 10000) of
                eexist ->
                        Parent ! {self(), eexist};
                _ when EnvStatus =/= ok ->
                        Parent ! {self(), env_failed};
                ok ->
                        save_params(Name, PostData),
                        Parent ! {self(), ok},
                        job_coordinator(Params);
                _ ->
                        Parent ! {self(), timeout}
        end.

check_mkdir(ok) -> ok;
check_mkdir({error, eexist}) -> ok;
check_mkdir(_) -> throw("creating directory failed").

make_local({_, true}, Root, Home) ->
        {ok, LocalRoot} = application:get_env(disco_localdir),
        [R, _] = filename:split(Home),
        check_mkdir(file:make_dir(filename:join(LocalRoot, R))),
        check_mkdir(file:make_dir(filename:join(LocalRoot, Home))),
        Dst = filename:join([LocalRoot, Home, "events"]),
        Src = filename:join([Root, Home, "events"]),
        ok = file:make_symlink(Dst, Src);
make_local(_, _, _) -> ok.

prepare_environment(Name) ->
        C = string:chr(Name, $/) + string:chr(Name, $.),
        if C > 0 ->
                throw("Invalid name");
        true -> ok
        end,
        {ok, Root} = application:get_env(disco_root),
        Home = disco_server:jobhome(Name),
        [R, _] = filename:split(Home),
        check_mkdir(file:make_dir(filename:join(Root, R))),
        check_mkdir(file:make_dir(filename:join(Root, Home))),
        make_local(application:get_env(resultfs_enabled), Root, Home).

save_params(Name, PostData) ->
        {ok, Root} = application:get_env(disco_root),
        Home = disco_server:jobhome(Name),
        ok = file:write_file(filename:join([Root, Home, "params"]), PostData).

find_values(Msg) ->
        {value, {_, NameB}} = lists:keysearch(<<"name">>, 1, Msg),
        Name = binary_to_list(NameB),

        {value, {_, InputStr}} = lists:keysearch(<<"input">>, 1, Msg),
        Inputs = lists:map(fun(Inp) ->
                        case string:tokens(Inp, "\n") of
                                [X] -> list_to_binary(X);
                                Y -> [list_to_binary(X) || X <- Y]
                        end
        end, string:tokens(binary_to_list(InputStr), " ")),

        {value, {_, NMapsStr}} = lists:keysearch(<<"nr_maps">>, 1, Msg),
        NMap = list_to_integer(binary_to_list(NMapsStr)),
        
        {value, {_, NRedStr}} = lists:keysearch(<<"nr_reduces">>, 1, Msg),
        NRed = list_to_integer(binary_to_list(NRedStr)),
                
        case lists:keysearch(<<"reduce">>, 1, Msg) of 
                false -> {Name, Inputs, NMap, NRed, false};
                _Else -> {Name, Inputs, NMap, NRed, true}
        end.

gethostname() ->
        {ok, SecondaryHostname} = inet:gethostname(),
        case application:get_env(disco_master_host) of
                {ok, ""} -> SecondaryHostname;
                {ok, Val} -> Val
        end.

set_disco_url(undefined, Msg) ->
        {value, {_, SPort}} =
                lists:keysearch(<<"SERVER_PORT">>, 1, Msg),
        {ok, Name} = application:get_env(disco_name),
        HostN = gethostname(),
        DiscoUrl = lists:flatten(["http://", HostN, ":",
                binary_to_list(SPort), "/disco/master/_", Name, "/"]),
        application:set_env(disco, disco_url, DiscoUrl);
set_disco_url(_, _) -> ok.

% handle() receives the SCGI request and reads POST data.
handle(Socket, Msg) ->
        {value, {_, CLenStr}} = lists:keysearch(<<"CONTENT_LENGTH">>, 1, Msg),
        CLen = list_to_integer(binary_to_list(CLenStr)),
        
        set_disco_url(application:get_env(disco_url), Msg),

        % scgi_recv_msg used instead of gen_tcp to work around gen_tcp:recv()'s
        % 16MB limit.
        {ok, PostData} = scgi:recv_msg(Socket, <<>>, CLen),
        Reply = case catch new_coordinator(PostData) of
                ok -> ["job started"];
                E -> ["ERROR: ", E]
        end,    
        gen_tcp:send(Socket, [?OK_HEADER, Reply]).


% work() is the heart of the map/reduce show. First it distributes tasks
% to nodes. After that, it starts to wait for the results and finally
% returns when it has gathered all the results.

%. 1. Basic case: Tasks to distribute, maximum number of concurrent tasks (N)
%  not reached.
work([{PartID, Input}|Inputs], Mode, Name, N, Max, Res) when N =< Max ->
        ok = gen_server:call(disco_server, {new_worker, 
                {Name, PartID, Mode, [], Input}}),
        work(Inputs, Mode, Name, N + 1, Max, Res);

% 2. Tasks to distribute but the maximum number of tasks are already running.
% Wait for tasks to return. Note that wait_workers() may return with the same
% number of tasks still running, i.e. N = M.
work([_|_] = IArg, Mode, Name, N, Max, Res) when N > Max ->
        M = wait_workers(N, Res, Name, Mode),
        work(IArg, Mode, Name, M, Max, Res);

% 3. No more tasks to distribute. Wait for tasks to return.
work([], Mode, Name, N, Max, Res) when N > 0 ->
        M = wait_workers(N, Res, Name, Mode),
        work([], Mode, Name, M, Max, Res);

% 4. No more tasks to distribute, no more tasks running. Done.
work([], _Mode, _Name, 0, _Max, _Res) -> ok.

% wait_workers receives messages from disco_server:clean_worker() that is
% called when a worker exits. 

% Error condition: should not happen.
wait_workers(0, _Res, _Name, _Mode) ->
        throw("Nothing to wait");

wait_workers(N, {Results, Failures}, Name, Mode) ->
        M = N - 1,
        receive
                {job_ok, {OobKeys, Result}, {Node, PartID}} -> 
                        event_server:event(Name, 
                                "Received results from ~s:~B @ ~s.",
                                        [Mode, PartID, Node], {task_ready, Mode}),
                        gen_server:cast(oob_server,
                                {store, Name, Node, OobKeys}),
                        ets:insert(Results, {Result, ok}),
                        M;

                {data_error, {_Msg, Input}, {Node, PartID}} ->
                        handle_data_error(Name, Input,
                          PartID, Mode, Node, Failures),
                        N;
                        
                {job_error, _Error, {_Node, _PartID}} ->
                        throw(logged_error);
                        
                {error, Error, {Node, PartID}} ->
                        event_server:event(Name, 
                                "ERROR: Worker crashed in ~s:~B @ ~s: ~p",
                                        [Mode, PartID, Node, Error], []),

                        throw(logged_error);

                {master_error, Error} ->
                        event_server:event(Name, 
                                "ERROR: Master terminated the job: ~s",
                                        [Error], []),
                        throw(logged_error);
                        
                Error ->
                        event_server:event(Name, 
                                "ERROR: Received an unknown error: ~p",
                                        [Error], []),
                        throw(logged_error)
        end.

% data_error signals that a task failed on an error that is not likely
% to repeat when the task is ran on another node. The function
% handle_data_error() schedules the failed task for a retry, with the
% failing node in its blacklist. If a task fails too many times, as 
% determined by check_failure_rate(), the whole job will be terminated.
handle_data_error(Name, FailedInput, PartID, Mode, Node, Failures) ->
        [{_, #failinfo{taskblack = Taskblack, inputs = Inputs}}] =
                ets:lookup(Failures, PartID),
        
        ok = check_failure_rate(Name, PartID, Mode, length(Taskblack)),
        NInputs = if length(Inputs) > 1 ->
                [{X, N} || {X, N} <- Inputs, X =/= FailedInput];
        true ->
                Inputs
        end,

        NTaskblack = [Node|Taskblack],
        ets:insert(Failures, {PartID,
                #failinfo{taskblack = NTaskblack, inputs = NInputs}}),
        
        ok = gen_server:call(disco_server, {new_worker, 
                {Name, PartID, Mode, NTaskblack, NInputs}}).

check_failure_rate(Name, PartID, Mode, L) ->
        V = case application:get_env(max_failure_rate) of
                undefined -> L > 3;
                {ok, N} -> L > N
        end,
        if V ->
                event_server:event(Name, 
                        "ERROR: ~s:~B failed ~B times. Aborting job.",
                                [Mode, PartID, L], []),
                throw(logged_error);
        true -> 
                ok
        end.

kill_job(Name, Msg, P, Type) ->
        event_server:event(Name, Msg, P, []),
        gen_server:call(disco_server, {kill_job, Name}),
        gen_server:cast(event_server, {job_done, Name}),
        exit(Type).

resultfs_enabled() ->
    S = case os:getenv("DISCO_FLAGS") of
            false -> [];
            X -> string:tokens(string:to_lower(X), " ")
    end,
    [ 1 || "resultfs" <- S] =/= [].

move_to_resultfs(_, _, false) -> ok;
move_to_resultfs(Name, R, _) ->
        event_server:event(Name, "Moving results to resultfs", [], []),
        case catch garbage_collect:move_results(R) of
                ok -> ok;
                {error, Node, Error} ->
                        kill_job(Name,
                        "ERROR: Moving to resultfs failed on ~s: ~s",
                                [Node, Error], logged_error);
                timeout ->
                        kill_job(Name,
                        "ERROR: Moving to resultfs failed (timeout)",
                                [], logged_error);
                Error ->
                        kill_job(Name, 
                        "ERROR: Moving to resultfs failed: ~p", 
                                [Error], unknown_error)
        end.

% run_task() is a common supervisor for both the map and reduce tasks.
% Its main function is to catch and report any errors that occur during
% work() calls.
run_task(Inputs, Mode, Name, MaxN) ->
        Failures = ets:new(error_log, [set]),
        Results = ets:new(results, [set]),
        ets:insert(Failures, [{PartID,
                #failinfo{taskblack = [], inputs = Input}} ||
                        {PartID, Input} <- Inputs]),

        case catch work(Inputs, Mode, Name, 0, MaxN, {Results, Failures}) of
                ok -> ok;
                logged_error ->
                        kill_job(Name, 
                        "ERROR: Job terminated due to the previous errors",
                                [], logged_error);
                Error ->
                        kill_job(Name, 
                        "ERROR: Job coordinator failed unexpectedly: ~p", 
                                [Error], unknown_error)
        end,

        R = [list_to_binary(X) || {X, _} <- ets:tab2list(Results)],
        move_to_resultfs(Name, R, resultfs_enabled()),
        ets:delete(Results),
        ets:delete(Failures),
        R.

        
job_coordinator({Name, Inputs, NMap, NRed, DoReduce}) ->
        event_server:event(Name, "Starting job", [], 
                {job_data, {NMap, NRed, DoReduce, Inputs}}),

        RedInputs = if NMap == 0 ->
                Inputs;
        true ->
                event_server:event(Name, "Map phase", [], {}),
                MapResults = run_task(map_input(Inputs), "map", Name, NMap),
                event_server:event(Name, "Map phase done", [], []),
                MapResults
        end,

        if DoReduce ->
                event_server:event(Name, "Starting reduce phase", [], {}),
                RedResults = run_task(reduce_input(Name, RedInputs, NRed),
                        "reduce", Name, NRed),
                
                if NMap > 0 ->
                        garbage_collect:remove_map_results(RedInputs);
                true -> ok
                end,
                
                event_server:event(Name, "Reduce phase done", [], []),
                event_server:event(Name, "READY", [], {ready, RedResults});
        true ->
                event_server:event(Name, "READY", [], {ready, RedInputs})
        end,
        gen_server:cast(event_server, {job_done, Name}).

map_input(Inputs) ->
        Prefs = lists:map(fun
                (Inp) when is_list(Inp) ->
                        [{<<"'", X/binary, "' ">>, pref_node(X)} || X <- Inp];
                (Inp) ->
                        [{<<"'", Inp/binary, "' ">>, pref_node(Inp)}]
        end, Inputs),
        lists:zip(lists:seq(0, length(Prefs) - 1), Prefs).

reduce_input(Name, Inputs, NRed) ->
        V = lists:any(fun erlang:is_list/1, Inputs),
        if V ->
                event_server:event(Name,
                        "ERROR: Reduce doesn't support redundant inputs"),
                throw({error, "redundant inputs in reduce"});
        true -> ok
        end,
        B = << <<"'", X/binary, "' ">> || X <- Inputs >>,

        % TODO: We could prioritize preferences according to partition sizes.
        N = length(Inputs),
        D = dict:from_list(lists:zip(lists:seq(1, N),
                [pref_node(X) || X <- Inputs])),
        [{X, [{B, dict:fetch(random:uniform(N), D)}]} ||
                X <- lists:seq(0, NRed - 1)].

% pref_node() suggests a preferred node for a task (one preserving locality)
% given the url of its input.

pref_node(X) when is_binary(X) -> pref_node(binary_to_list(X));
pref_node("disco://" ++ _ = Uri) -> remote_uri(Uri);
pref_node("dir://" ++ _ = Uri) -> remote_uri(Uri);
pref_node("http://" ++ _ = Uri) -> remote_uri(Uri);
pref_node("dfs://" ++ _ = Uri) -> remote_uri(Uri);
pref_node(_) -> false.
remote_uri(Uri) -> string:sub_word(Uri, 2, $/).
