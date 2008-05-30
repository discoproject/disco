
-module(handle_job).
-export([handle/2, job_coordinator/4]).

-define(OK_HEADER, "HTTP/1.1 200 OK\n"
                   "Status: 200 OK\n"
                   "Content-type: text/plain\n\n").

% In theory we could keep the HTTP connection pending until the job
% finishes but in practice long-living HTTP connections are a bad idea.
% Thus, the HTTP request spawns a new process, job_coordinator, that 
% takes care of coordinating the whole map-reduce show, including
% fault-tolerance. The HTTP request returns immediately. It may poll
% the job status e.g. by using handle_ctrl's get_results.
new_coordinator(Name, Msg, PostData) ->
        S = self(),
        P = spawn(fun() -> job_coordinator(S, Name, Msg, PostData) end),
        receive
                {P, ok} -> [?OK_HEADER, "job started"]
        after 5000 ->
                [?OK_HEADER, "couldn't start a new job coordinator"]
        end.     

% init_job() checks that there isn't already a job existing with the same name.
init_job(PostData) ->
        Msg = netstring:decode_netstring_fd(PostData),
        {value, {_, Name}} = lists:keysearch("name", 1, Msg),
        error_logger:info_report([{"New job", Name}]),
        
        case gen_server:call(event_server, {get_job_events, Name}) of
                {ok, []} -> new_coordinator(Name, Msg, list_to_binary(PostData));
                {ok, _Events} -> [?OK_HEADER, 
                                "ERROR: job ", Name, " already exists"]
        end.

% handle() receives the SCGI request and reads POST data.
handle(Socket, Msg) ->
        {value, {_, CLenStr}} = lists:keysearch("CONTENT_LENGTH", 1, Msg),
        CLen = list_to_integer(CLenStr),
        {ok, PostData} = gen_tcp:recv(Socket, CLen, 30000),
        gen_tcp:send(Socket, init_job(PostData)).

% pref_node() suggests a preferred node for a task (one preserving locality)
% given the url of its input.
pref_node([$h, $t, $t, $p, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node([$d, $i, $s, $c, $o, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node([$d, $i, $r, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node(Host) -> Host.

% work() is the heart of the map/reduce show. First it distributes tasks
% to nodes. After that, it starts to wait for the results and finally
% returns when it has gathered all the results.

%. 1. Basic case: Tasks to distribute, maximum number of concurrent tasks (N)
%  not reached.
work([{PartID, Input}|Inputs], Mode, Name, Data, N, Max, Res) when N =< Max ->
        PrefNode = pref_node(Input),
        ok = gen_server:call(disco_server, {new_worker, 
                {Name, PartID, Mode, {PrefNode, []}, Input, Data}}),
        work(Inputs, Mode, Name, Data, N + 1, Max, Res);

% 2. Tasks to distribute but the maximum number of tasks are already running.
% Wait for tasks to return. Note that wait_workers() may return with the same
% number of tasks still running, i.e. N = M.
work([_|_] = IArg, Mode, Name, Data, N, Max, Res) when N > Max ->
        M = wait_workers(N, Res, Name, Mode),
        work(IArg, Mode, Name, Data, M, Max, Res);

% 3. No more tasks to distribute. Wait for tasks to return.
work([], Mode, Name, Data, N, Max, Res) when N > 0 ->
        M = wait_workers(N, Res, Name, Mode),
        work([], Mode, Name, Data, M, Max, Res);

% 4. No more tasks to distribute, no more tasks running. Done.
work([], _Mode, _Name, _Data, 0, _Max, _Res) -> ok.

% wait_workers receives messages from disco_server:clean_worker() that is
% called when a worker exits. 

% Error condition: should not happen.
wait_workers(0, _Res, _Name, _Mode) ->
        throw("Nothing to wait");

wait_workers(N, {ResNodes, ErrLog}, Name, Mode) ->
        M = N - 1,
        receive
                {job_ok, _Result, {Node, PartID}} -> 
                        event_server:event(Name, 
                                "Received results from ~s:~B @ ~s.",
                                        [Mode, PartID, Node], [task_ready, Mode]),
                        ets:insert(ResNodes, {lists:flatten(["dir://", Node, "/",
                                Mode, "/", Name]), ok}),
                        M;

                {data_error, {_Msg, Input, Data}, {Node, PartID}} ->
                        handle_data_error(Name, Input, Data,
                                PartID, Mode, Node, ErrLog),
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
handle_data_error(Name, Input, Data, PartID, Mode, Node, ErrLog) ->
        ets:insert(ErrLog, {PartID, Node}),
        {_, ErrNodes} = lists:unzip(ets:lookup(ErrLog, PartID)),
        ok = check_failure_rate(Name, PartID, Mode, length(ErrNodes)),
        ok = gen_server:call(disco_server, {new_worker, 
                {Name, PartID, Mode, {none, ErrNodes}, Input, Data}}).

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

find_values(Msg) ->
        {value, {_, InputStr}} = lists:keysearch("input", 1, Msg),
        Inputs = string:tokens(InputStr, " "),

        {value, {_, NMapsStr}} = lists:keysearch("nr_maps", 1, Msg),
        NMap = list_to_integer(NMapsStr),
        
        {value, {_, NRedStr}} = lists:keysearch("nr_reduces", 1, Msg),
        NRed = list_to_integer(NRedStr),
                
        case lists:keysearch("reduce", 1, Msg) of 
                false -> {Inputs, NMap, NRed, false};
                _Else -> {Inputs, NMap, NRed, true}
        end.

% supervise_work() is a common supervisor for both the map and reduce tasks.
% Its main function is to catch and report any errors that occur during
% work() calls.
supervise_work(Inputs, Mode, Name, Msg, MaxN) ->
        ErrLog = ets:new(error_log, [bag]),
        ResNodes = ets:new(node_results, [set]),
        case catch work(Inputs, Mode, Name, Msg,
                        0, MaxN, {ResNodes, ErrLog}) of
                ok -> ok;
                logged_error ->
                        event_server:event(Name, 
                        "ERROR: Job terminated due to the previous errors",
                                [], []),
                        gen_server:call(disco_server, {kill_job, Name}),
                        exit(logged_error);
                Error ->
                        event_server:event(Name, 
                        "ERROR: Job coordinator failed unexpectedly: ~p", 
                                [Error], []),
                        gen_server:call(disco_server, {kill_job, Name}),
                        exit(unknown_error)
        end,
        ets:delete(ErrLog),
        ResNodes.

% job_coordinator() encapsulates the map/reduce steps:
% 1) Parse the request
% 2) Run map
% 3) Optionally run reduce
job_coordinator(Parent, Name, Msg, PostData) ->
        event_server:event(Name, "Job coordinator starts", [], [start, self()]),
        Parent ! {self(), ok},

        {MapInputs, NMap, NRed, DoReduce} = case catch find_values(Msg) of
                {A, B, C, D} -> {A, B, C, D}; 
                MError -> event_server:event(Name, 
                        "ERROR: Couldn't parse the job packet: ~p", [MError]),
                        exit(logged_error)
        end,

        event_server:event(Name, "Starting map phase", [], 
                [map_data, self(), NMap, NRed, DoReduce, MapInputs]),

        EnumMapInputs = lists:zip(
                lists:seq(0, length(MapInputs) - 1), MapInputs),
        MapResults = supervise_work(EnumMapInputs, "map", Name, PostData, NMap),
        MapList = [X || {X, _} <- ets:tab2list(MapResults)],
        RedInputs = [{X, MapList} || X <- lists:seq(0, NRed - 1)],
        ets:delete(MapResults),

        event_server:event(Name, "Map phase done", [], []),

        if DoReduce ->
                event_server:event(Name, "Starting reduce phase", [],
                        [red_data, RedInputs]),
                RedResults = supervise_work(RedInputs, "reduce", Name, PostData, NRed),
                event_server:event(Name, "Reduce phase done", [], []),
                event_server:event(Name, "READY", [], [ready, 
                        [list_to_binary(X) ||
                                {X, _} <- ets:tab2list(RedResults)]]),
                ets:delete(RedResults);
        true ->
                event_server:event(Name, "READY", [], [ready,
                        [list_to_binary(X) || X <- MapList]])
        end.
