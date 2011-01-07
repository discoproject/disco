-module(disco_worker).
-behaviour(gen_server).

-export([start_worker/2, worker/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("disco.hrl").

-record(state, {port :: port(),
                task :: task(),
                start_time :: timer:timestamp(),
                eventserver :: pid(),
                eventstream :: event_stream:event_stream(),
                child_pid :: 'none' | string(),
                host :: string(),
                linecount :: non_neg_integer(),
                errlines :: message_buffer:message_buffer(),
                results :: string(),
                debug :: bool(),
                last_event :: timer:timestamp(),
                event_counter :: non_neg_integer()}).

-define(RATE_WINDOW, 100000). % 100ms
-define(RATE_LIMIT, 25).
-define(ERRLINES_MAX, 100).
-define(OOB_MAX, 1000).

start_worker(NodeMon, {_EventServer, Host, Task} = Arg) ->
    NodeMon ! {is_ready, self()},
    receive
        node_ready ->
            % run the worker on the actual host
            spawn_link(disco:node(Host), ?MODULE, worker, [{self(), Arg}])
    after 30000 ->
            exit({fatal, "Node unavailable"})
    end,
    receive
        {has_jobpack, Worker} ->
            Worker;
        {needs_jobpack, Worker} ->
            JobHome = disco:jobhome(Task#task.jobname),
            Worker ! {jobpack, jobpack:read(JobHome)}
    end,
    link(Worker),
    process_flag(trap_exit, true),
    receive
        {'EXIT', Worker, Reason} ->
            exit(Reason)
    end.

worker(Arg) ->
    case gen_server:start_link(?MODULE, Arg, [{timeout, 30000}]) of
        {ok, Worker} ->
            Worker;
        {error, Reason} ->
            exit({fatal, disco:format("Worker init failed: ~p", [Reason])})
    end.

init({Parent, {EventServer, Host, Task}}) ->
    erlang:monitor(process, Task#task.from),
    JobHome = disco:jobhome(Task#task.jobname,
                            filename:join(disco:get_setting("DISCO_DATA"), Host)),
    disco:make_dir(JobHome),
    case jobpack:exists(JobHome) of
        true ->
            Parent ! {has_jobpack, self()};
        false ->
            Parent ! {needs_jobpack, self()},
            receive
                {jobpack, JobPack} ->
                    jobpack:save(JobPack, JobHome),
                    jobpack:extract(JobPack, JobHome)
            end
    end,

    Worker = filename:join(JobHome, binary_to_list(Task#task.worker)),
    file:change_mode(Worker, 8#755),
    Command = "nice -n 19 " ++ Worker,
    Options = [{cd, JobHome},
               {line, 100000},
               binary,
               exit_status,
               use_stdio,
               stderr_to_stdout,
               {env,
                [{"LD_LIBRARY_PATH", "lib"},
                 {"LC_ALL", "C"}] ++
                [{Setting, disco:get_setting(Setting)}
                 || Setting <- disco:settings()]}],

    {ok, #state{port = open_port({spawn, Command}, Options),
                task = Task,
                host = Host,
                child_pid = none,
                eventserver = EventServer,
                eventstream = event_stream:new(),
                linecount = 0,
                start_time = now(),
                last_event = now(),
                event_counter = 0,
                errlines = message_buffer:new(?ERRLINES_MAX),
                debug = disco:get_setting("DISCO_DEBUG") =/= "off",
                results = []}}.

worker_send(Data, #state{port = Port}) ->
    Message = bencode:encode(Data),
    Length = list_to_binary(integer_to_list(size(Message))),
    port_command(Port, <<Length/binary, "\n", Message/binary, "\n">>).

event(Event, #state{task = Task, eventserver = EventServer, host = Host}) ->
    event_server:task_event(Task, Event, {}, Host, EventServer).

handle_event({event, {<<"DAT">>, _Time, _Tags, Message}}, State) ->
    {stop, {shutdown, {error, Message}}, State};

handle_event({event, {<<"END">>, _Time, _Tags, _Message}}, State) ->
    Message = "Task finished in " ++ disco:format_time_since(State#state.start_time),
    event({<<"DONE">>, Message}, State),
    {stop, {shutdown, {done, State#state.results}}, State};

handle_event({event, {<<"ERR">>, _Time, _Tags, Message}}, State) ->
    {stop, {shutdown, {fatal, Message}}, State};

handle_event({event, {<<"PID">>, _Time, _Tags, ChildPID}}, State) ->
    event({"PID", "Child PID is " ++ binary_to_list(ChildPID)}, State),
    worker_send(<<"ok">>, State),
    {noreply, State#state{child_pid = binary_to_list(ChildPID)}};

handle_event({event, {<<"OUT">>, _Time, _Tags, Results}}, State) ->
    event({"OUT", "Results at " ++ Results}, State),
    worker_send(<<"ok">>, State),
    {noreply, State#state{results = Results}};

handle_event({event, {<<"STA">>, _Time, _Tags, Message}}, State) ->
    event({<<"STA">>, Message}, State),
    worker_send(<<"ok">>, State),
    {noreply, State};

handle_event({event, {<<"TSK">>, _Time, _Tags, _Message}},
             #state{task = Task, host = Host} = State) ->
    event({<<"TSK">>, "Task info requested"}, State),
    worker_send(dict:from_list([{<<"id">>, Task#task.taskid},
                                {<<"mode">>, list_to_binary(Task#task.mode)},
                                {<<"jobname">>, list_to_binary(Task#task.jobname)},
                                {<<"host">>, list_to_binary(Host)}]), State),
    {noreply, State};

handle_event({event, {<<"INP">>, _Time, _Tags, _Message}}, #state{task = Task} = State) ->
    worker_send(case Task#task.chosen_input of
                    List when is_list(List) ->
                        List;
                    Binary when is_binary(Binary) ->
                        [Binary]
                end, State),
    {noreply, State};

% rate limited event
handle_event({event, {Type, _Time, _Tags, Payload}}, State) ->
    Now = now(),
    EventGap = timer:now_diff(Now, State#state.last_event),
    if EventGap > ?RATE_WINDOW ->
            event({Type, Payload}, State),
            worker_send(<<"ok">>, State),
            {noreply, State#state{last_event = Now, event_counter = 1}};
       State#state.event_counter > ?RATE_LIMIT ->
            {stop, {shutdown, {fatal, "Event rate limit exceeded. Too many msg() calls?"}}, State};
       true ->
            event({Type, Payload}, State),
            worker_send(<<"ok">>, State),
            {noreply, State#state{event_counter = State#state.event_counter + 1}}
    end;

handle_event({errline, _Line}, #state{errlines = {_Q, overflow, _Max}} = State) ->
    Garbage = message_buffer:to_string(State#state.errlines),
    {stop, {shutdown, {fatal, "Worker failed:\n" ++ Garbage}}, State};
handle_event({errline, Line}, State) ->
    {noreply, State#state{errlines = message_buffer:append(Line, State#state.errlines)}};

handle_event({malformed_event, Reason}, State) ->
    {stop, {shutdown, {fatal, Reason}}, State};
handle_event(_EventState, State) ->
    {noreply, State}.

handle_info({_Port, {data, Data}}, #state{eventstream = EventStream} = State) ->
    EventStream1 = event_stream:feed(Data, EventStream),
    {next_stream, {_NextState, EventState}} = EventStream1,
    handle_event(EventState, State#state{eventstream = EventStream1,
                                         linecount = State#state.linecount + 1});
handle_info({_Port, {exit_status, _Status}}, State) ->
    Reason =  "Worker died. Last words:\n" ++ message_buffer:to_string(State#state.errlines),
    {stop, {shutdown, {fatal, Reason}}, State};

handle_info({'DOWN', _, _, _, Info}, State) ->
    {stop, {shutdown, {fatal, Info}}, State}.

handle_call(kill_worker, _From, State) ->
    {stop, {shutdown, {fatal, "Worker killed"}}, State}.

handle_cast(kill_worker, State) ->
    {stop, {shutdown, {fatal, "Worker killed"}}, State}.

terminate(_Reason, #state{child_pid = Pid}) when Pid =/= none ->
    % Kill child processes of the worker process
    os:cmd("pkill -9 -P " ++ Pid),
    % Kill the worker process
    os:cmd("kill -9 " ++ Pid);
terminate(_Reason, State) ->
    event({<<"WARN">>, "PID unknown: worker could not be killed"}, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
