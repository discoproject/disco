-module(disco_worker).
-behaviour(gen_server).

-export([start_worker/2]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("disco.hrl").

-record(state, {master :: node(),
                task :: task(),
                port :: port(),
                child_pid :: 'none' | string(),
                last_event :: timer:timestamp(),
                start_time :: timer:timestamp(),
                linecount :: non_neg_integer(),
                errlines :: message_buffer:message_buffer(),
                event_counter :: non_neg_integer(),
                event_stream :: event_stream:event_stream(),
                results :: string()}).

-define(RATE_WINDOW, 100000). % 100ms
-define(RATE_LIMIT, 25).
-define(ERRLINES_MAX, 100).
-define(OOB_MAX, 1000).

start_worker(Master, Task) ->
    gen_server:start(?MODULE, {Master, Task}, [{timeout, 30000}]).

init({Master, Task}) ->
    erlang:monitor(process, Task#task.from),
    WorkerPid = self(),
    spawn(fun () ->
                  WorkerPid ! {work, jobhome(Task#task.jobname, Master)}
          end),
    {ok,
     #state{master = Master,
            task = Task,
            port = none,
            child_pid = none,
            last_event = now(),
            start_time = now(),
            linecount = 0,
            errlines = message_buffer:new(?ERRLINES_MAX),
            event_counter = 0,
            event_stream = event_stream:new(),
            results = []},
     60000}.

worker_send(Data, #state{port = Port}) ->
    Message = dencode:encode(Data),
    Length = list_to_binary(integer_to_list(size(Message))),
    port_command(Port, <<Length/binary, "\n", Message/binary, "\n">>).

event(Event, #state{master = Master, task = Task}) ->
    event_server:task_event(Task, Event, {}, disco:host(node()), {event_server, Master}).

handle_event({event, {<<"DAT">>, _Time, _Tags, Message}}, State) ->
    {stop, {shutdown, {error, Message}}, State};

handle_event({event, {<<"END">>, _Time, _Tags, _Message}}, State) ->
    Message = "Task finished in " ++ disco:format_time_since(State#state.start_time),
    event({<<"DONE">>, Message}, State),
    {stop, {shutdown, {done, State#state.results}}, State};

handle_event({event, {<<"ERR">>, _Time, _Tags, Message}}, State) ->
    {stop, {shutdown, {fatal, Message}}, State};

handle_event({event, {<<"JOB">>, _Time, _Tags, _Message}},
             #state{task = Task} = State) ->
    event({<<"TSK">>, "Task info requested"}, State),
    JobHome = jobhome(Task#task.jobname),
    worker_send(list_to_binary(jobpack:jobfile(JobHome)), State),
    {noreply, State};

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
             #state{task = Task} = State) ->
    event({<<"TSK">>, "Task info requested"}, State),
    worker_send(dict:from_list([{<<"taskid">>, Task#task.taskid},
                                {<<"mode">>, list_to_binary(Task#task.mode)},
                                {<<"jobname">>, list_to_binary(Task#task.jobname)},
                                {<<"host">>, list_to_binary(disco:host(node()))}]), State),
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

handle_info(timeout, State) ->
    {stop, {shutdown, {fatal, "Worker timed out"}}, State};
handle_info({work, JobHome}, #state{task = Task, port = none} = State) ->
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
    {noreply, State#state{port = open_port({spawn, Command}, Options)}};

handle_info({_Port, {data, Data}}, #state{event_stream = EventStream} = State) ->
    EventStream1 = event_stream:feed(Data, EventStream),
    {next_stream, {_NextState, EventState}} = EventStream1,
    handle_event(EventState, State#state{event_stream = EventStream1,
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

jobhome(JobName) ->
    disco:jobhome(JobName, disco_node:home()).

jobhome(JobName, Master) ->
    JobHome = jobhome(JobName),
    JobAtom = list_to_atom(disco:hexhash(JobName)),
    case jobpack:extracted(JobHome) of
        true ->
            JobHome;
        false ->
            case catch register(JobAtom, self()) of
                true ->
                    disco:make_dir(JobHome),
                    JobPack = gen_server:call({disco_server, Master},
                                              {jobpack, JobName}),
                    jobpack:save(JobPack, JobHome),
                    jobpack:extract(JobPack, JobHome),
                    JobHome;
                _Else ->
                    case whereis(JobAtom) of
                        undefined ->
                            jobhome(JobName, Master);
                        JobProc ->
                            process_flag(trap_exit, true),
                            link(JobProc),
                            receive
                                {'EXIT', noproc} ->
                                    jobhome(JobName, Master);
                                {'EXIT', JobProc, normal} ->
                                    JobHome
                            end
                    end
            end
    end.
