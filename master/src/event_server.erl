-module(event_server).
-behaviour(gen_server).

% Cluster state.
-export([update_nodes/1]).
% Job notification.
-export([new_job/3, end_job/1, clean_job/1]).
% Retrieval.
-export([get_jobs/0, get_jobs/1, get_jobinfo/1, get_job_msgs/3,
         get_stage_results/2, get_results/1, get_job_event_handler/1]).

% Event logging.
-export([event/4, event/5, event/6, job_done_event/2]).
% Server.
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([json_list/1]).

-include("gs_util.hrl").
-include("event.hrl").

-type joblist_entry() :: {JobName :: jobname(),
                          job_status(),
                          StartTime :: erlang:timestamp()}.

-type stage_dict() :: disco_dict(stage_name(), non_neg_integer()).

-export_type([event/0, job_eventinfo/0]).

% Cluster configuration.

-spec update_nodes([host()]) -> ok.
update_nodes(Hosts) ->
    gen_server:cast(?MODULE, {update_nodes, Hosts}).

% Job notification.

-spec new_job(jobname(), pid(), [stage_name()]) -> {ok, jobname(), [host()]}.
new_job(Prefix, JobCoord, Stages) ->
    gen_server:call(?MODULE, {new_job, Prefix, JobCoord, Stages}, 10000).

-spec end_job(jobname()) -> ok.
end_job(JobName) ->
    gen_server:cast(?MODULE, {job_done, JobName}).

-spec clean_job(jobname()) -> ok.
clean_job(JobName) ->
    gen_server:cast(?MODULE, {clean_job, JobName}).

% Retrieval.

-spec get_jobs() -> {ok, [joblist_entry()]}.
get_jobs() ->
    gen_server:call(?MODULE, get_jobs, infinity).

-spec get_jobs(Master :: node()) -> {ok, [joblist_entry()]}.
get_jobs(Master) ->
    gen_server:call({?MODULE, Master}, get_jobs, infinity).

-spec get_jobinfo(jobname()) -> invalid_job | {ok, job_eventinfo()}.
get_jobinfo(JobName) ->
    gen_server:call(?MODULE, {get_jobinfo, JobName}).

-spec get_job_event_handler(jobname()) -> invalid_job | pid().
get_job_event_handler(JobName) ->
    gen_server:call(?MODULE, {get_job_event_handler, JobName}).

-spec get_job_msgs(jobname(), string(), integer()) -> {ok, [binary()]}.
get_job_msgs(JobName, Q, N) ->
    gen_server:call(?MODULE, {get_job_msgs, JobName, string:to_lower(Q), N}).

-spec get_stage_results(jobname(), stage_name()) -> invalid_job | not_ready
                                        | {ok, [[url()]]}.
get_stage_results(JobName, StageName) ->
    gen_server:call(?MODULE, {get_stage_results, JobName, StageName}).

-spec get_results(jobname()) -> invalid_job | {ready, pid(), [url() | [url()]]}
                                    | {job_status(), pid()}.
get_results(JobName) ->
    gen_server:call(?MODULE, {get_results, JobName}).

% Event logging.

-spec event(jobname(), nonempty_string(), list(), none | event()) -> ok.
event(JobName, MsgFormat, Args, Event) ->
    event("master", JobName, MsgFormat, Args, Event).

-spec event(host(), jobname(), nonempty_string(), list(), none | event()) -> ok.
event(Host, JobName, MsgFormat, Args, Event) ->
    event(?MODULE, Host, JobName, MsgFormat, Args, Event).

-spec event(server(), host(), jobname(), nonempty_string(),
            list(), none | event()) -> ok.
event(EventServer, Host, JobName, MsgFormat, Args, Event) ->
    RawMsg = disco:format(MsgFormat, Args),
    Json = try mochijson2:encode(list_to_binary(RawMsg))
           catch _:_ ->
                   Hex = ["WARNING: Binary message data: ",
                          [io_lib:format("\\x~2.16.0b",[N]) || N <- RawMsg]],
                   mochijson2:encode(list_to_binary(Hex))
           end,
    Msg = list_to_binary(Json),
    gen_server:cast(EventServer, {add_job_event, Host, JobName, Msg, Event}).

-spec job_done_event(jobname(), [url()|[url()]]) -> ok.
job_done_event(JobName, Results) ->
    gen_server:cast(?MODULE, {job_done_event, JobName, Results}).

% Server.

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Event server starts"),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

-type event_dict() :: disco_dict(jobname(), pid()).

-record(state, {events = dict:new() :: event_dict(),
                hosts  = []         :: [host()]}).
-type state() :: #state{}.

-spec init(_) -> gs_init().
init(_Args) ->
    {ok, #state{}}.

-spec handle_call(term(), from(), state()) -> gs_reply(term()) | gs_noreply().

handle_call({new_job, JobPrefix, Pid, Stages}, _From,
            #state{events = Events0, hosts = Hosts} = S) ->
    case unique_key(JobPrefix, Events0) of
        invalid_prefix ->
            {reply, {error, invalid_prefix}, S};
        {ok, JobName} ->
            Events = dict:store(JobName, job_event:start_link(Pid, JobName, Stages), Events0),
            {reply, {ok, JobName, Hosts}, S#state{events = Events}}
    end;
handle_call(get_jobs, _F, #state{events = Events} = S) ->
    {reply, do_get_jobs(Events), S};
handle_call({get_job_event_handler, JobName}, _F, #state{events = Events} = S) ->
    {reply, do_get_job_event_handler(JobName, Events), S};
handle_call({get_job_msgs, JobName, Query, N}, _F, #state{events = Events} = S) ->
    {reply, do_get_job_msgs(JobName, Query, N, Events), S};
handle_call({get_results, JobName}, _F, #state{events = Events} = S) ->
    {reply, do_get_results(JobName, Events), S};
handle_call({get_stage_results, JobName, StageName}, _F, #state{events = Events} = S) ->
    {reply, do_get_stage_results(JobName, StageName, Events), S};
handle_call({get_jobinfo, JobName}, _F, #state{events = Events} = S) ->
    {reply, do_get_jobinfo(JobName, Events), S}.

-spec handle_cast(term(), state()) -> gs_noreply().

handle_cast({update_nodes, Hosts}, S) ->
    {noreply, S#state{hosts = Hosts}};
handle_cast({add_job_event, Host, JobName, Msg, Event}, S) ->
    {noreply, do_add_job_event(Host, JobName, Msg, Event, S)};
handle_cast({job_done_event, JobName, Results}, S) ->
    {noreply, do_job_done_event(JobName, Results, S)};
% XXX: Some aux process could go through the jobs periodically and
% check that the job coord is still alive - if not, call job_done for
% the zombie job.
handle_cast({job_done, JobName}, S) ->
    {noreply, do_job_done(JobName, S)};
handle_cast({clean_job, JobName}, S) ->
    #state{events = Events} = S1 = do_job_done(JobName, S),
    delete_jobdir(JobName),
    S2 = case dict:find(JobName, Events) of
        error -> S1;
        {ok, JE} ->
            job_event:purge(JE),
            S1#state{events = dict:erase(JobName, Events)}
    end,
    {noreply, S2#state{events = dict:erase(JobName, Events)}}.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(Msg, State) ->
    lager:warning("Unknown message received: ~p", [Msg]),
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% Server implemention.

-spec do_get_jobs(event_dict()) -> {ok, [joblist_entry()]}.
do_get_jobs(Events) ->
    Jobs = dict:fold(fun (Name, JobEnt, Acc) ->
                             Start = job_event:get_start(JobEnt),
                             [{Name, job_event:get_status(JobEnt), Start}|Acc]
                     end, [], Events),
    {ok, Jobs}.

-spec do_get_job_msgs(jobname(), string(), integer(), event_dict()) -> {ok, [binary()]}.
do_get_job_msgs(JobName, [], N0, Events) ->
    N = min(N0, 1000),
    case dict:find(JobName, Events) of
        {ok, JE} ->
            {ok, job_event:get_msgs(JE, N)};
        error ->
            {ok, json_list(tail_log(JobName, N))}
    end;
do_get_job_msgs(JobName, Query, N0, _Events) ->
    {ok, json_list(grep_log(JobName, Query, min(N0, 1000)))}.

-spec do_get_job_event_handler(jobname(), event_dict()) -> invalid_job | pid().
do_get_job_event_handler(JobName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, JE} ->
            JE
    end.

-spec do_get_results(jobname(), event_dict())
                    -> invalid_job | {job_status(), pid()}
                           | {ready, pid(), [url() | [url()]]}.
do_get_results(JobName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, JE} -> job_event:get_results(JE)
    end.

-spec do_get_stage_results(jobname(), stage_name(), event_dict())
                        -> invalid_job | not_ready
                               | {ok, [[url()]]}.
do_get_stage_results(JobName, StageName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, JE} ->
            job_event:get_stage_results(JE, StageName)
    end.

-spec do_get_jobinfo(jobname(), event_dict()) -> invalid_job | {ok, job_eventinfo()}.
do_get_jobinfo(JobName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, JE} ->
            {ok, job_event:get_info(JE)}
    end.

-spec do_add_job_event(host(), jobname(), binary(), event(), state()) -> state().
do_add_job_event(Host, JobName, Msg, Event, #state{events = Events} = S) ->
    case dict:is_key(JobName, Events) of
        true -> add_event(Host, JobName, Msg, Event, S);
        false -> S
    end.

add_event(Host, JobName, Msg, Event, #state{events = Events} = S) ->
  {ok, JE} = dict:find(JobName, Events),
  job_event:add_event(JE, Host, Msg, Event),
  S.

-spec do_job_done(jobname(), state()) -> state().
do_job_done(JobName, #state{events = Events} = S) ->
    case dict:find(JobName, Events) of
        error -> S;
        {ok, JE} ->
            job_event:done(JE),
            S
    end.

-spec do_job_done_event(jobname(), [url() | [url()]], state()) -> state().
do_job_done_event(JobName, Results, #state{events = Events} = S) ->
    case dict:find(JobName, Events) of
        {ok, JE} ->
            Start = job_event:get_start(JE),
            Msg = disco:format("\"READY: Job done in ~s\"",
                               [disco:format_time_since(Start)]),
            Event = {ready, Results},
            add_event("master", JobName, list_to_binary(Msg), Event, S);
        error ->
            S
    end.

% Misc utilities

-spec json_list([binary()]) -> [binary()].
json_list(List) -> json_list(List, []).
json_list([], _) -> [];
json_list([X], L) ->
    [<<"[">>, lists:reverse([X|L]), <<"]">>];
json_list([X|R], L) ->
    json_list(R, [<<X/binary, ",">>|L]).

-spec unique_key(jobname(), event_dict()) -> invalid_prefix | {ok, jobname()}.
unique_key(Prefix, Dict) ->
    C = string:chr(Prefix, $/) + string:chr(Prefix, $.),
    if C > 0 ->
        invalid_prefix;
    true ->
        {MegaSecs, Secs, MicroSecs} = now(),
        Key = disco:format("~s@~.16b:~.16b:~.16b", [Prefix, MegaSecs, Secs, MicroSecs]),
        case dict:is_key(Key, Dict) of
            false -> {ok, Key};
            true -> unique_key(Prefix, Dict)
        end
    end.

% Utilities to process job event file.

event_log(JobName) ->
    filename:join(disco:jobhome(JobName), "events").

tail_log(JobName, N) ->
    Tail = string:tokens(os:cmd(["tail -n ", integer_to_list(N), " ",
                                 event_log(JobName),
                                 " 2>/dev/null"]), "\n"),
    [list_to_binary(L) || L <- lists:reverse(Tail)].

grep_log(JobName, Query, N) ->
    % We dont want execute stuff like "grep -i `rm -Rf *` ..." so
    % only whitelisted characters are allowed in the query
    CQ = re:replace(Query, "[^a-zA-Z0-9:-_!@]", "", [global, {return, list}]),
    Lines = string:tokens(os:cmd(["grep -i \"", CQ ,"\" ",
                                  event_log(JobName),
                                  " 2>/dev/null | head -n ", integer_to_list(N)]), "\n"),
    [list_to_binary(L) || L <- lists:reverse(Lines)].

delete_jobdir(JobName) ->
    Safe = string:chr(JobName, $.) + string:chr(JobName, $/),
    if Safe =:= 0 ->
            _ = os:cmd("rm -Rf " ++ disco:jobhome(JobName)),
            ok;
       true ->
            ok
    end.
