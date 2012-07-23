-module(lock_server).
-behaviour(gen_server).

-include("gs_util.hrl").

-export([start_link/0, lock/3]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(PROC_TIMEOUT, 10 * 60 * 1000).

-type lockname() :: nonempty_string().
-record(state, {waiters :: gb_tree(),
                procs   :: gb_tree()}).
-type state() :: #state{}.

-spec start_link() -> no_return().
start_link() ->
    process_flag(trap_exit, true),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, _Server} -> ok;
	{error, {already_started, _Server}} -> exit(already_started)
    end,
    receive
        {'EXIT', _, Reason0} -> exit(Reason0)
    end.

-spec lock(lockname(), fun(() -> ok), non_neg_integer()) -> ok | {error, term()}.
lock(JobName, Proc, Timeout) ->
    gen_server:call(?MODULE, {wait, JobName, Proc}, Timeout).

-spec init(_) -> gs_init().
init(_Args) ->
    process_flag(trap_exit, true),
    {ok, #state{waiters = gb_trees:empty(),
                procs = gb_trees:empty()}}.

-spec handle_call({wait, lockname(), fun(() -> ok)}, from(), state())
                 -> gs_noreply().
handle_call({wait, Key, Proc}, From, #state{procs = Procs,
                                            waiters = Waiters} = S) ->
    {WaiterList1, S1} =
        case gb_trees:lookup(Key, Waiters) of
            none ->
                Pid = spawn_link(Proc),
                {ok, TRef} = timer:kill_after(?PROC_TIMEOUT, Pid),
                Procs1 = gb_trees:insert(Pid, {Key, TRef}, Procs),
                {[], S#state{procs = Procs1}};
            {value, WaiterList} ->
                {WaiterList, S}
        end,
    Waiters1 = gb_trees:enter(Key, [From|WaiterList1], Waiters),
    {noreply, S1#state{waiters = Waiters1}}.

-spec handle_info({'EXIT', pid(), term()}, state()) -> gs_noreply().
handle_info({'EXIT', Pid, Reason}, #state{procs = Procs,
                                          waiters = Waiters} = S) ->
    {value, {Key, TRef}} = gb_trees:lookup(Pid, Procs),
    {value, WaiterList} = gb_trees:lookup(Key, Waiters),
    Msg = if Reason =:= normal -> ok; true -> {error, Reason} end,
    _ = [gen_server:reply(From, Msg) || From <- WaiterList],
    _ = timer:cancel(TRef),
    {noreply, S#state{procs = gb_trees:delete(Pid, Procs),
                      waiters = gb_trees:delete(Key, Waiters)}}.

-spec handle_cast(term(), state()) -> gs_noreply().
handle_cast(_, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

