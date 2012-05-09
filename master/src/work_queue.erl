-module(work_queue).
-export([start_link/1, add_work/2]).

-type work() :: fun(() -> ok).
-type workid() :: non_neg_integer().
-record(q, {max_active   :: workid(),
            next_id = 0  :: workid(),
            active  = [] :: [T],
            waiting = [] :: [{T, work()}]}).
-opaque q() :: #q{}.
-export_type([q/0]).

-spec start_link(non_neg_integer()) -> pid().
start_link(QueueSize) ->
    Q = #q{max_active = QueueSize},
    spawn_link(fun() -> work_server(Q) end).

-spec add_work(pid(), work()) -> ok.
add_work(WkServer, Fun) ->
    WkServer ! {add_work, Fun},
    ok.

-spec end_work(pid(), workid()) -> ok.
end_work(WkServer, Id) ->
    WkServer ! {end_work, Id},
    ok.

-spec work_server(q()) -> no_return().
work_server(Q) ->
    receive
        {add_work, Fun} ->
            Q1 = do_add_work(Q, Fun),
            work_server(Q1);
        {end_work, Id} ->
            Q1 = do_end_work(Q, Id),
            work_server(Q1)
    end.

-spec do_add_work(q(), work()) -> q().
do_add_work(#q{max_active = MaxA, active = A, next_id = Id} = Q, Fun)
  when length(A) < MaxA ->
    Srv = self(),
    % The below assumes that the work Fun() eventually terminate,
    % otherwise we have a problem with the unboundedly growing work
    % queue.
    spawn(fun() -> catch Fun(), end_work(Srv, Id) end),
    Q#q{active = [Id | A], next_id = Id + 1};
do_add_work(#q{waiting = W, next_id = Id} = Q, Fun) ->
    Q#q{waiting = [{Id, Fun} | W], next_id = Id + 1}.

-spec do_end_work(q(), workid()) -> q().
do_end_work(#q{active = A, waiting = []} = Q, Id) ->
    Q#q{active = A -- [Id]};
do_end_work(#q{max_active = MaxA, active = A,
               waiting = [{WId, WFun} | WRem]} = Q, Id) ->
    Srv = self(),
    case A -- [Id] of
        ARem when length(ARem) < MaxA ->
            spawn(fun() -> catch WFun(), end_work(Srv, WId) end),
            Q#q{active = [WId | A], waiting = WRem};
        ARem ->
            Q#q{active = ARem}
    end.
