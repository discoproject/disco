-module(event_stream).
-behaviour(gen_fsm).

-export([start_link/1]).
-export([init/1, code_change/4,
         handle_event/3, handle_info/3, handle_sync_event/4,
         terminate/3]).
-export([feed/2]).
-export([outside_event/2, inside_event/2]).

-define(MESSAGES_MAX, 100).
-define(OOB_KEY_MAX, 256).
-define(EVENT_OPEN,  "**<", EventType:3/binary, ">").
-define(EVENT_CLOSE, "<>**").
-define(TIMESTAMP,
        Year:2/binary, "/", Month:2/binary, "/",  Day:2/binary, " ",
        Hour:2/binary, ":", Minute:2/binary, ":", Second:2/binary).


start_link(Worker) ->
        gen_fsm:start_link(event_stream, {Worker}, []).

feed(EventStream, Data) ->
        gen_fsm:send_event(EventStream, Data).

outside_event(Data, {Worker} = _StateData) ->
        case Data of
                {eol, <<?EVENT_OPEN, " ", ?TIMESTAMP, " ", TagBin/binary>>} ->
                        Tags = string:tokens(binary_to_list(TagBin), " "),
                        {next_state, inside_event,
                         {EventType,
                          {Year, Month, Day, Hour, Minute, Second},
                          Tags,
                          message_buffer:new(?MESSAGES_MAX),
                         Worker}};
                {_IsEOL, <<BadMessage/binary>>} ->
                        gen_server:cast(Worker, {errline,
                                                 binary_to_list(BadMessage) ++ case _IsEOL of
                                                                                       true -> "";
                                                                                       false -> "..."
                                                                               end}),
                        {next_state, outside_event, {Worker}}
        end.

inside_event(Data, {EventType, Time, Tags, Messages, Worker} = _StateData) ->
        case Data of
                {eol, <<?EVENT_CLOSE>>} ->
                        case catch finalize_event(EventType, Messages) of
                                {ok, Payload} ->
                                        gen_server:cast(Worker, {event, {EventType, Time, Tags, Payload}});
                                {error, Reason} ->
                                        gen_server:cast(Worker, {malformed_event, Reason});
                                _Else ->
                                        gen_server:cast(Worker, {malformed_event,
                                                                 "Invalid " ++ EventType ++ ": " ++
                                                                 message_buffer:to_string(Messages)})
                        end,
                        {next_state, outside_event, {Worker}};
                {_IsEOL, <<Message/binary>>} ->
                        {next_state, inside_event,
                         {EventType, Time, Tags,
                          message_buffer:append(binary_to_list(Message), Messages),
                          Worker}}
        end.

finalize_event(<<"PID">>, Messages) ->
        [ChildPID] = message_buffer:to_list(Messages),
        {ok, ChildPID};

finalize_event(<<"OOB">>, Messages) ->
        [Message] = message_buffer:to_list(Messages),
        [Key|Path] = string:tokens(Message, " "),
        case length(Key) > ?OOB_KEY_MAX of
                true ->
                        {error, "OOB key too long: " ++ Key ++ ". Max " ++
                         integer_to_list(?OOB_KEY_MAX) ++ " characters"};
                false ->
                        {ok, {Key, Path}}
        end;

finalize_event(<<"OUT">>, Messages) ->
        [Results] = message_buffer:to_list(Messages),
        {ok, Results};

finalize_event(_EventType, Messages) ->
        {ok, message_buffer:to_string(Messages)}.

%%%%%%%%%%%%%%%%%%%
% gen_fsm callbacks
%%%%%%%%%%%%%%%%%%%

init(Args) ->
        {ok, outside_event, Args}.

code_change(_OldVsn, StateName, StateData, _Extra) ->
        {ok, StateName, StateData}.

handle_event(_Event, _StateName, _StateData) ->
        {stop, reject, []}.

handle_info(_Info, _StateName, _StateData) ->
        {stop, reject, []}.

handle_sync_event(_Event, _From, _StateName, _StateData) ->
        {stop, reject, fail, []}.

terminate(_Reason, _StateName, _StateData) ->
        ok.
