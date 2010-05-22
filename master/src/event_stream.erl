-module(event_stream).

-export([new/0, feed/2]).

-define(MESSAGES_MAX, 100).
-define(EVENT_OPEN,  "**<", EventType:3/binary, ">").
-define(EVENT_CLOSE, "<>**").
-define(TIMESTAMP,
    Year:2/binary, "/", Month:2/binary, "/",  Day:2/binary, " ",
    Hour:2/binary, ":", Minute:2/binary, ":", Second:2/binary).

new() ->
    {next_stream, {outside_event, {}}}.

feed(Data, {next_stream, State}) -> handle_state(State, Data).

handle_state({outside_event, _StateData}, Data) ->
    case Data of
        {eol, <<?EVENT_OPEN, " ", ?TIMESTAMP, TagBin/binary>>} ->
            Tags = string:tokens(binary_to_list(TagBin), " "),
            {next_stream, {inside_event,
                       {EventType,
                    {Year, Month, Day, Hour, Minute, Second},
                    Tags,
                    message_buffer:new(?MESSAGES_MAX)}}};
        {eol, <<?EVENT_OPEN, " ", Message/binary>>} ->
            {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(now()),
            Messages = message_buffer:new(?MESSAGES_MAX),
            {next_stream, {outside_event,
                       event({EventType,
                          {Year, Month, Day, Hour, Minute, Second},
                          [],
                         message_buffer:append(binary_to_list(Message), Messages)})}};
        {IsEOL, <<BadMessage/binary>>} ->
            {next_stream, {outside_event,
                       {errline, binary_to_list(BadMessage) ++
                    case IsEOL of
                        eol -> "";
                        _ -> "..."
                    end}}}
    end;

handle_state({inside_event, {EventType, Time, Tags, Messages} = _StateData}, Data) ->
    case Data of
        {eol, <<?EVENT_CLOSE>>} ->
            {next_stream, {outside_event,
                       event({EventType, Time, Tags, Messages})}};
        {_IsEOL, <<Message/binary>>} ->
            {next_stream, {inside_event,
                      {EventType, Time, Tags,
                       message_buffer:append(binary_to_list(Message), Messages)}}}
    end.

event({EventType, Time, Tags, Messages}) ->
    case catch finalize_event({EventType, Time, Tags, Messages}) of
        {ok, Payload} ->
            {event, {EventType, Time, Tags, Payload}};
        {error, Reason} ->
            {malformed_event, Reason};
        _Else ->
            {malformed_event,
             "Invalid " ++ EventType ++ ": " ++
             message_buffer:to_string(Messages)}
    end.

finalize_event({<<"PID">>, _Time, _Tags, Messages}) ->
    [ChildPID] = message_buffer:to_list(Messages),
    {ok, ChildPID};

finalize_event({<<"OUT">>, _Time, _Tags, Messages}) ->
    [Results] = message_buffer:to_list(Messages),
    {ok, Results};

finalize_event({_EventType, _Time, _Tags, Messages}) ->
    {ok, message_buffer:to_string(Messages)}.
