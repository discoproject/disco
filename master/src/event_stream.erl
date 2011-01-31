-module(event_stream).

-export([new/0, feed/2]).

-define(MESSAGES_MAX, 100).
-define(EVENT_OPEN,  "**<", EventType:3/binary, ">").
-define(EVENT_OPENV, "**<", EventType:3/binary, ":", Version:2/binary, ">").
-define(EVENT_CLOSE, "<>**").
-define(TIMESTAMP,
    Year:2/binary, "/", Month:2/binary, "/",  Day:2/binary, " ",
    Hour:2/binary, ":", Minute:2/binary, ":", Second:2/binary).

-type event_type() :: binary().
-type bin_time() :: {binary(), binary(), binary(), binary(), binary(), binary()}.
-type gen_time() :: {non_neg_integer(), 1..12, 1..255, byte(), byte(), byte()}.
-type inside_event() :: {event_type(), bin_time(), [string()],
     message_buffer:message_buffer()}.
-type outside_event() :: {'event', {event_type(), gen_time(), [string()], string()}}
    | {'malformed_event', string()}.
-type state() :: {'inside_event', inside_event()} | {'outside_event', outside_event() | {'errline', string()} | {}}.
-type event_stream() :: {'next_stream', state()}.
-type data() :: {'eol' | _, binary()}.

-spec new() -> event_stream().
new() ->
    {next_stream, {outside_event, {}}}.

-spec feed(data(), event_stream()) -> event_stream().
feed(Data, {next_stream, State}) ->
    handle_state(State, Data).

handle_event_header(EventType, _Version, <<" ", ?TIMESTAMP, TagBin/binary>>) ->
    Tags = string:tokens(binary_to_list(TagBin), " "),
    {next_stream, {inside_event,
                   {EventType,
                    {Year, Month, Day, Hour, Minute, Second},
                    Tags,
                    message_buffer:new(?MESSAGES_MAX)}}};

handle_event_header(EventType, _Version, <<" ", Message/binary>>) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(now()),
    Messages = message_buffer:new(?MESSAGES_MAX),
    {next_stream, {outside_event,
                   event({EventType,
                          {Year, Month, Day, Hour, Minute, Second},
                          [],
                          message_buffer:append(Message, Messages)},
                         raw)}};

handle_event_header(_EventType, _Version, <<BadMessage/binary>>) ->
    {next_stream, {outside_event, {errline, BadMessage}}}.

-spec handle_state(state(), data()) -> event_stream().
handle_state({outside_event, _StateData}, {eol, <<?EVENT_OPEN, Rest/binary>>}) ->
    handle_event_header(EventType, <<"00">>, Rest);

handle_state({outside_event, _StateData}, {eol, <<?EVENT_OPENV, Rest/binary>>}) ->
    handle_event_header(EventType, Version, Rest);

handle_state({outside_event, _StateData}, {eol, <<BadMessage/binary>>}) ->
    handle_event_header(notype, noversion, BadMessage);

handle_state({outside_event, _StateData}, {noeol, <<BadMessage/binary>>}) ->
    handle_event_header(notype, noversion, <<BadMessage/binary, "...">>);

handle_state({inside_event, {EventType, Time, Tags, Messages} = _StateData},
             {eol, <<?EVENT_CLOSE>>}) ->
    {next_stream, {outside_event,
                   event({EventType, Time, Tags, Messages}, dencode)}};

handle_state({inside_event, {EventType, Time, Tags, Messages} = _StateData},
             {_IsEOL, <<Message/binary>>}) ->
    {next_stream, {inside_event,
                   {EventType, Time, Tags,
                    message_buffer:append(Message, Messages)}}}.

-spec event({event_type(), gen_time(), [binary()],
             message_buffer:message_buffer()}, atom()) -> outside_event().
event({EventType, Time, Tags, Messages}, Encoding) ->
    case catch finalize_event({EventType, Time, Tags, Messages}, Encoding) of
        {ok, Payload} ->
            {event, {EventType, Time, Tags, Payload}};
        {error, Reason} ->
            {malformed_event, Reason};
        _Else ->
            {malformed_event,
             disco:format("Invalid '~s' event in stream:~n~s",
                          [EventType, message_buffer:to_string(Messages)])}
    end.

-spec finalize_event({event_type(), gen_time(), [binary()],
                      message_buffer:message_buffer()}, atom()) -> {'ok', string()}.
finalize_event({_EventType, _Time, _Tags, Messages}, dencode) ->
    {ok, dencode:decode(list_to_binary(message_buffer:to_string(Messages)))};
finalize_event({_EventType, _Time, _Tags, Messages}, raw) ->
    {ok, message_buffer:to_binary(Messages)}.
